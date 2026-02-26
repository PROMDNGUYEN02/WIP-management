from __future__ import annotations

import json
import logging
import re
import time
from collections.abc import Iterable
from datetime import datetime, timedelta, timezone
from typing import Any

from wip_management.config import settings
from wip_management.domain.models.tray import SignalSource, TrayId, TraySignal, Watermark
from wip_management.infrastructure.sqlserver.connection import SQLServerConnection
from wip_management.infrastructure.sqlserver.window_cache import SQLWindowRowCache

_TOKEN_PATTERN = re.compile(r"V[A-Z]\d{4,}", re.IGNORECASE)
log = logging.getLogger(__name__)


class CcuRepo:
    def __init__(self, connection: SQLServerConnection) -> None:
        self._conn = connection
        self._table = SQLServerConnection.table_name(settings.sql_schema, settings.ccu_table)
        self._lot_col = SQLServerConnection.column_name(settings.ccu_lot_column)
        self._start_col = SQLServerConnection.column_name(settings.ccu_start_column)
        self._end_col = SQLServerConnection.column_name(settings.ccu_end_column)
        self._record_col = SQLServerConnection.column_name(settings.ccu_record_json_column)
        self._tray_key = settings.ccu_json_tray_id_key
        self._pos_key = settings.ccu_json_pos_key
        self._keyword = settings.record_filter_keyword.strip().upper()
        self._tray_cells_cache: dict[str, list[dict[str, str | None]]] = {}
        self._tray_cells_cache_keys: dict[str, set[tuple[str, str | None, str | None]]] = {}
        self._tray_cells_cache_max_trays = 2000
        self._tray_cells_max_rows_per_tray = 2000
        self._cell_owner_cache: dict[str, dict[str, str | None]] = {}
        self._cell_owner_cache_times: dict[str, datetime] = {}
        self._cell_owner_cache_max_entries = 200000
        self._range_cache = SQLWindowRowCache(
            name="CCU",
            overlap_seconds=settings.delta_overlap_seconds,
            max_rows=max(50000, settings.max_fetch_batch * 8),
        )

    async def start(self) -> None:
        await self._conn.start()

    async def close(self) -> None:
        await self._conn.close()

    async def fetch_initial(self, start_time: datetime, end_time: datetime) -> list[TraySignal]:
        log.info("CCU initial fetch start start_time=%s end_time=%s", start_time.isoformat(), end_time.isoformat())
        started_at = time.perf_counter()
        rows = await self._fetch_range(start_time=start_time, end_time=end_time)
        signals = self._rows_to_signals(rows)
        elapsed_ms = int((time.perf_counter() - started_at) * 1000)
        log.info(
            "CCU initial fetch done elapsed_ms=%s raw_rows=%s signals=%s",
            elapsed_ms,
            len(rows),
            len(signals),
        )
        return signals

    async def fetch_delta(self, watermark: Watermark, end_time: datetime) -> list[TraySignal]:
        overlap_start = watermark.collected_time - timedelta(seconds=settings.delta_overlap_seconds)
        log.debug(
            "CCU delta fetch start watermark=%s overlap_start=%s end_time=%s",
            watermark.collected_time.isoformat(),
            overlap_start.isoformat(),
            end_time.isoformat(),
        )
        started_at = time.perf_counter()
        rows = await self._fetch_range(start_time=overlap_start, end_time=end_time)
        signals = self._rows_to_signals(rows)
        elapsed_ms = int((time.perf_counter() - started_at) * 1000)
        log.debug("CCU delta fetch done elapsed_ms=%s raw_rows=%s signals=%s", elapsed_ms, len(rows), len(signals))
        return signals

    async def fetch_by_tray_ids(self, tray_ids: Iterable[str], end_time: datetime) -> list[TraySignal]:
        wanted = {tray_id.strip() for tray_id in tray_ids if tray_id and tray_id.strip()}
        if not wanted:
            log.debug("CCU backfill skipped because tray_ids is empty")
            return []
        log.info("CCU backfill fetch start tray_count=%s end_time=%s", len(wanted), end_time.isoformat())
        started_at = time.perf_counter()
        day_start = end_time.replace(hour=settings.initial_load_start_hour, minute=0, second=0, microsecond=0)
        rows = await self._fetch_range(start_time=day_start, end_time=end_time)
        latest_by_tray: dict[str, TraySignal] = {}
        for signal in self._rows_to_signals(rows):
            tray_key = str(signal.tray_id)
            if tray_key not in wanted:
                continue
            previous = latest_by_tray.get(tray_key)
            if previous is None or signal.collected_time >= previous.collected_time:
                latest_by_tray[tray_key] = signal
        out = list(latest_by_tray.values())
        out.sort(key=lambda item: (item.collected_time, str(item.tray_id)))
        elapsed_ms = int((time.perf_counter() - started_at) * 1000)
        log.info(
            "CCU backfill fetch done elapsed_ms=%s raw_rows=%s matched_trays=%s signals=%s",
            elapsed_ms,
            len(rows),
            len(latest_by_tray),
            len(out),
        )
        return out

    async def peek_latest_signal_time(self, end_time: datetime) -> datetime | None:
        query = (
            "SELECT TOP 1 COALESCE("
            f"{self._end_col}, {self._start_col}"
            ") AS latest_signal_time "
            f"FROM {self._table} "
            "WHERE COALESCE("
            f"{self._end_col}, {self._start_col}"
            ") <= ? "
            "ORDER BY COALESCE("
            f"{self._end_col}, {self._start_col}"
            ") DESC"
        )
        rows = await self._conn.query_rows(query, [end_time])
        if not rows:
            log.debug("CCU peek latest returned empty rowset")
            return None
        latest = _coerce_datetime(rows[0].get("latest_signal_time"))
        if latest is None:
            log.debug("CCU peek latest signal_time was not parseable")
            return None
        log.debug("CCU peek latest signal_time=%s", latest.isoformat())
        return latest

    async def fetch_tray_cells(
        self,
        tray_id: str,
        start_time: datetime,
        end_time: datetime,
    ) -> list[dict[str, str | None]]:
        wanted = _normalize_tray_id(tray_id)
        if not wanted:
            return []
        cached = self._get_cached_cells_for_tray(wanted)
        if cached is not None:
            log.info(
                "CCU tray detail fetch tray_id=%s cells=%s source=memory-cache",
                wanted,
                len(cached),
            )
            return cached
        if settings.sql_driver.strip().lower() == "sql server":
            log.warning(
                "CCU tray detail skipped tray_id=%s reason=legacy-driver-no-cache",
                wanted,
            )
            return []
        query = (
            "SELECT TOP 5000 "
            f"{self._lot_col} AS lot_no, "
            f"{self._start_col} AS start_time, "
            f"{self._end_col} AS end_time, "
            f"{self._record_col} AS record_json "
            f" FROM {self._table} "
            "WHERE ("
            f"({self._end_col} IS NOT NULL AND {self._end_col} >= ? AND {self._end_col} <= ?) "
            "OR "
            f"({self._end_col} IS NULL AND {self._start_col} >= ? AND {self._start_col} <= ?)"
            ") "
            "AND record_json LIKE ? "
            "ORDER BY COALESCE(end_time, start_time) ASC, lot_no ASC, start_time ASC, end_time ASC"
        )
        like_text = f"%{wanted}%"
        rows = await self._conn.query_rows(
            query,
            [start_time, end_time, start_time, end_time, like_text],
        )

        out: list[dict[str, str | None]] = []
        seen: set[tuple[str, str | None, str | None]] = set()
        for row in rows:
            raw_json = row.get("record_json")
            parsed_json = _parse_json(raw_json)
            raw_text = _stringify_record_json(raw_json, parsed_json)
            tray_key, _ = _extract_tray_id_key(
                parsed_json=parsed_json,
                raw_text=raw_text,
                primary_key=self._tray_key,
                fallback_keys=("TRAY_ID", "TRAYBARCODE", "TRAY_BARCODE"),
            )
            if tray_key != wanted:
                continue
            lot_value = row.get("lot_no")
            cell_id = str(lot_value).strip() if lot_value is not None else ""
            start_dt = _coerce_datetime(row.get("start_time"))
            end_dt = _coerce_datetime(row.get("end_time"))
            start_iso = start_dt.isoformat(sep=" ", timespec="seconds") if start_dt else None
            end_iso = end_dt.isoformat(sep=" ", timespec="seconds") if end_dt else None
            dedup_key = (cell_id, start_iso, end_iso)
            if dedup_key in seen:
                continue
            seen.add(dedup_key)
            out.append(
                {
                    "cell_id": cell_id,
                    "start_time": start_iso,
                    "end_time": end_iso,
                }
            )

        out.sort(key=lambda item: (item.get("start_time") or "", item.get("cell_id") or ""))
        if out:
            self._replace_cached_cells_for_tray(wanted, out)
        log.info(
            "CCU tray detail fetch tray_id=%s cells=%s raw_rows=%s start_time=%s end_time=%s",
            wanted,
            len(out),
            len(rows),
            start_time.isoformat(),
            end_time.isoformat(),
        )
        return out

    async def fetch_cell_owner(
        self,
        cell_id: str,
        start_time: datetime,
        end_time: datetime,
    ) -> dict[str, str | None] | None:
        wanted_cell = str(cell_id).strip().upper()
        if not wanted_cell:
            return None
        cached_owner = self._cell_owner_cache.get(wanted_cell)
        if cached_owner is not None:
            log.info("CCU cell owner lookup cache hit cell_id=%s tray_id=%s", wanted_cell, cached_owner.get("tray_id"))
            return dict(cached_owner)
        if settings.sql_driver.strip().lower() == "sql server":
            log.warning(
                "CCU cell owner lookup skipped cell_id=%s reason=legacy-driver-no-cache",
                wanted_cell,
            )
            return None
        query = (
            "SELECT TOP 400 "
            f"{self._lot_col} AS lot_no, "
            f"{self._start_col} AS start_time, "
            f"{self._end_col} AS end_time, "
            f"{self._record_col} AS record_json "
            f" FROM {self._table} "
            "WHERE ("
            f"({self._end_col} IS NOT NULL AND {self._end_col} >= ? AND {self._end_col} <= ?) "
            "OR "
            f"({self._end_col} IS NULL AND {self._start_col} >= ? AND {self._start_col} <= ?)"
            ") "
            "AND UPPER(CAST(lot_no AS NVARCHAR(128))) = ? "
            "ORDER BY COALESCE(end_time, start_time) DESC"
        )
        rows = await self._conn.query_rows(
            query,
            [start_time, end_time, start_time, end_time, wanted_cell],
        )
        if not rows:
            log.info(
                "CCU cell owner lookup empty cell_id=%s start_time=%s end_time=%s",
                wanted_cell,
                start_time.isoformat(),
                end_time.isoformat(),
            )
            return None

        for row in rows:
            raw_json = row.get("record_json")
            parsed_json = _parse_json(raw_json)
            raw_text = _stringify_record_json(raw_json, parsed_json)
            tray_key, _ = _extract_tray_id_key(
                parsed_json=parsed_json,
                raw_text=raw_text,
                primary_key=self._tray_key,
                fallback_keys=("TRAY_ID", "TRAYBARCODE", "TRAY_BARCODE"),
            )
            if not tray_key:
                continue
            start_dt = _coerce_datetime(row.get("start_time"))
            end_dt = _coerce_datetime(row.get("end_time"))
            result = {
                "cell_id": wanted_cell,
                "tray_id": tray_key,
                "start_time": start_dt.isoformat(sep=" ", timespec="seconds") if start_dt else None,
                "end_time": end_dt.isoformat(sep=" ", timespec="seconds") if end_dt else None,
            }
            self._cache_cell_owner(
                cell_id=wanted_cell,
                tray_id=tray_key,
                start_time=start_dt,
                end_time=end_dt,
            )
            log.info(
                "CCU cell owner lookup hit cell_id=%s tray_id=%s start_time=%s end_time=%s",
                wanted_cell,
                tray_key,
                result["start_time"],
                result["end_time"],
            )
            return result

        log.info(
            "CCU cell owner lookup unresolved tray_id cell_id=%s scanned_rows=%s",
            wanted_cell,
            len(rows),
        )
        return None

    async def _fetch_range(self, start_time: datetime, end_time: datetime) -> list[dict[str, Any]]:
        return await self._range_cache.fetch(
            start_time=start_time,
            end_time=end_time,
            fetch_uncached=self._fetch_range_uncached,
        )

    async def _fetch_range_uncached(self, start_time: datetime, end_time: datetime) -> list[dict[str, Any]]:
        all_rows: list[dict[str, Any]] = []
        offset = 0
        page = 0
        while True:
            batch = await self._fetch_batch(start_time=start_time, end_time=end_time, offset=offset)
            if not batch:
                log.debug("CCU fetch range page=%s offset=%s returned empty", page, offset)
                break
            all_rows.extend(batch)
            log.debug("CCU fetch range page=%s offset=%s batch_rows=%s", page, offset, len(batch))
            if len(batch) < settings.max_fetch_batch:
                break
            offset += len(batch)
            page += 1
        return all_rows

    async def _fetch_batch(
        self,
        *,
        start_time: datetime,
        end_time: datetime,
        offset: int,
    ) -> list[dict[str, Any]]:
        query = (
            "SELECT "
            f"{self._lot_col} AS lot_no, "
            f"{self._start_col} AS start_time, "
            f"{self._end_col} AS end_time, "
            f"{self._record_col} AS record_json "
            f" FROM {self._table} "
            "WHERE ("
            f"({self._end_col} IS NOT NULL AND {self._end_col} >= ? AND {self._end_col} <= ?) "
            "OR "
            f"({self._end_col} IS NULL AND {self._start_col} >= ? AND {self._start_col} <= ?)"
            ") "
            "ORDER BY COALESCE(end_time, start_time) ASC, lot_no ASC, start_time ASC, end_time ASC "
            "OFFSET ? ROWS FETCH NEXT ? ROWS ONLY"
        )
        params: list[Any] = [
            start_time,
            end_time,
            start_time,
            end_time,
            offset,
            settings.max_fetch_batch,
        ]
        return await self._conn.query_rows(query, params)

    def _rows_to_signals(self, rows: list[dict[str, Any]]) -> list[TraySignal]:
        per_tray: dict[str, dict[str, Any]] = {}
        skipped_no_time = 0
        skipped_bad_json = 0
        skipped_no_token = 0
        skipped_no_tray = 0
        missing_lot_rows = 0
        parsed_from_text = 0
        tray_from_token = 0

        for row in rows:
            start_time = _coerce_datetime(row.get("start_time"))
            end_time = _coerce_datetime(row.get("end_time"))
            collected_time = end_time or start_time
            if collected_time is None:
                skipped_no_time += 1
                continue

            raw_json = row.get("record_json")
            parsed_json = _parse_json(raw_json)
            if parsed_json is None:
                skipped_bad_json += 1
            raw_text = _stringify_record_json(raw_json, parsed_json)
            token = _extract_token(raw_text)
            lot_value = row.get("lot_no")
            lot_no = str(lot_value).strip() if lot_value is not None else ""
            if token is None and lot_no:
                token = _extract_token(lot_no)
            if token is None and self._keyword:
                skipped_no_token += 1
                continue
            searchable = f"{raw_text} {lot_no} {token or ''}".upper()
            if self._keyword and self._keyword not in searchable:
                skipped_no_token += 1
                continue

            tray_key, tray_source = _extract_tray_id_key(
                parsed_json=parsed_json,
                raw_text=raw_text,
                primary_key=self._tray_key,
                fallback_keys=("TRAY_ID", "TRAYBARCODE", "TRAY_BARCODE"),
            )
            if not tray_key:
                skipped_no_tray += 1
                continue
            if tray_source in {"text", "marker"}:
                parsed_from_text += 1
            if tray_source == "token":
                tray_from_token += 1

            if not lot_no:
                missing_lot_rows += 1
            self._cache_cell_row(
                tray_key=tray_key,
                lot_no=lot_no,
                start_time=start_time,
                end_time=end_time,
            )

            stat = per_tray.get(tray_key)
            if stat is None:
                stat = {
                    "quantity": 0,
                    "min_pos": None,
                    "max_pos": None,
                    "start_at_min_pos": None,
                    "end_at_max_pos": None,
                    "first_start_time": None,
                    "last_end_time": None,
                    "latest_signal_time": collected_time,
                    "latest_lot_no": lot_no or None,
                    "record_token": token,
                }
                per_tray[tray_key] = stat

            stat["quantity"] += 1
            if lot_no:
                stat["latest_lot_no"] = lot_no
            if token:
                stat["record_token"] = token

            pos_raw = _find_json_value(parsed_json, self._pos_key)
            if pos_raw is None:
                pos_raw = _find_text_value(raw_text, self._pos_key)
            if pos_raw is None:
                pos_raw = _find_text_value(raw_text, "TRAY_POS")
            pos_value = _to_int(pos_raw)
            if pos_value is not None:
                current_min = stat["min_pos"]
                current_max = stat["max_pos"]
                if current_min is None or pos_value < current_min:
                    stat["min_pos"] = pos_value
                    stat["start_at_min_pos"] = start_time or collected_time
                if current_max is None or pos_value > current_max:
                    stat["max_pos"] = pos_value
                    stat["end_at_max_pos"] = end_time or collected_time

            if start_time is not None:
                first_start = stat["first_start_time"]
                if first_start is None or start_time < first_start:
                    stat["first_start_time"] = start_time
            if end_time is not None:
                last_end = stat["last_end_time"]
                if last_end is None or end_time > last_end:
                    stat["last_end_time"] = end_time
            if collected_time > stat["latest_signal_time"]:
                stat["latest_signal_time"] = collected_time

        out: list[TraySignal] = []
        for tray_text, stat in per_tray.items():
            start_dt = stat["start_at_min_pos"] or stat["first_start_time"]
            end_dt = stat["end_at_max_pos"] or stat["last_end_time"] or stat["latest_signal_time"]
            payload = {
                "lot_no": stat["latest_lot_no"],
                "quantity": stat["quantity"],
                "start_time": start_dt.isoformat() if start_dt is not None else None,
                "end_time": end_dt.isoformat() if end_dt is not None else None,
                "min_position": stat["min_pos"],
                "max_position": stat["max_pos"],
                "record_token": stat["record_token"],
            }
            out.append(
                TraySignal(
                    source=SignalSource.CCU,
                    tray_id=TrayId(tray_text),
                    collected_time=stat["latest_signal_time"],
                    payload=payload,
                )
            )

        out.sort(key=lambda item: (item.collected_time, str(item.tray_id)))
        log_level = log.warning if rows and not out else log.debug
        log_level(
            "CCU convert rows done input=%s output=%s skipped_no_time=%s skipped_bad_json=%s skipped_no_token=%s skipped_no_tray=%s missing_lot_rows=%s parsed_from_text=%s tray_from_token=%s keyword=%s",
            len(rows),
            len(out),
            skipped_no_time,
            skipped_bad_json,
            skipped_no_token,
            skipped_no_tray,
            missing_lot_rows,
            parsed_from_text,
            tray_from_token,
            self._keyword or "<none>",
        )
        return out

    def _cache_cell_row(
        self,
        *,
        tray_key: str,
        lot_no: str,
        start_time: datetime | None,
        end_time: datetime | None,
    ) -> None:
        if not tray_key or not lot_no:
            return
        start_iso = start_time.isoformat(sep=" ", timespec="seconds") if start_time else None
        end_iso = end_time.isoformat(sep=" ", timespec="seconds") if end_time else None
        dedup_key = (lot_no, start_iso, end_iso)

        if tray_key not in self._tray_cells_cache:
            if len(self._tray_cells_cache) >= self._tray_cells_cache_max_trays:
                oldest_tray = next(iter(self._tray_cells_cache))
                self._tray_cells_cache.pop(oldest_tray, None)
                self._tray_cells_cache_keys.pop(oldest_tray, None)
            self._tray_cells_cache[tray_key] = []
            self._tray_cells_cache_keys[tray_key] = set()

        keys = self._tray_cells_cache_keys[tray_key]
        if dedup_key in keys:
            return
        rows = self._tray_cells_cache[tray_key]
        rows.append({"cell_id": lot_no, "start_time": start_iso, "end_time": end_iso})
        keys.add(dedup_key)

        if len(rows) > self._tray_cells_max_rows_per_tray:
            removed = rows.pop(0)
            removed_key = (
                str(removed.get("cell_id") or ""),
                removed.get("start_time"),
                removed.get("end_time"),
            )
            keys.discard(removed_key)
        self._cache_cell_owner(
            cell_id=lot_no,
            tray_id=tray_key,
            start_time=start_time,
            end_time=end_time,
        )

    def _cache_cell_owner(
        self,
        *,
        cell_id: str,
        tray_id: str,
        start_time: datetime | None,
        end_time: datetime | None,
    ) -> None:
        cell_key = str(cell_id).strip().upper()
        tray_key = str(tray_id).strip()
        if not cell_key or not tray_key:
            return
        signal_time = end_time or start_time
        if signal_time is None:
            signal_time = datetime.min
        prev_time = self._cell_owner_cache_times.get(cell_key)
        if prev_time is not None and prev_time > signal_time:
            return
        owner = {
            "cell_id": cell_key,
            "tray_id": tray_key,
            "start_time": start_time.isoformat(sep=" ", timespec="seconds") if start_time else None,
            "end_time": end_time.isoformat(sep=" ", timespec="seconds") if end_time else None,
        }
        if cell_key in self._cell_owner_cache:
            self._cell_owner_cache.pop(cell_key, None)
            self._cell_owner_cache_times.pop(cell_key, None)
        self._cell_owner_cache[cell_key] = owner
        self._cell_owner_cache_times[cell_key] = signal_time
        overflow = len(self._cell_owner_cache) - self._cell_owner_cache_max_entries
        if overflow > 0:
            stale_keys = list(self._cell_owner_cache.keys())[:overflow]
            for stale_key in stale_keys:
                self._cell_owner_cache.pop(stale_key, None)
                self._cell_owner_cache_times.pop(stale_key, None)

    def _replace_cached_cells_for_tray(self, tray_key: str, rows: list[dict[str, str | None]]) -> None:
        keys: set[tuple[str, str | None, str | None]] = set()
        dedup_rows: list[dict[str, str | None]] = []
        for row in rows:
            dedup_key = (
                str(row.get("cell_id") or ""),
                row.get("start_time"),
                row.get("end_time"),
            )
            if dedup_key in keys:
                continue
            keys.add(dedup_key)
            dedup_rows.append(
                {
                    "cell_id": dedup_key[0],
                    "start_time": dedup_key[1],
                    "end_time": dedup_key[2],
                }
            )
            if len(dedup_rows) >= self._tray_cells_max_rows_per_tray:
                break
        self._tray_cells_cache[tray_key] = dedup_rows
        self._tray_cells_cache_keys[tray_key] = keys

    def _get_cached_cells_for_tray(self, tray_key: str) -> list[dict[str, str | None]] | None:
        rows = self._tray_cells_cache.get(tray_key)
        if rows is None:
            return None
        out = [dict(item) for item in rows]
        out.sort(key=lambda item: (item.get("start_time") or "", item.get("cell_id") or ""))
        return out


def _parse_json(value: Any) -> Any | None:
    if isinstance(value, (dict, list)):
        return value
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return None
    if isinstance(parsed, str):
        nested = parsed.strip()
        if nested:
            try:
                parsed_nested = json.loads(nested)
                if isinstance(parsed_nested, (dict, list)):
                    return parsed_nested
            except json.JSONDecodeError:
                return None
    if isinstance(parsed, (dict, list)):
        return parsed
    return None


def _extract_token(text: str) -> str | None:
    match = _TOKEN_PATTERN.search(text)
    if not match:
        return None
    return match.group(0).upper()


def _find_json_value(obj: Any, target_key: str) -> Any | None:
    if isinstance(obj, dict):
        for key, value in obj.items():
            if str(key).upper() == target_key.upper():
                return value
        for value in obj.values():
            found = _find_json_value(value, target_key)
            if found is not None:
                return found
        return None
    if isinstance(obj, list):
        for value in obj:
            found = _find_json_value(value, target_key)
            if found is not None:
                return found
    return None


def _to_int(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    try:
        return int(str(value).strip())
    except Exception:  # noqa: BLE001
        return None


def _stringify_record_json(raw_json: Any, parsed_json: Any | None) -> str:
    if isinstance(raw_json, str):
        return raw_json
    if parsed_json is not None:
        try:
            return json.dumps(parsed_json, ensure_ascii=True)
        except Exception:  # noqa: BLE001
            return str(raw_json)
    if raw_json is None:
        return ""
    return str(raw_json)


def _find_text_value(text: str, target_key: str) -> str | None:
    if not text:
        return None
    key = re.escape(target_key)
    patterns = (
        r'["\']?' + key + r'["\']?\s*[:=]\s*["\']?([^,"\'\}\]\s]+)',
        key + r"\s*[:=]\s*([^,;\s]+)",
    )
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if not match:
            continue
        value = match.group(1).strip().strip("\"'")
        if value:
            return value
    return None


def _coerce_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value
        return value.astimezone(timezone.utc).replace(tzinfo=None)

    text = str(value).strip()
    if not text:
        return None
    normalized = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
        if parsed.tzinfo is not None:
            return parsed.astimezone(timezone.utc).replace(tzinfo=None)
        return parsed
    except ValueError:
        pass

    for fmt in (
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S",
        "%Y/%m/%d %H:%M:%S",
        "%Y-%m-%d",
    ):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def _normalize_tray_id(raw_value: str) -> str:
    text = raw_value.strip().strip("\"'[]()")
    if not text:
        return ""
    # Keep only alphanumeric to align CCU/FPC barcode variants
    normalized = re.sub(r"[^A-Za-z0-9]", "", text).upper()
    return normalized


def _extract_tray_id_key(
    *,
    parsed_json: Any | None,
    raw_text: str,
    primary_key: str,
    fallback_keys: tuple[str, ...],
) -> tuple[str, str]:
    # 1) JSON key first
    json_value = _find_json_value(parsed_json, primary_key)
    if json_value is not None:
        key = _normalize_tray_id(str(json_value))
        if _is_plausible_tray_id(key):
            return key, "json"

    # 2) Text key matches
    for key_name in (primary_key, *fallback_keys):
        text_value = _find_text_value(raw_text, key_name)
        if text_value is None:
            continue
        key = _normalize_tray_id(text_value)
        if _is_plausible_tray_id(key):
            return key, "text"

    # 3) Marker-based value in flattened payloads
    marker_value = _extract_after_markers(raw_text, ("COLLECTPARAMVALUE", "PARAMVALUE", "PARAMRESULT"))
    if marker_value is not None:
        key = _normalize_tray_id(marker_value)
        if _is_plausible_tray_id(key):
            return key, "marker"

    # 4) Fallback token pattern
    token = _extract_token(raw_text)
    if token is not None:
        key = _normalize_tray_id(token)
        if _is_plausible_tray_id(key):
            return key, "token"

    return "", ""


def _extract_after_markers(text: str, markers: tuple[str, ...]) -> str | None:
    upper_text = text.upper()
    for marker in markers:
        idx = upper_text.find(marker)
        if idx < 0:
            continue
        segment = text[idx + len(marker) :]
        token = _extract_token(segment)
        if token:
            return token
    return None


def _is_plausible_tray_id(value: str) -> bool:
    if not value:
        return False
    if len(value) < 6 or len(value) > 40:
        return False
    if not any(ch.isdigit() for ch in value):
        return False
    bad_prefixes = (
        "FIELDNAME",
        "PARAMNAME",
        "MAXVALUE",
        "STDVALUE",
        "WIPID",
        "MINVALUE",
        "LINENO",
        "RECORDTIME",
        "CREATEUSER",
        "CREATEDATE",
        "COLLECTPARAMVALUE",
    )
    return not value.startswith(bad_prefixes)
