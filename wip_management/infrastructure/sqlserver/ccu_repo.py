from __future__ import annotations

import asyncio
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
        self._update_col = SQLServerConnection.column_name(settings.update_time_column)
        self._record_col = SQLServerConnection.column_name(settings.ccu_record_json_column)
        self._tray_key = settings.ccu_json_tray_id_key
        self._pos_key = settings.ccu_json_pos_key
        self._keyword = settings.record_filter_keyword.strip().upper()
        self._sql_tray_markers = _build_sql_marker_patterns(
            self._tray_key,
            fallback_keys=("TRAY_ID", "TRAYBARCODE", "TRAY_BARCODE"),
        )
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

    async def fetch_by_tray_ids(
        self,
        tray_ids: Iterable[str],
        end_time: datetime,
        *,
        allow_historical_scan: bool = True,
        allow_targeted_lookup: bool = True,
    ) -> list[TraySignal]:
        wanted = {tray_id.strip() for tray_id in tray_ids if tray_id and tray_id.strip()}
        if not wanted:
            log.debug("CCU backfill skipped because tray_ids is empty")
            return []
        log.info(
            "CCU backfill fetch start tray_count=%s end_time=%s allow_historical_scan=%s allow_targeted_lookup=%s",
            len(wanted),
            end_time.isoformat(),
            allow_historical_scan,
            allow_targeted_lookup,
        )
        started_at = time.perf_counter()
        day_start = end_time.replace(hour=settings.initial_load_start_hour, minute=0, second=0, microsecond=0)
        lookback_start = end_time - timedelta(hours=max(1.0, float(settings.ccu_backfill_lookback_hours)))
        backfill_step = timedelta(hours=max(0.25, float(settings.ccu_backfill_step_hours)))
        latest_by_tray: dict[str, TraySignal] = {}
        raw_rows_total = 0
        scan_count = 0

        # 1) Fast-path for small missing sets: targeted lookup first.
        targeted_first_limit = max(1, int(settings.ccu_backfill_targeted_first_max_trays))
        targeted_first = allow_targeted_lookup and len(wanted) <= targeted_first_limit
        unresolved = set(wanted)
        if targeted_first:
            targeted_signals, targeted_rows = await self._fetch_latest_signals_for_unresolved_trays(
                tray_ids=unresolved,
                start_time=lookback_start,
                end_time=end_time,
            )
            raw_rows_total += targeted_rows
            if targeted_signals:
                for signal in targeted_signals:
                    latest_by_tray[str(signal.tray_id)] = signal
            unresolved = wanted.difference(latest_by_tray.keys())
            log.info(
                "CCU backfill targeted-first done tray_count=%s resolved=%s unresolved=%s rows=%s",
                len(wanted),
                len(targeted_signals),
                len(unresolved),
                targeted_rows,
            )
        if targeted_first and unresolved:
            log.info(
                "CCU backfill targeted-first partial unresolved=%s continue_with_window_scan=True",
                len(unresolved),
            )

        # 2) Current day window (00:00 today -> now)
        if unresolved:
            rows = await self._fetch_range(start_time=day_start, end_time=end_time)
            raw_rows_total += len(rows)
            before_count = len(latest_by_tray)
            _collect_latest_by_tray_from_rows(
                rows=rows,
                wanted=unresolved,
                latest_by_tray=latest_by_tray,
                row_parser=self._rows_to_signals,
            )
            resolved_in_day = len(latest_by_tray) - before_count
            unresolved = wanted.difference(latest_by_tray.keys())
            log.info(
                "CCU backfill day-window resolved=%s unresolved=%s raw_rows=%s",
                resolved_in_day,
                len(unresolved),
                len(rows),
            )

        # 3) Walk backwards by fixed step (e.g. 23:00 yesterday, 22:00, ...)
        window_end = day_start
        empty_streak = 0
        no_progress_streak = 0
        max_empty_streak = 2
        max_no_progress_streak = 3
        while allow_historical_scan and unresolved and lookback_start < window_end:
            window_start = max(lookback_start, window_end - backfill_step)
            rows = await self._fetch_range(start_time=window_start, end_time=window_end)
            raw_rows_total += len(rows)
            before_count = len(latest_by_tray)
            _collect_latest_by_tray_from_rows(
                rows=rows,
                wanted=unresolved,
                latest_by_tray=latest_by_tray,
                row_parser=self._rows_to_signals,
            )
            newly_resolved = len(latest_by_tray) - before_count
            unresolved = wanted.difference(latest_by_tray.keys())
            scan_count += 1
            empty_streak = (empty_streak + 1) if not rows else 0
            no_progress_streak = (no_progress_streak + 1) if newly_resolved == 0 else 0
            log.info(
                "CCU backfill step=%s window_start=%s window_end=%s raw_rows=%s newly_resolved=%s unresolved=%s",
                scan_count,
                window_start.isoformat(),
                window_end.isoformat(),
                len(rows),
                newly_resolved,
                len(unresolved),
            )
            if unresolved and (empty_streak >= max_empty_streak or no_progress_streak >= max_no_progress_streak):
                log.info(
                    "CCU backfill switch-to-targeted unresolved=%s empty_streak=%s/%s no_progress_streak=%s/%s",
                    len(unresolved),
                    empty_streak,
                    max_empty_streak,
                    no_progress_streak,
                    max_no_progress_streak,
                )
                break
            if window_start == lookback_start:
                break
            window_end = window_start

        if unresolved and allow_targeted_lookup:
            unresolved_before_targeted = set(unresolved)
            targeted_signals, targeted_rows = await self._fetch_latest_signals_for_unresolved_trays(
                tray_ids=unresolved,
                start_time=lookback_start,
                end_time=end_time,
            )
            raw_rows_total += targeted_rows
            if targeted_signals:
                for signal in targeted_signals:
                    latest_by_tray[str(signal.tray_id)] = signal
                unresolved = wanted.difference(latest_by_tray.keys())
            log.info(
                "CCU backfill targeted lookup done tray_count=%s resolved=%s unresolved=%s rows=%s",
                len(unresolved_before_targeted),
                len(targeted_signals),
                len(unresolved),
                targeted_rows,
            )
        elif unresolved and (not allow_historical_scan) and (not allow_targeted_lookup):
            log.info(
                "CCU backfill skip targeted lookup unresolved=%s reason=fast-path-disabled",
                len(unresolved),
            )
        elif unresolved and (not allow_targeted_lookup):
            log.info(
                "CCU backfill skip targeted lookup unresolved=%s reason=disabled",
                len(unresolved),
            )

        out = list(latest_by_tray.values())
        out.sort(key=lambda item: (item.collected_time, str(item.tray_id)))
        elapsed_ms = int((time.perf_counter() - started_at) * 1000)
        unresolved_list = sorted(wanted.difference(latest_by_tray.keys()))
        unresolved_sample = unresolved_list[:5]
        log.info(
            "CCU backfill fetch done elapsed_ms=%s raw_rows=%s scans=%s matched_trays=%s unresolved=%s unresolved_sample=%s signals=%s lookback_start=%s step_hours=%.2f",
            elapsed_ms,
            raw_rows_total,
            scan_count,
            len(latest_by_tray),
            len(unresolved_list),
            unresolved_sample,
            len(out),
            lookback_start.isoformat(),
            backfill_step.total_seconds() / 3600.0,
        )
        return out

    async def _fetch_latest_signals_for_unresolved_trays(
        self,
        *,
        tray_ids: set[str],
        start_time: datetime,
        end_time: datetime,
    ) -> tuple[list[TraySignal], int]:
        if not tray_ids:
            return [], 0
        tray_ids_sorted = sorted(tray_ids)
        unresolved: set[str] = set(tray_ids_sorted)
        latest_by_tray: dict[str, TraySignal] = {}
        raw_rows_total = 0
        batch_matched_total = 0

        batch_enabled = bool(settings.ccu_backfill_targeted_batch_enabled)
        batch_chunk_size = max(2, int(settings.ccu_backfill_targeted_batch_max_trays))
        batch_rows_per_tray = max(20, int(settings.ccu_backfill_targeted_batch_rows_per_tray))
        if batch_enabled and len(unresolved) >= 2:
            chunk_count = 0
            for chunk_ids in _chunked(sorted(unresolved), batch_chunk_size):
                chunk_count += 1
                chunk_set = set(chunk_ids)
                rows = await self._fetch_latest_rows_for_tray_ids_batch(
                    tray_ids=chunk_set,
                    start_time=start_time,
                    end_time=end_time,
                    rows_per_tray=batch_rows_per_tray,
                )
                raw_rows_total += len(rows)
                if not rows:
                    continue
                matched_by_tray = _match_latest_signals_to_tray_ids(
                    signals=self._rows_to_signals(rows),
                    tray_ids=chunk_set,
                )
                if not matched_by_tray:
                    continue
                for tray_id, signal in matched_by_tray.items():
                    previous = latest_by_tray.get(tray_id)
                    if previous is None or signal.collected_time >= previous.collected_time:
                        latest_by_tray[tray_id] = signal
                resolved = set(matched_by_tray.keys())
                batch_matched_total += len(resolved)
                unresolved.difference_update(resolved)
            if batch_matched_total:
                log.info(
                    "CCU targeted batch lookup matched=%s/%s unresolved=%s rows=%s chunks=%s",
                    batch_matched_total,
                    len(tray_ids_sorted),
                    len(unresolved),
                    raw_rows_total,
                    chunk_count,
                )

        if not unresolved:
            out = list(latest_by_tray.values())
            out.sort(key=lambda item: (item.collected_time, str(item.tray_id)))
            return out, raw_rows_total

        workers = max(1, int(settings.ccu_backfill_targeted_workers))
        sem = asyncio.Semaphore(workers)

        async def _fetch_one(tray_id: str) -> tuple[str, int, TraySignal | None, bool]:
            async with sem:
                rows = await self._fetch_latest_rows_for_tray_id(
                    tray_id=tray_id,
                    start_time=start_time,
                    end_time=end_time,
                )
            if not rows:
                return tray_id, 0, None, False
            signals = self._rows_to_signals(rows)
            matched = next(
                (
                    signal
                    for signal in signals
                    if _tray_ids_equivalent(str(signal.tray_id), tray_id)
                ),
                None,
            )
            used_fallback = False
            if matched is None:
                matched = _build_backfill_fallback_signal(tray_id=tray_id, rows=rows)
                used_fallback = matched is not None
            return tray_id, len(rows), matched, used_fallback

        fallback_ids = sorted(unresolved)
        tasks = [asyncio.create_task(_fetch_one(tray_id), name=f"ccu-backfill-{tray_id}") for tray_id in fallback_ids]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        fallback_matched = 0
        for tray_id, result in zip(fallback_ids, results, strict=True):
            if isinstance(result, Exception):
                log.warning("CCU targeted lookup failed tray_id=%s error=%r", tray_id, result)
                continue
            _, raw_rows, matched, used_fallback = result
            raw_rows_total += raw_rows
            if matched is not None:
                previous = latest_by_tray.get(tray_id)
                if previous is None or matched.collected_time >= previous.collected_time:
                    latest_by_tray[tray_id] = matched
            if used_fallback:
                fallback_matched += 1
        out = list(latest_by_tray.values())
        out.sort(key=lambda item: (item.collected_time, str(item.tray_id)))
        if fallback_matched:
            log.info(
                "CCU targeted lookup fallback matched=%s/%s",
                fallback_matched,
                len(fallback_ids),
            )
        return out, raw_rows_total

    async def _fetch_latest_rows_for_tray_ids_batch(
        self,
        *,
        tray_ids: set[str],
        start_time: datetime,
        end_time: datetime,
        rows_per_tray: int,
    ) -> list[dict[str, Any]]:
        if not tray_ids:
            return []
        patterns: list[str] = []
        for tray_id in sorted(tray_ids):
            patterns.extend(_tray_like_patterns(tray_id))
        if not patterns:
            return []
        like_patterns = list(dict.fromkeys(patterns))
        like_filter = " OR ".join(f"{self._record_col} LIKE ?" for _ in like_patterns)
        requested_rows = max(len(tray_ids) * max(20, rows_per_tray), len(tray_ids) * 40)
        max_rows = max(500, int(settings.ccu_backfill_targeted_batch_max_rows))
        top_n = min(max_rows, requested_rows)
        query = (
            f"SELECT TOP {top_n} "
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
            f"AND ({like_filter}) "
            "ORDER BY COALESCE(end_time, start_time) DESC"
        )
        params: list[Any] = [start_time, end_time, start_time, end_time, *like_patterns]
        return await self._conn.query_rows(
            query,
            params,
            query_name="ccu.backfill_latest_by_tray_ids_batch",
        )

    async def _fetch_latest_rows_for_tray_id(
        self,
        *,
        tray_id: str,
        start_time: datetime,
        end_time: datetime,
    ) -> list[dict[str, Any]]:
        if not tray_id:
            return []
        like_patterns = _tray_like_patterns(tray_id)
        if not like_patterns:
            return []
        like_filter = " OR ".join(f"{self._record_col} LIKE ?" for _ in like_patterns)
        query = (
            "SELECT TOP 120 "
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
            f"AND ({like_filter}) "
            "ORDER BY COALESCE(end_time, start_time) DESC"
        )
        params: list[Any] = [start_time, end_time, start_time, end_time, *like_patterns]
        return await self._conn.query_rows(
            query,
            params,
            query_name="ccu.backfill_latest_by_tray_id",
        )

    async def peek_latest_signal_time(self, end_time: datetime) -> datetime | None:
        query = (
            "SELECT TOP 1 latest_signal_time FROM ("
            f"SELECT MAX({self._end_col}) AS latest_signal_time "
            f"FROM {self._table} "
            f"WHERE {self._end_col} IS NOT NULL AND {self._end_col} <= ? "
            "UNION ALL "
            f"SELECT MAX({self._start_col}) AS latest_signal_time "
            f"FROM {self._table} "
            f"WHERE {self._end_col} IS NULL AND {self._start_col} <= ?"
            ") AS latest "
            "WHERE latest_signal_time IS NOT NULL "
            "ORDER BY latest_signal_time DESC"
        )
        rows = await self._conn.query_rows(
            query,
            [end_time, end_time],
            query_name="ccu.peek_latest_signal_time",
        )
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
            query_name="ccu.fetch_tray_cells",
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
            query_name="ccu.fetch_cell_owner",
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
        rows = await self._fetch_batch(
            start_time=start_time,
            end_time=end_time,
            offset=0,
            query_name="ccu.window_fetch_batch",
        )
        log.debug("CCU fetch range rows=%s start_time=%s end_time=%s", len(rows), start_time.isoformat(), end_time.isoformat())
        return rows

    async def _fetch_batch(
        self,
        *,
        start_time: datetime,
        end_time: datetime,
        offset: int,
        query_name: str = "ccu.fetch_batch",
    ) -> list[dict[str, Any]]:
        if offset > 0:
            # SQL is already deduped to one tray-bearing row per LOT_NO for this window.
            return []
        marker_filter = " OR ".join(f"{self._record_col} LIKE ?" for _ in self._sql_tray_markers)
        query = (
            "WITH cte AS ("
            "SELECT "
            f"{self._lot_col} AS lot_no, "
            f"{self._start_col} AS start_time, "
            f"{self._end_col} AS end_time, "
            f"{self._record_col} AS record_json, "
            f"{self._update_col} AS update_time, "
            "ROW_NUMBER() OVER (PARTITION BY "
            f"{self._lot_col} ORDER BY {self._update_col} DESC, {self._start_col} DESC, {self._end_col} DESC) AS rn "
            f"FROM {self._table} "
            f"WHERE {self._update_col} >= ? AND {self._update_col} <= ? "
            f"AND ({marker_filter})"
            ") "
            "SELECT lot_no, start_time, end_time, record_json, update_time "
            "FROM cte WHERE rn = 1 "
            "ORDER BY update_time ASC, lot_no ASC, start_time ASC, end_time ASC"
        )
        params: list[Any] = [
            start_time,
            end_time,
            *self._sql_tray_markers,
        ]
        return await self._conn.query_rows(query, params, query_name=query_name)

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
            raw_text = _stringify_record_json(raw_json, None)
            token = _extract_token(raw_text)
            lot_value = row.get("lot_no")
            lot_no = str(lot_value).strip() if lot_value is not None else ""
            if token is None and lot_no:
                token = _extract_token(lot_no)
            if self._keyword:
                if token is None:
                    skipped_no_token += 1
                    continue
                searchable = f"{raw_text} {lot_no} {token or ''}".upper()
                if self._keyword not in searchable:
                    skipped_no_token += 1
                    continue

            tray_key = ""
            tray_source = ""
            tray_text_direct = _find_text_value(raw_text, self._tray_key)
            if tray_text_direct is not None:
                tray_key = _normalize_tray_id(tray_text_direct)
                if not _is_plausible_tray_id(tray_key):
                    tray_key = ""
            if not tray_key:
                for key_name in ("TRAY_ID", "TRAYBARCODE", "TRAY_BARCODE"):
                    value = _find_text_value(raw_text, key_name)
                    if value is None:
                        continue
                    candidate = _normalize_tray_id(value)
                    if _is_plausible_tray_id(candidate):
                        tray_key = candidate
                        tray_source = "text"
                        break
            if tray_key and not tray_source:
                tray_source = "text"
            parsed_json = None
            if not tray_key:
                parsed_json = _parse_json(raw_json)
                if parsed_json is None:
                    skipped_bad_json += 1
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
                    "seen_lots": set(),
                    "start_at_pos_1": None,
                    "end_at_pos_400": None,
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

            if lot_no:
                seen_lots = stat["seen_lots"]
                if lot_no not in seen_lots:
                    seen_lots.add(lot_no)
                    stat["quantity"] += 1
            else:
                stat["quantity"] += 1
            if lot_no:
                stat["latest_lot_no"] = lot_no
            if token:
                stat["record_token"] = token

            pos_raw = _find_text_value(raw_text, self._pos_key)
            if pos_raw is None:
                pos_raw = _find_text_value(raw_text, "TRAY_POS")
            if pos_raw is None:
                if parsed_json is None:
                    parsed_json = _parse_json(raw_json)
                    if parsed_json is None:
                        skipped_bad_json += 1
                pos_raw = _find_json_value(parsed_json, self._pos_key)
            pos_value = _to_int(pos_raw)
            if pos_value is not None:
                # Business rule: start prefers position #1; fallback is minimum position.
                if pos_value == 1:
                    candidate_start = start_time or collected_time
                    existing_start = stat["start_at_pos_1"]
                    if candidate_start is not None and (
                        existing_start is None or candidate_start < existing_start
                    ):
                        stat["start_at_pos_1"] = candidate_start
                # Business rule: end prefers position #400; fallback is maximum position.
                if pos_value == 400:
                    candidate_end = end_time or collected_time
                    existing_end = stat["end_at_pos_400"]
                    if candidate_end is not None and (
                        existing_end is None or candidate_end > existing_end
                    ):
                        stat["end_at_pos_400"] = candidate_end
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
            start_dt = stat["start_at_pos_1"] or stat["start_at_min_pos"] or stat["first_start_time"]
            end_dt = (
                stat["end_at_pos_400"]
                or stat["end_at_max_pos"]
                or stat["last_end_time"]
                or stat["latest_signal_time"]
            )
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


def _build_sql_marker_patterns(primary_key: str, *, fallback_keys: tuple[str, ...]) -> tuple[str, ...]:
    keys = [primary_key, *fallback_keys]
    out: list[str] = []
    seen: set[str] = set()
    for key in keys:
        normalized = str(key).strip()
        if not normalized:
            continue
        upper = normalized.upper()
        if upper in seen:
            continue
        seen.add(upper)
        out.append(f"%{normalized}%")
    return tuple(out)


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


def _tray_like_patterns(raw_value: str) -> list[str]:
    normalized = _normalize_tray_id(raw_value)
    if not normalized:
        return []
    # Keep exact and flexible patterns so we can match barcode variants with separators.
    exact = f"%{normalized}%"
    flexible = "%" + "%".join(normalized) + "%"
    if flexible == exact:
        return [exact]
    return [exact, flexible]


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


def _collect_latest_by_tray_from_rows(
    *,
    rows: list[dict[str, Any]],
    wanted: set[str],
    latest_by_tray: dict[str, TraySignal],
    row_parser,
) -> None:
    if not rows or not wanted:
        return
    for signal in row_parser(rows):
        tray_key = str(signal.tray_id)
        if tray_key not in wanted:
            continue
        previous = latest_by_tray.get(tray_key)
        if previous is None or signal.collected_time >= previous.collected_time:
            latest_by_tray[tray_key] = signal


def _chunked(values: list[str], chunk_size: int) -> list[list[str]]:
    if chunk_size <= 0:
        return [values]
    out: list[list[str]] = []
    for i in range(0, len(values), chunk_size):
        out.append(values[i : i + chunk_size])
    return out


def _match_latest_signals_to_tray_ids(
    *,
    signals: list[TraySignal],
    tray_ids: set[str],
) -> dict[str, TraySignal]:
    if not signals or not tray_ids:
        return {}
    wanted = sorted(tray_ids)
    matched: dict[str, TraySignal] = {}
    for signal in signals:
        signal_tray_id = str(signal.tray_id)
        for wanted_tray_id in wanted:
            if not _tray_ids_equivalent(signal_tray_id, wanted_tray_id):
                continue
            previous = matched.get(wanted_tray_id)
            if previous is None or signal.collected_time >= previous.collected_time:
                matched[wanted_tray_id] = signal
    return matched


def _tray_ids_equivalent(candidate: str, wanted: str) -> bool:
    candidate_norm = _normalize_tray_id(candidate)
    wanted_norm = _normalize_tray_id(wanted)
    if not candidate_norm or not wanted_norm:
        return False
    if candidate_norm == wanted_norm:
        return True
    return (candidate_norm in wanted_norm) or (wanted_norm in candidate_norm)


def _build_backfill_fallback_signal(*, tray_id: str, rows: list[dict[str, Any]]) -> TraySignal | None:
    if not rows:
        return None

    latest_signal_time: datetime | None = None
    first_start: datetime | None = None
    last_end: datetime | None = None
    latest_lot_no: str | None = None
    record_token: str | None = None

    for row in rows:
        start_time = _coerce_datetime(row.get("start_time"))
        end_time = _coerce_datetime(row.get("end_time"))
        collected_time = end_time or start_time
        if collected_time is not None and (latest_signal_time is None or collected_time > latest_signal_time):
            latest_signal_time = collected_time
        if start_time is not None and (first_start is None or start_time < first_start):
            first_start = start_time
        if end_time is not None and (last_end is None or end_time > last_end):
            last_end = end_time

        lot_value = row.get("lot_no")
        lot_no = str(lot_value).strip() if lot_value is not None else ""
        if lot_no:
            latest_lot_no = lot_no
        raw_json = row.get("record_json")
        parsed_json = _parse_json(raw_json)
        raw_text = _stringify_record_json(raw_json, parsed_json)
        token = _extract_token(raw_text)
        if token is None and lot_no:
            token = _extract_token(lot_no)
        if token:
            record_token = token

    if latest_signal_time is None:
        return None

    payload = {
        "lot_no": latest_lot_no,
        "quantity": None,
        "start_time": first_start.isoformat() if first_start is not None else None,
        "end_time": (last_end or latest_signal_time).isoformat(),
        "min_position": None,
        "max_position": None,
        "record_token": record_token,
        "matched_by": "targeted_fallback",
    }
    return TraySignal(
        source=SignalSource.CCU,
        tray_id=TrayId(_normalize_tray_id(tray_id) or tray_id.strip()),
        collected_time=latest_signal_time,
        payload=payload,
    )
