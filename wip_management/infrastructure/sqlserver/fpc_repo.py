from __future__ import annotations

import json
import logging
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any

from wip_management.config import settings
from wip_management.domain.models.tray import SignalSource, TrayId, TraySignal, Watermark
from wip_management.infrastructure.sqlserver.connection import SQLServerConnection

_TOKEN_PATTERN = re.compile(r"V[A-Z]\d{4,}", re.IGNORECASE)
log = logging.getLogger(__name__)


class FpcRepo:
    def __init__(self, connection: SQLServerConnection) -> None:
        self._conn = connection
        self._table = SQLServerConnection.table_name(settings.sql_schema, settings.fpc_table)
        self._lot_col = SQLServerConnection.column_name(settings.fpc_lot_column)
        self._start_col = SQLServerConnection.column_name(settings.fpc_start_column)
        self._end_col = SQLServerConnection.column_name(settings.fpc_end_column)
        self._record_col = SQLServerConnection.column_name(settings.fpc_record_json_column)
        self._tray_key = settings.fpc_json_tray_id_key
        self._cell_key = settings.fpc_json_pos_key
        self._keyword = settings.record_filter_keyword.strip().upper()

    async def start(self) -> None:
        await self._conn.start()

    async def close(self) -> None:
        await self._conn.close()

    async def fetch_initial(self, start_time: datetime, end_time: datetime) -> list[TraySignal]:
        log.info("FPC initial fetch start start_time=%s end_time=%s", start_time.isoformat(), end_time.isoformat())
        started_at = time.perf_counter()
        rows = await self._fetch_range(start_time=start_time, end_time=end_time)
        signals = self._rows_to_signals(rows)
        elapsed_ms = int((time.perf_counter() - started_at) * 1000)
        log.info(
            "FPC initial fetch done elapsed_ms=%s raw_rows=%s signals=%s",
            elapsed_ms,
            len(rows),
            len(signals),
        )
        return signals

    async def fetch_delta(self, watermark: Watermark, end_time: datetime) -> list[TraySignal]:
        overlap_start = watermark.collected_time - timedelta(seconds=settings.delta_overlap_seconds)
        log.debug(
            "FPC delta fetch start watermark=%s overlap_start=%s end_time=%s",
            watermark.collected_time.isoformat(),
            overlap_start.isoformat(),
            end_time.isoformat(),
        )
        started_at = time.perf_counter()
        rows = await self._fetch_range(start_time=overlap_start, end_time=end_time)
        signals = self._rows_to_signals(rows)
        elapsed_ms = int((time.perf_counter() - started_at) * 1000)
        log.debug("FPC delta fetch done elapsed_ms=%s raw_rows=%s signals=%s", elapsed_ms, len(rows), len(signals))
        return signals

    async def peek_latest_signal_time(self, end_time: datetime) -> datetime | None:
        query_end = (
            f"SELECT TOP 1 {self._end_col} AS latest_signal_time "
            f"FROM {self._table} "
            f"WHERE {self._end_col} IS NOT NULL AND {self._end_col} <= ? "
            f"ORDER BY {self._end_col} DESC"
        )
        rows_end = await self._conn.query_rows(query_end, [end_time])
        if rows_end:
            latest_end = _coerce_datetime(rows_end[0].get("latest_signal_time"))
            if latest_end is not None:
                log.debug("FPC peek latest signal from end_time=%s", latest_end.isoformat())
                return latest_end

        query_start = (
            f"SELECT TOP 1 {self._start_col} AS latest_signal_time "
            f"FROM {self._table} "
            f"WHERE {self._end_col} IS NULL AND {self._start_col} <= ? "
            f"ORDER BY {self._start_col} DESC"
        )
        rows_start = await self._conn.query_rows(query_start, [end_time])
        if not rows_start:
            log.debug("FPC peek latest returned empty rowset")
            return None
        latest_start = _coerce_datetime(rows_start[0].get("latest_signal_time"))
        if latest_start is None:
            log.debug("FPC peek latest start_time was not parseable")
            return None
        log.debug("FPC peek latest signal from start_time=%s", latest_start.isoformat())
        return latest_start

    async def _fetch_range(self, start_time: datetime, end_time: datetime) -> list[dict[str, Any]]:
        all_rows: list[dict[str, Any]] = []
        offset = 0
        page = 0
        while True:
            batch = await self._fetch_batch(start_time=start_time, end_time=end_time, offset=offset)
            if not batch:
                log.debug("FPC fetch range page=%s offset=%s returned empty", page, offset)
                break
            all_rows.extend(batch)
            log.debug("FPC fetch range page=%s offset=%s batch_rows=%s", page, offset, len(batch))
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
            "SELECT lot_no, start_time, end_time, record_json FROM ("
            " SELECT "
            f"{self._lot_col} AS lot_no, "
            f"{self._start_col} AS start_time, "
            f"{self._end_col} AS end_time, "
            f"{self._record_col} AS record_json "
            f" FROM {self._table} "
            f" WHERE {self._end_col} IS NOT NULL AND {self._end_col} >= ? AND {self._end_col} <= ? "
            " UNION ALL "
            " SELECT "
            f"{self._lot_col} AS lot_no, "
            f"{self._start_col} AS start_time, "
            f"{self._end_col} AS end_time, "
            f"{self._record_col} AS record_json "
            f" FROM {self._table} "
            f" WHERE {self._end_col} IS NULL AND {self._start_col} >= ? AND {self._start_col} <= ? "
            ") AS source_rows "
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
                fallback_keys=("TRAYID", "TRAY_ID", "TRAYBARCODE", "TRAY_BARCODE"),
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

            stat = per_tray.get(tray_key)
            if stat is None:
                stat = {
                    "cell_count": 0,
                    "first_start_time": None,
                    "last_end_time": None,
                    "latest_signal_time": collected_time,
                    "latest_lot_no": lot_no or None,
                    "record_token": token,
                }
                per_tray[tray_key] = stat

            stat["cell_count"] += 1
            if lot_no:
                stat["latest_lot_no"] = lot_no
            if token:
                stat["record_token"] = token

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
            payload = {
                "lot_no": stat["latest_lot_no"],
                "cell_count": stat["cell_count"],
                "precharge_start_time": stat["first_start_time"].isoformat() if stat["first_start_time"] else None,
                "precharge_end_time": stat["last_end_time"].isoformat() if stat["last_end_time"] else None,
                "record_token": stat["record_token"],
            }
            out.append(
                TraySignal(
                    source=SignalSource.FPC,
                    tray_id=TrayId(tray_text),
                    collected_time=stat["latest_signal_time"],
                    payload=payload,
                )
            )

        out.sort(key=lambda item: (item.collected_time, str(item.tray_id)))
        log_level = log.warning if rows and not out else log.debug
        log_level(
            "FPC convert rows done input=%s output=%s skipped_no_time=%s skipped_bad_json=%s skipped_no_token=%s skipped_no_tray=%s missing_lot_rows=%s parsed_from_text=%s tray_from_token=%s keyword=%s",
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
