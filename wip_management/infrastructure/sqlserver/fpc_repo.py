from __future__ import annotations

import json
import re
from datetime import datetime, timedelta
from typing import Any

from wip_management.config import settings
from wip_management.domain.models.tray import SignalSource, TrayId, TraySignal, Watermark
from wip_management.infrastructure.sqlserver.connection import SQLServerConnection

_TOKEN_PATTERN = re.compile(r"\bVB\d+\b", re.IGNORECASE)


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
        self._signal_expr = f"COALESCE({self._end_col}, {self._start_col})"

    async def start(self) -> None:
        await self._conn.start()

    async def close(self) -> None:
        await self._conn.close()

    async def fetch_initial(self, start_time: datetime, end_time: datetime) -> list[TraySignal]:
        rows = await self._fetch_range(start_time=start_time, end_time=end_time)
        return self._rows_to_signals(rows)

    async def fetch_delta(self, watermark: Watermark, end_time: datetime) -> list[TraySignal]:
        overlap_start = watermark.collected_time - timedelta(seconds=settings.delta_overlap_seconds)
        rows = await self._fetch_range(start_time=overlap_start, end_time=end_time)
        return self._rows_to_signals(rows)

    async def peek_latest_signal_time(self, end_time: datetime) -> datetime | None:
        where_parts, params = self._base_where(start_time=None, end_time=end_time)
        query = (
            f"SELECT MAX({self._signal_expr}) AS latest_signal_time "
            f"FROM {self._table} "
            f"WHERE {' AND '.join(where_parts)}"
        )
        rows = await self._conn.query_rows(query, params)
        if not rows:
            return None
        latest = rows[0].get("latest_signal_time")
        if isinstance(latest, datetime):
            return latest
        return None

    async def _fetch_range(self, start_time: datetime, end_time: datetime) -> list[dict[str, Any]]:
        all_rows: list[dict[str, Any]] = []
        offset = 0
        while True:
            batch = await self._fetch_batch(start_time=start_time, end_time=end_time, offset=offset)
            if not batch:
                break
            all_rows.extend(batch)
            if len(batch) < settings.max_fetch_batch:
                break
            offset += len(batch)
        return all_rows

    async def _fetch_batch(
        self,
        *,
        start_time: datetime,
        end_time: datetime,
        offset: int,
    ) -> list[dict[str, Any]]:
        where_parts, params = self._base_where(start_time=start_time, end_time=end_time)
        query = (
            "SELECT "
            f"{self._lot_col} AS lot_no, "
            f"{self._start_col} AS start_time, "
            f"{self._end_col} AS end_time, "
            f"{self._record_col} AS record_json "
            f"FROM {self._table} "
            f"WHERE {' AND '.join(where_parts)} "
            f"ORDER BY {self._signal_expr} ASC, {self._lot_col} ASC, {self._start_col} ASC, {self._end_col} ASC "
            "OFFSET ? ROWS FETCH NEXT ? ROWS ONLY"
        )
        params.extend([offset, settings.max_fetch_batch])
        return await self._conn.query_rows(query, params)

    def _base_where(self, *, start_time: datetime | None, end_time: datetime) -> tuple[list[str], list[Any]]:
        where_parts = [f"{self._signal_expr} <= ?"]
        params: list[Any] = [end_time]
        if start_time is not None:
            where_parts.insert(0, f"{self._signal_expr} >= ?")
            params.insert(0, start_time)
        if self._keyword:
            where_parts.append(f"{self._record_col} LIKE ?")
            params.append(f"%{self._keyword}%")
        return where_parts, params

    def _rows_to_signals(self, rows: list[dict[str, Any]]) -> list[TraySignal]:
        out: list[TraySignal] = []
        for row in rows:
            start_time = row.get("start_time")
            end_time = row.get("end_time")
            collected_time = end_time if isinstance(end_time, datetime) else start_time
            if not isinstance(collected_time, datetime):
                continue

            raw_json = row.get("record_json")
            parsed_json = _parse_json(raw_json)
            if parsed_json is None:
                continue

            raw_text = raw_json if isinstance(raw_json, str) else json.dumps(parsed_json, ensure_ascii=True)
            token = _extract_token(raw_text)
            if token is None:
                continue
            if self._keyword and self._keyword not in token and self._keyword not in raw_text.upper():
                continue

            tray_id = _find_json_value(parsed_json, self._tray_key)
            if tray_id is None:
                continue
            tray_text = str(tray_id).strip()
            if not tray_text:
                continue

            payload = {
                "lot_no": row.get("lot_no"),
                "start_time": start_time.isoformat() if isinstance(start_time, datetime) else None,
                "end_time": end_time.isoformat() if isinstance(end_time, datetime) else None,
                "record_json": parsed_json,
                "cell_position": _find_json_value(parsed_json, self._cell_key),
                "record_token": token,
            }
            out.append(
                TraySignal(
                    source=SignalSource.FPC,
                    tray_id=TrayId(tray_text),
                    collected_time=collected_time,
                    payload=payload,
                )
            )
        out.sort(key=lambda item: (item.collected_time, str(item.tray_id)))
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
