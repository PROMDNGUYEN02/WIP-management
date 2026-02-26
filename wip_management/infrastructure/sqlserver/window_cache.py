from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Any, Awaitable, Callable

log = logging.getLogger(__name__)


class SQLWindowRowCache:
    """
    Reuse SQL rows across overlapping time-window requests.

    Pattern:
    - First request: fetch full window and cache rows.
    - Next request with same start and newer end: fetch only incremental tail and merge.
    - Request inside cached window: return filtered rows from memory.
    """

    def __init__(
        self,
        *,
        name: str,
        overlap_seconds: float,
        max_rows: int,
    ) -> None:
        self._name = name
        self._overlap = timedelta(seconds=max(0.0, overlap_seconds))
        self._max_rows = max(1000, int(max_rows))
        self._lock = asyncio.Lock()
        self._start: datetime | None = None
        self._end: datetime | None = None
        self._rows: list[dict[str, Any]] = []
        self._keys: set[tuple[str, str, str, int]] = set()

    async def fetch(
        self,
        *,
        start_time: datetime,
        end_time: datetime,
        fetch_uncached: Callable[[datetime, datetime], Awaitable[list[dict[str, Any]]]],
    ) -> list[dict[str, Any]]:
        if end_time < start_time:
            return []
        async with self._lock:
            if self._can_serve_from_cache(start_time=start_time, end_time=end_time):
                return self._slice_rows(start_time=start_time, end_time=end_time)

            if self._can_extend_tail(start_time=start_time, end_time=end_time):
                assert self._end is not None
                delta_start = max(start_time, self._end - self._overlap)
                if end_time > self._end:
                    delta_rows = await fetch_uncached(delta_start, end_time)
                    self._merge_rows(delta_rows)
                    self._end = end_time
                return self._slice_rows(start_time=start_time, end_time=end_time)

            rows = await fetch_uncached(start_time, end_time)
            self._reset(start_time=start_time, end_time=end_time, rows=rows)
            return [dict(item) for item in self._rows]

    def _can_serve_from_cache(self, *, start_time: datetime, end_time: datetime) -> bool:
        if self._start is None or self._end is None or not self._rows:
            return False
        return start_time >= self._start and end_time <= self._end

    def _can_extend_tail(self, *, start_time: datetime, end_time: datetime) -> bool:
        if self._start is None or self._end is None or not self._rows:
            return False
        return start_time == self._start and end_time >= self._end

    def _reset(self, *, start_time: datetime, end_time: datetime, rows: list[dict[str, Any]]) -> None:
        self._start = start_time
        self._end = end_time
        self._rows.clear()
        self._keys.clear()
        self._merge_rows(rows)
        log.debug(
            "%s range cache reset start=%s end=%s rows=%s",
            self._name,
            start_time.isoformat(),
            end_time.isoformat(),
            len(self._rows),
        )

    def _merge_rows(self, rows: list[dict[str, Any]]) -> None:
        for row in rows:
            key = _row_key(row)
            if key in self._keys:
                continue
            self._keys.add(key)
            self._rows.append(dict(row))

        overflow = len(self._rows) - self._max_rows
        if overflow > 0:
            self._rows = self._rows[overflow:]
            self._keys = {_row_key(item) for item in self._rows}
        log.debug("%s range cache merged rows=%s total=%s", self._name, len(rows), len(self._rows))

    def _slice_rows(self, *, start_time: datetime, end_time: datetime) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for row in self._rows:
            signal_time = _row_signal_time(row)
            if signal_time is None:
                continue
            if start_time <= signal_time <= end_time:
                out.append(dict(row))
        log.debug(
            "%s range cache hit start=%s end=%s rows=%s",
            self._name,
            start_time.isoformat(),
            end_time.isoformat(),
            len(out),
        )
        return out


def _row_signal_time(row: dict[str, Any]) -> datetime | None:
    end_time = _to_datetime(row.get("end_time"))
    if end_time is not None:
        return end_time
    return _to_datetime(row.get("start_time"))


def _row_key(row: dict[str, Any]) -> tuple[str, str, str, int]:
    lot_no = str(row.get("lot_no") or "")
    start_key = _datetime_key(_to_datetime(row.get("start_time")))
    end_key = _datetime_key(_to_datetime(row.get("end_time")))
    # Hash keeps key compact even if record_json is large.
    record_hash = hash(str(row.get("record_json") or ""))
    return (lot_no, start_key, end_key, record_hash)


def _datetime_key(value: datetime | None) -> str:
    if value is None:
        return ""
    return value.isoformat(sep=" ", timespec="seconds")


def _to_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        normalized = text.replace("T", " ")
        for candidate in (normalized, f"{normalized}Z"):
            try:
                return datetime.fromisoformat(candidate.replace("Z", "+00:00"))
            except ValueError:
                continue
    return None
