# wip_management/infrastructure/sqlserver/fpc_repo.py
from __future__ import annotations

import asyncio
import atexit
import logging
import threading
import time
from collections.abc import AsyncIterator
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from typing import Any

from wip_management.config import settings
from wip_management.domain.models.tray import SignalSource, TrayId, TraySignal, Watermark
from wip_management.infrastructure.sqlserver.connection import SQLServerConnection

log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Fast JSON parsing
# ─────────────────────────────────────────────────────────────────────────────
try:
    import orjson

    def _fast_json_loads(s: str | bytes) -> dict:
        if isinstance(s, str):
            s = s.encode("utf-8")
        return orjson.loads(s)
except ImportError:
    import json

    def _fast_json_loads(s: str | bytes) -> dict:
        if isinstance(s, bytes):
            s = s.decode("utf-8")
        return json.loads(s)


# ─────────────────────────────────────────────────────────────────────────────
# Thread Pool
# ─────────────────────────────────────────────────────────────────────────────
_PARSE_EXECUTOR: ThreadPoolExecutor | None = None
_EXECUTOR_LOCK = threading.Lock()


def _get_parse_executor() -> ThreadPoolExecutor:
    global _PARSE_EXECUTOR
    if _PARSE_EXECUTOR is None:
        with _EXECUTOR_LOCK:
            if _PARSE_EXECUTOR is None:
                _PARSE_EXECUTOR = ThreadPoolExecutor(
                    max_workers=2,
                    thread_name_prefix="fpc-parse",
                )
                atexit.register(_shutdown_executor)
    return _PARSE_EXECUTOR


def _shutdown_executor() -> None:
    global _PARSE_EXECUTOR
    if _PARSE_EXECUTOR is not None:
        _PARSE_EXECUTOR.shutdown(wait=False, cancel_futures=True)
        _PARSE_EXECUTOR = None


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────
_BAD_PREFIXES = frozenset((
    "FIELDNAME", "PARAMNAME", "MAXVALUE", "WIPID",
    "RECORDTIME", "CREATEUSER", "CREATEDATE", "STDVALUE",
))


def _parse_record_json(raw: str) -> dict[str, str]:
    """
    Parse RECORD_JSON string into a flat dict.
    Safely handles None, empty, and short strings — no DB-side LEN() needed.
    """
    if not raw or len(raw) < 10:
        return {}
    try:
        outer = _fast_json_loads(raw)
    except (ValueError, TypeError):
        return {}
    if not isinstance(outer, dict):
        return {}
    result: dict[str, str] = {}
    for field_name, field_obj in outer.items():
        if isinstance(field_obj, dict):
            v = field_obj.get("paramValue")
            if v is not None:
                result[field_name] = str(v)
        elif isinstance(field_obj, (str, int, float)):
            result[field_name] = str(field_obj)
    return result


def _is_valid_tray_id(value: str) -> bool:
    if not value:
        return False
    length = len(value)
    if length < 6 or length > 40:
        return False
    has_digit = any(c.isdigit() for c in value)
    if not has_digit:
        return False
    upper_value = value.upper()
    return not any(upper_value.startswith(p) for p in _BAD_PREFIXES)


def _coerce_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.replace(tzinfo=None) if value.tzinfo else value
    text = str(value).strip()
    if not text or len(text) < 10:
        return None
    try:
        normalized = text.replace("Z", "").replace("T", " ")
        if "." in normalized:
            normalized = normalized[:26]
        return datetime.fromisoformat(normalized)
    except ValueError:
        pass
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(text[:26], fmt)
        except ValueError:
            continue
    return None


def _decode_cell_status(raw_status: str) -> tuple[bool, bool]:
    if not raw_status:
        return False, False
    s = raw_status.strip().upper()
    try:
        val = float(s)
        if val >= 100:
            return True, False
        if val == 0:
            return False, True
        return False, False
    except ValueError:
        pass
    if s in ("OK", "PASS", "GOOD", "1"):
        return True, False
    if s in ("NG", "FAIL", "BAD", "0"):
        return False, True
    return False, False


def _is_summary_record(fields: dict[str, str], tray_key: str) -> bool:
    return bool(fields.get(tray_key, "").strip())


def _parse_fpc_rows_sync(
    rows: list[dict[str, Any]],
    tray_key: str,
    pos_key: str,
    keyword: str,
) -> list[TraySignal]:
    per_tray: dict[str, dict[str, Any]] = {}
    skipped_step = 0
    skipped_other = 0
    parsed = 0
    keyword_upper = keyword.upper() if keyword else ""

    for row in rows:
        start_time = _coerce_datetime(row.get("start_time"))
        end_time = _coerce_datetime(row.get("end_time"))
        collected = end_time or start_time
        if collected is None:
            skipped_other += 1
            continue

        raw_json = row.get("record_json")
        if not raw_json:
            skipped_other += 1
            continue

        raw_text = str(raw_json)
        if keyword_upper and keyword_upper not in raw_text.upper():
            skipped_other += 1
            continue

        fields = _parse_record_json(raw_text)
        if not fields:
            skipped_other += 1
            continue

        if not _is_summary_record(fields, tray_key):
            skipped_step += 1
            continue

        raw_tray = fields.get(tray_key, "").strip()
        tray_id = raw_tray.upper()
        if not _is_valid_tray_id(tray_id):
            skipped_other += 1
            continue

        raw_pos = fields.get(pos_key, "").strip()
        pos_value: int | None = None
        if raw_pos:
            try:
                pos_value = int(float(raw_pos))
            except (ValueError, TypeError):
                pass

        lot_no = (
            fields.get("LOT_NO", "").strip()
            or str(row.get("lot_no") or "").strip()
        )
        cell_barcode = fields.get("Cell_barcode", "").strip()
        cell_status = fields.get("Cell_Status", "")
        machine_id = fields.get("Machine_ID", "").strip()
        line_code = fields.get("Line_code", "").strip()
        total_cap = fields.get("Total_Capacity", "").strip()
        yield_rate = fields.get("Yield_rate", "").strip()
        input_num = fields.get("Input_number", "").strip()
        output_num = fields.get("Output_number", "").strip()
        process_name = fields.get("Process name", "").strip()
        cell_batch = fields.get("Cell_batch", "").strip()
        is_ok, is_ng = _decode_cell_status(cell_status)

        if tray_id not in per_tray:
            per_tray[tray_id] = {
                "seen_positions": set(),
                "lot_nos": set(),
                "cell_barcodes": set(),
                "latest_lot": None,
                "latest_cell_bc": None,
                "min_pos": None,
                "max_pos": None,
                "first_start": None,
                "last_end": None,
                "latest_time": collected,
                "machine_id": machine_id or None,
                "line_code": line_code or None,
                "process_name": process_name or None,
                "cell_batch": cell_batch or None,
                "latest_yield": yield_rate or None,
                "latest_cap": total_cap or None,
                "input_number": input_num or None,
                "output_number": output_num or None,
                "cell_ok": 0,
                "cell_ng": 0,
            }

        stat = per_tray[tray_id]

        if pos_value is not None:
            stat["seen_positions"].add(pos_value)
            if stat["min_pos"] is None or pos_value < stat["min_pos"]:
                stat["min_pos"] = pos_value
            if stat["max_pos"] is None or pos_value > stat["max_pos"]:
                stat["max_pos"] = pos_value

        if lot_no:
            stat["lot_nos"].add(lot_no)
            stat["latest_lot"] = lot_no
        if cell_barcode:
            stat["cell_barcodes"].add(cell_barcode)
            stat["latest_cell_bc"] = cell_barcode
        if is_ok:
            stat["cell_ok"] += 1
        elif is_ng:
            stat["cell_ng"] += 1
        if machine_id:
            stat["machine_id"] = machine_id
        if line_code:
            stat["line_code"] = line_code
        if process_name:
            stat["process_name"] = process_name
        if cell_batch:
            stat["cell_batch"] = cell_batch
        if yield_rate:
            stat["latest_yield"] = yield_rate
        if total_cap:
            stat["latest_cap"] = total_cap
        if input_num:
            stat["input_number"] = input_num
        if output_num:
            stat["output_number"] = output_num

        if start_time and (
            stat["first_start"] is None or start_time < stat["first_start"]
        ):
            stat["first_start"] = start_time
        if end_time and (stat["last_end"] is None or end_time > stat["last_end"]):
            stat["last_end"] = end_time
        if collected > stat["latest_time"]:
            stat["latest_time"] = collected

        parsed += 1

    out: list[TraySignal] = []
    for tray_id, stat in per_tray.items():
        quantity = (
            len(stat["seen_positions"])
            if stat["seen_positions"]
            else len(stat["lot_nos"])
        )
        payload: dict[str, Any] = {
            "lot_no": stat["latest_lot"],
            "cell_barcode": stat["latest_cell_bc"],
            "quantity": quantity,
            "lot_count": len(stat["lot_nos"]),
            "start_time": (
                stat["first_start"].isoformat() if stat["first_start"] else None
            ),
            "end_time": (
                stat["last_end"].isoformat() if stat["last_end"] else None
            ),
            "min_position": stat["min_pos"],
            "max_position": stat["max_pos"],
            "machine_id": stat["machine_id"],
            "line_code": stat["line_code"],
            "process_name": stat["process_name"],
            "cell_batch": stat["cell_batch"],
            "yield_rate": stat["latest_yield"],
            "total_capacity": stat["latest_cap"],
            "input_number": stat["input_number"],
            "output_number": stat["output_number"],
            "cell_ok_count": stat["cell_ok"],
            "cell_ng_count": stat["cell_ng"],
            "precharge_start_time": (
                stat["first_start"].isoformat() if stat["first_start"] else None
            ),
            "precharge_end_time": (
                stat["last_end"].isoformat() if stat["last_end"] else None
            ),
            "cell_count": quantity,
        }
        out.append(TraySignal(
            source=SignalSource.FPC,
            tray_id=TrayId(tray_id),
            collected_time=stat["latest_time"],
            payload=payload,
        ))

    out.sort(key=lambda s: (s.collected_time, str(s.tray_id)))
    return out


class FpcRepo:
    """
    FPC Repository with:
    - Fix #1: IS_HIS + DEL_FLAG row filters (configurable via .env)
    - Fix #5: Keyset pagination (index seek per page, no OFFSET re-scan)
    - Fix #4: Stuck-watermark guard (delta fetches skip dead windows)
    - Perf:   Removed LEN(RECORD_JSON) > 10 — non-sargable filter that
              forces SQL Server to read every blob.  The Python parser
              already handles NULL/empty/short JSON safely.
    """

    def __init__(self, connection: SQLServerConnection) -> None:
        self._conn = connection
        self._table = SQLServerConnection.table_name(
            settings.sql_schema, settings.fpc_table
        )
        self._lot_col = SQLServerConnection.column_name(settings.fpc_lot_column)
        self._start_col = SQLServerConnection.column_name(settings.fpc_start_column)
        self._end_col = SQLServerConnection.column_name(settings.fpc_end_column)
        self._update_col = SQLServerConnection.column_name(settings.update_time_column)
        self._record_col = SQLServerConnection.column_name(
            settings.fpc_record_json_column
        )
        self._tray_key = settings.fpc_json_tray_id_key
        self._pos_key = settings.fpc_json_pos_key
        self._keyword = settings.record_filter_keyword.strip().upper()

        # Build the WHERE filter clauses from config (Fix #1)
        self._extra_where = self._build_extra_where()

        # Density probe cache/controls (startup-cost mitigation)
        self._density_probe_lock: asyncio.Lock | None = None
        self._density_cache: dict[str, Any] | None = None
        self._density_cache_time: datetime | None = None

    # ── Fix #1: build IS_HIS / DEL_FLAG filter ───────────────────────────────
    def _build_extra_where(self) -> str:
        parts: list[str] = []
        if settings.fpc_filter_is_his:
            is_his_col = SQLServerConnection.column_name(settings.fpc_is_his_column)
            val = settings.fpc_is_his_active_value
            parts.append(f"  AND {is_his_col} = '{val}'")
        if settings.fpc_filter_del_flag:
            del_col = SQLServerConnection.column_name(settings.fpc_del_flag_column)
            del_val = settings.fpc_del_flag_deleted_value
            parts.append(
                f"  AND ({del_col} IS NULL OR {del_col} <> '{del_val}')"
            )
        return "\n".join(parts)

    async def start(self) -> None:
        pass

    async def close(self) -> None:
        pass

    # ── Diagnostic: density probe ─────────────────────────────────────────────
    async def probe_density(
        self, start_time: datetime, end_time: datetime
    ) -> None:
        if not settings.fpc_density_probe_enabled:
            return
        ttl = timedelta(seconds=settings.fpc_density_probe_cache_ttl_seconds)

        cached = self._get_density_cache(ttl)
        if cached is not None:
            self._log_density_stats(cached, cache_hit=True)
            return

        if self._density_probe_lock is None:
            self._density_probe_lock = asyncio.Lock()

        async with self._density_probe_lock:
            cached = self._get_density_cache(ttl)
            if cached is not None:
                self._log_density_stats(cached, cache_hit=True)
                return

            try:
                query = f"""
                SELECT
                    COUNT(*)  AS total_rows,
                    COUNT(DISTINCT {self._lot_col}) AS distinct_lots,
                    SUM(CASE WHEN [IS_HIS] = '0' THEN 1 ELSE 0 END) AS active_rows,
                    SUM(CASE WHEN [IS_HIS] = '1' THEN 1 ELSE 0 END) AS historical_rows,
                    SUM(CASE WHEN [DEL_FLAG] = '1' THEN 1 ELSE 0 END) AS deleted_rows
                FROM {self._table} WITH (NOLOCK)
                WHERE {self._update_col} >= ?
                  AND {self._update_col} <= ?
                """
                rows = await self._conn.query_rows(
                    query,
                    [start_time, end_time],
                    query_name="fpc.density_probe",
                )
                if not rows:
                    return

                parsed = self._parse_density_row(rows[0])
                self._density_cache = parsed
                self._density_cache_time = datetime.now()
                self._log_density_stats(parsed, cache_hit=False)
            except Exception:
                log.warning("FPC density probe failed (non-fatal)", exc_info=True)

    def _get_density_cache(self, ttl: timedelta) -> dict[str, Any] | None:
        if self._density_cache is None or self._density_cache_time is None:
            return None
        if datetime.now() - self._density_cache_time >= ttl:
            return None
        return dict(self._density_cache)

    def _parse_density_row(self, row: dict[str, Any]) -> dict[str, Any]:
        total = int(row.get("total_rows", 0) or 0)
        active = int(row.get("active_rows", 0) or 0)
        historical = int(row.get("historical_rows", 0) or 0)
        deleted = int(row.get("deleted_rows", 0) or 0)
        lots = int(row.get("distinct_lots", 0) or 0)
        filter_pct = round(active / total * 100, 1) if total else 0.0
        return {
            "total_rows": total,
            "active_rows": active,
            "historical_rows": historical,
            "deleted_rows": deleted,
            "distinct_lots": lots,
            "filter_keeps_pct": filter_pct,
        }

    def _log_density_stats(self, stats: dict[str, Any], *, cache_hit: bool) -> None:
        total = int(stats.get("total_rows", 0) or 0)
        active = int(stats.get("active_rows", 0) or 0)
        historical = int(stats.get("historical_rows", 0) or 0)
        deleted = int(stats.get("deleted_rows", 0) or 0)
        lots = int(stats.get("distinct_lots", 0) or 0)
        filter_pct = float(stats.get("filter_keeps_pct", 0.0) or 0.0)
        cache_tag = "cached" if cache_hit else "fresh"
        log.info(
            "FPC density probe (%s) total=%s active(IS_HIS=0)=%s "
            "historical=%s deleted=%s lots=%s filter_keeps=%.1f%%",
            cache_tag,
            total,
            active,
            historical,
            deleted,
            lots,
            filter_pct,
        )
        if total > 0 and active == 0:
            log.warning(
                "FPC IS_HIS filter returns 0 rows! "
                "Consider setting fpc_filter_is_his=false in .env"
            )
        # Operational hint for long-term performance.
        if total >= 1_000_000:
            log.warning(
                "FPC table is large (rows=%s). Consider nightly archive "
                "of historical data to keep startup/delta queries fast.",
                total,
            )

    async def fetch_initial(
        self,
        start_time: datetime,
        end_time: datetime,
    ) -> list[TraySignal]:
        log.info(
            "FPC initial fetch start start_time=%s end_time=%s filters=%s",
            start_time.isoformat(),
            end_time.isoformat(),
            ("IS_HIS+DEL_FLAG" if self._extra_where else "none"),
        )
        if settings.fpc_density_probe_enabled:
            asyncio.ensure_future(self.probe_density(start_time, end_time))

        t0 = time.perf_counter()
        rows = await self._fetch_range(start_time, end_time)
        signals = await self._parse_rows_parallel(rows)
        log.info(
            "FPC initial fetch done elapsed_ms=%s raw_rows=%s signals=%s",
            int((time.perf_counter() - t0) * 1000),
            len(rows),
            len(signals),
        )
        return signals

    async def iter_initial_chunks(
        self,
        *,
        start_time: datetime,
        end_time: datetime,
    ) -> AsyncIterator[tuple[list[TraySignal], int]]:
        rows = await self._fetch_range(start_time, end_time)
        if rows:
            signals = await self._parse_rows_parallel(rows)
            yield signals, len(rows)

    async def fetch_delta(
        self,
        watermark: Watermark,
        end_time: datetime,
    ) -> list[TraySignal]:
        overlap_start = watermark.collected_time - timedelta(
            seconds=settings.delta_overlap_seconds
        )
        if overlap_start >= end_time:
            log.debug(
                "FPC delta fetch skipped: overlap_start=%s >= end_time=%s",
                overlap_start.isoformat(),
                end_time.isoformat(),
            )
            return []

        t0 = time.perf_counter()
        rows = await self._fetch_range(overlap_start, end_time)
        signals = await self._parse_rows_parallel(rows)
        log.debug(
            "FPC delta fetch done elapsed_ms=%s raw_rows=%s signals=%s",
            int((time.perf_counter() - t0) * 1000),
            len(rows),
            len(signals),
        )
        return signals

    async def peek_latest_signal_time(self, end_time: datetime) -> datetime | None:
        query = f"""
        SELECT TOP 1 COALESCE({self._end_col}, {self._start_col}) AS latest
        FROM {self._table} WITH (NOLOCK)
        WHERE {self._update_col} <= ?
        ORDER BY {self._update_col} DESC
        """
        rows = await self._conn.query_rows(
            query, [end_time], query_name="fpc.peek"
        )
        return _coerce_datetime(rows[0].get("latest")) if rows else None

    # ══════════════════════════════════════════════════════════════════════════
    # Keyset pagination — no LEN() filter
    # ══════════════════════════════════════════════════════════════════════════
    async def _fetch_range(
        self,
        start_time: datetime,
        end_time: datetime,
    ) -> list[dict[str, Any]]:
        """
        Keyset pagination with IS NOT NULL only (no LEN).

        Why no LEN(RECORD_JSON) > 10:
        ─────────────────────────────
        LEN() on a large text column is NON-SARGABLE — SQL Server must read
        every RECORD_JSON blob from disk to compute its length, even if the
        column has an index.  This adds ~30-40% I/O overhead per page.

        The Python parser (_parse_record_json) already handles empty/short
        JSON by returning {} → the row is harmlessly skipped.  Removing the
        DB-side filter trades a tiny amount of extra Python work (checking
        a few empty strings) for a large reduction in SQL Server I/O.
        """
        batch_size = max(1, int(settings.max_fetch_batch))
        pages = 0
        all_rows: list[dict[str, Any]] = []

        last_ct: datetime | None = None
        last_lot: str = ""

        while True:
            if last_ct is None:
                keyset_clause = ""
                params: list[Any] = [start_time, end_time]
            else:
                keyset_clause = (
                    f"  AND ({self._update_col} > ?\n"
                    f"        OR ({self._update_col} = ? "
                    f"AND {self._lot_col} > ?))"
                )
                params = [start_time, end_time, last_ct, last_ct, last_lot]

            query = f"""
            SELECT TOP {batch_size}
                {self._lot_col}    AS lot_no,
                {self._start_col}  AS start_time,
                {self._end_col}    AS end_time,
                {self._record_col} AS record_json,
                {self._update_col} AS update_time
            FROM {self._table} WITH (NOLOCK)
            WHERE {self._update_col} >= ?
              AND {self._update_col} <= ?
              AND {self._record_col} IS NOT NULL
{self._extra_where}
{keyset_clause}
            ORDER BY {self._update_col} ASC, {self._lot_col} ASC
            """

            try:
                chunk = await self._conn.query_rows(
                    query,
                    params,
                    query_name="fpc.fetch_range",
                )
            except Exception as exc:
                if pages == 0:
                    log.warning(
                        "FPC keyset pagination failed (%r), "
                        "falling back to OFFSET pagination",
                        exc,
                    )
                    return await self._fetch_range_offset_fallback(
                        start_time, end_time, batch_size
                    )
                raise

            if not chunk:
                break

            all_rows.extend(chunk)
            pages += 1

            if len(chunk) < batch_size:
                break

            last_row = chunk[-1]
            last_ct = _coerce_datetime(last_row.get("update_time")) or last_ct
            last_lot = str(last_row.get("lot_no") or "")

        if pages > 1:
            log.info(
                "FPC fetch range keyset-paged pages=%s raw_rows=%s batch_size=%s",
                pages,
                len(all_rows),
                batch_size,
            )

        return all_rows

    async def _fetch_range_offset_fallback(
        self,
        start_time: datetime,
        end_time: datetime,
        batch_size: int,
    ) -> list[dict[str, Any]]:
        """OFFSET-based fallback — also without LEN() filter."""
        offset = 0
        pages = 0
        rows_desc: list[dict[str, Any]] = []

        while True:
            query = f"""
            SELECT
                {self._lot_col}    AS lot_no,
                {self._start_col}  AS start_time,
                {self._end_col}    AS end_time,
                {self._record_col} AS record_json,
                {self._update_col} AS update_time
            FROM {self._table} WITH (NOLOCK)
            WHERE {self._update_col} >= ?
              AND {self._update_col} <= ?
              AND {self._record_col} IS NOT NULL
{self._extra_where}
            ORDER BY {self._update_col} DESC, {self._lot_col} DESC
            OFFSET ? ROWS
            FETCH NEXT {batch_size} ROWS ONLY
            """
            chunk = await self._conn.query_rows(
                query,
                [start_time, end_time, offset],
                query_name="fpc.fetch_range",
            )
            if not chunk:
                break
            rows_desc.extend(chunk)
            pages += 1
            if len(chunk) < batch_size:
                break
            offset += batch_size

        if pages > 1:
            log.info(
                "FPC fetch range offset-paged pages=%s raw_rows=%s",
                pages,
                len(rows_desc),
            )
        rows_desc.reverse()
        return rows_desc

    async def _parse_rows_parallel(
        self,
        rows: list[dict[str, Any]],
    ) -> list[TraySignal]:
        if not rows:
            return []
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            _get_parse_executor(),
            _parse_fpc_rows_sync,
            rows,
            self._tray_key,
            self._pos_key,
            self._keyword,
        )
