# wip_management/infrastructure/sqlserver/connection.py

from __future__ import annotations

import asyncio
import logging
import re
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any

import aioodbc

from wip_management.config import settings

log = logging.getLogger(__name__)

_DRIVER_VERSION_PATTERN = re.compile(
    r"^ODBC Driver (\d+) for SQL Server$", re.IGNORECASE
)
_VALID_SQL_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_LEGACY_SQL_DRIVER_NAME = "sql server"
_AUTO_DRIVER_KEYWORDS = {"", "auto", "best"}


# ═══════════════════════════════════════════════════════════════════════════
# Query Statistics
# ═══════════════════════════════════════════════════════════════════════════


@dataclass(slots=True)
class QueryStats:
    """Track query performance statistics."""

    total_queries: int = 0
    total_time_ms: int = 0
    total_queue_wait_ms: int = 0
    total_db_exec_ms: int = 0
    total_fetch_ms: int = 0
    slow_queries: int = 0
    errors: int = 0
    recent_times: deque = field(default_factory=lambda: deque(maxlen=100))

    def record(
        self,
        elapsed_ms: int,
        is_slow: bool,
        is_error: bool,
        *,
        queue_wait_ms: int = 0,
        db_exec_ms: int = 0,
        fetch_ms: int = 0,
    ) -> None:
        self.total_queries += 1
        self.total_time_ms += elapsed_ms
        self.total_queue_wait_ms += max(0, int(queue_wait_ms))
        self.total_db_exec_ms += max(0, int(db_exec_ms))
        self.total_fetch_ms += max(0, int(fetch_ms))
        self.recent_times.append(elapsed_ms)
        if is_slow:
            self.slow_queries += 1
        if is_error:
            self.errors += 1

    def avg_time_ms(self) -> float:
        if not self.recent_times:
            return 0.0
        return sum(self.recent_times) / len(self.recent_times)

    def to_dict(self) -> dict[str, Any]:
        total = max(1, self.total_queries)
        return {
            "total_queries": self.total_queries,
            "total_time_ms": self.total_time_ms,
            "total_queue_wait_ms": self.total_queue_wait_ms,
            "total_db_exec_ms": self.total_db_exec_ms,
            "total_fetch_ms": self.total_fetch_ms,
            "slow_queries": self.slow_queries,
            "errors": self.errors,
            "avg_recent_ms": round(self.avg_time_ms(), 1),
            "avg_total_ms": round(self.total_time_ms / total, 1),
            "avg_queue_wait_ms": round(self.total_queue_wait_ms / total, 1),
            "avg_db_exec_ms": round(self.total_db_exec_ms / total, 1),
            "avg_fetch_ms": round(self.total_fetch_ms / total, 1),
        }


# ═══════════════════════════════════════════════════════════════════════════
# Connection Lane — isolated pool + optional shared executor
# ═══════════════════════════════════════════════════════════════════════════


class _ConnectionLane:
    """
    A single ODBC connection behind an asyncio pool (size=1).

    Thread-safety modes
    ───────────────────
    • **Own executor** (modern driver):
        Each lane creates ThreadPoolExecutor(max_workers=1).
        Different lanes → different OS threads → true parallelism.

    • **Shared executor** (legacy driver):
        All lanes share ONE ThreadPoolExecutor(max_workers=1).
        All ODBC calls funnel through a single OS thread → safe.
        Different lanes still provide routing / metrics separation.
    """

    def __init__(
        self,
        lane_id: int,
        name: str = "",
        shared_executor: ThreadPoolExecutor | None = None,
    ) -> None:
        self.lane_id = lane_id
        self.name = name or f"lane-{lane_id}"
        self.pool: aioodbc.Pool | None = None
        self._executor: ThreadPoolExecutor | None = None
        self._owns_executor: bool = (shared_executor is None)
        self._shared_executor = shared_executor
        self._lock: asyncio.Lock | None = None
        self._query_count: int = 0
        self._active_query: str | None = None

    async def start(self, *, dsn: str) -> None:
        if self._shared_executor is not None:
            # Legacy mode: reuse the single shared executor
            self._executor = self._shared_executor
            self._owns_executor = False
        else:
            # Modern mode: own dedicated thread
            self._executor = ThreadPoolExecutor(
                max_workers=1,
                thread_name_prefix=f"pyodbc-{self.name}",
            )
            self._owns_executor = True

        self._lock = asyncio.Lock()
        self.pool = await aioodbc.create_pool(
            dsn=dsn,
            autocommit=True,
            minsize=1,
            maxsize=1,
            executor=self._executor,
        )
        log.debug(
            "Lane '%s' (id=%d) started shared_executor=%s",
            self.name,
            self.lane_id,
            not self._owns_executor,
        )

    async def close(self) -> None:
        if self.pool is not None:
            self.pool.close()
            await self.pool.wait_closed()
            self.pool = None
        # Only shutdown executor if this lane created it
        if self._executor is not None and self._owns_executor:
            self._executor.shutdown(wait=True, cancel_futures=False)
        self._executor = None
        self._lock = None
        log.debug(
            "Lane '%s' (id=%d) closed queries_executed=%d",
            self.name,
            self.lane_id,
            self._query_count,
        )

    def is_busy(self) -> bool:
        """Check if lane is currently executing a query."""
        return self._active_query is not None

    async def execute(
        self,
        query: str,
        params: list[Any],
        timeout: int,
        query_name: str = "",
    ) -> tuple[list[tuple], list[str], int, int, int]:
        """Execute query on this lane with serialization."""
        if self.pool is None or self._lock is None:
            raise RuntimeError(f"Lane '{self.name}' not started")

        lock_wait_started_at = time.perf_counter()
        async with self._lock:
            lock_acquired_at = time.perf_counter()
            lane_wait_ms = int((lock_acquired_at - lock_wait_started_at) * 1000)
            self._query_count += 1
            self._active_query = query_name or "unknown"
            try:
                async with self.pool.acquire() as conn:
                    async with conn.cursor() as cur:
                        cur.timeout = timeout
                        execute_started_at = time.perf_counter()
                        await cur.execute(query, params)
                        db_exec_ms = int(
                            (time.perf_counter() - execute_started_at) * 1000
                        )

                        rows: list[tuple[Any, ...]] = []
                        cols: list[str] = []
                        fetch_started_at = time.perf_counter()

                        while True:
                            if cur.description is not None:
                                cols = [col[0] for col in cur.description]
                                rows = await cur.fetchall()
                                break
                            has_next = await cur.nextset()
                            if not has_next:
                                break

                        fetch_ms = int(
                            (time.perf_counter() - fetch_started_at) * 1000
                        )
                        return rows, cols, lane_wait_ms, db_exec_ms, fetch_ms
            finally:
                self._active_query = None


# ═══════════════════════════════════════════════════════════════════════════
# Lane Router — maps query names to lanes
# ═══════════════════════════════════════════════════════════════════════════


class _LaneRouter:
    """
    Routes queries to appropriate lanes based on query name.

    3-Lane routing:
      - Lane 0 (ccu): ccu.fetch_range, ccu.backfill_*
      - Lane 1 (fpc): fpc.fetch_range, fpc.density_probe, fpc.backfill_*
      - Lane 2 (ui):  ccu.fetch_tray_cells, dashboard.*, fallback
    """

    _ROUTING_3LANE: dict[str, int] = {
        "ccu.fetch_range": 0,
        "ccu.backfill": 0,
        "fpc.fetch_range": 1,
        "fpc.density_probe": 1,
        "fpc.backfill": 1,
        "ccu.fetch_tray_cells": 2,
        "dashboard": 2,
    }

    _ROUTING_2LANE: dict[str, int] = {
        "fpc.": 1,
    }

    @classmethod
    def route(cls, query_name: str, lane_count: int) -> int:
        """Select lane index for given query name."""
        name_lower = (query_name or "").lower().strip()

        if lane_count >= 3:
            for prefix, lane_idx in cls._ROUTING_3LANE.items():
                if name_lower.startswith(prefix):
                    return min(lane_idx, lane_count - 1)
            return min(2, lane_count - 1)

        elif lane_count == 2:
            if name_lower.startswith("fpc."):
                return 1
            return 0

        else:
            return 0


# ═══════════════════════════════════════════════════════════════════════════
# Connection Pool
# ═══════════════════════════════════════════════════════════════════════════


class SQLServerConnection:
    """
    SQL Server connection pool with thread-safety for legacy ODBC driver.

    ARCHITECTURE — WHY MULTIPLE CONNECTIONS STILL CRASH
    ───────────────────────────────────────────────────
    The legacy 'SQL Server' ODBC driver (SQLSRV32.DLL) is loaded ONCE
    into the process as a shared library.  Even with separate
    pyodbc.Connection objects, calling the driver from different OS
    threads simultaneously corrupts the DLL's internal state →
    "Windows fatal exception: access violation".

    SOLUTION: all pyodbc calls go through ONE ThreadPoolExecutor
    (max_workers=1) so that only ONE OS thread ever touches the DLL.

    MODES
    ─────
    1. SERIALIZED  (legacy, parallel=false):
       1 lane · 1 connection · 1 thread.

    2. MULTI-LANE SAFE  (legacy, parallel=true):
       3 lanes · 3 connections · 1 SHARED thread.
       Logical separation (routing + metrics) without crash risk.

    3. MODERN  (ODBC Driver 17/18):
       N lanes · N connections · N threads.  Full parallelism.
    """

    _LEGACY_LANE_COUNT = 3

    def __init__(self) -> None:
        self._lanes: list[_ConnectionLane] | None = None
        self._lane_count: int = 0
        self._shared_executor: ThreadPoolExecutor | None = None

        self._query_semaphore: asyncio.Semaphore | None = None
        self._slow_query_threshold_ms: int = settings.sql_slow_query_threshold_ms
        self._slow_query_threshold_fetch_range_ms: int = (
            settings.sql_slow_query_threshold_fetch_range_ms
        )
        self._slow_query_threshold_peek_ms: int = (
            settings.sql_slow_query_threshold_peek_ms
        )
        self._slow_query_threshold_tray_cells_ms: int = (
            settings.sql_slow_query_threshold_tray_cells_ms
        )
        self._configured_driver: str = settings.sql_driver
        self._active_driver: str = ""
        self._is_legacy_driver: bool = False
        self._legacy_parallel_enabled: bool = False

        # Performance monitoring
        self._stats: dict[str, QueryStats] = {}
        self._stats_lock = asyncio.Lock()

    async def start(self) -> None:
        if self._lanes is not None:
            log.debug("SQL pool already started")
            return

        candidates = build_driver_candidates(settings.sql_driver)
        if not candidates:
            raise RuntimeError(
                "No SQL Server ODBC driver detected. "
                "Install ODBC Driver 17+ or use built-in 'SQL Server' driver."
            )

        log.info(
            "Starting SQL pool with %s driver candidate(s): %s",
            len(candidates),
            candidates,
        )

        last_error: Exception | None = None
        tried: list[str] = []

        for driver in candidates:
            tried.append(driver)
            try:
                log.debug("Trying SQL driver=%s", driver)

                is_legacy = driver.strip().lower() == _LEGACY_SQL_DRIVER_NAME
                self._is_legacy_driver = is_legacy
                dsn = settings.build_odbc_dsn(driver=driver)

                if is_legacy:
                    allow_parallel = bool(
                        getattr(
                            settings,
                            "sql_allow_legacy_parallel_queries",
                            False,
                        )
                    )
                    self._legacy_parallel_enabled = allow_parallel

                    if allow_parallel:
                        await self._start_legacy_multi_lane(
                            dsn=dsn, driver=driver
                        )
                    else:
                        await self._start_legacy_serialized(
                            dsn=dsn, driver=driver
                        )
                else:
                    concurrency = max(
                        1, int(settings.sql_max_concurrent_queries)
                    )
                    await self._start_modern_lanes(
                        dsn=dsn, driver=driver, concurrency=concurrency
                    )

                self._active_driver = driver
                return

            except Exception as exc:
                last_error = exc
                await self._cleanup_on_failure()
                log.warning(
                    "Failed to create SQL pool with driver=%s error=%r",
                    driver,
                    exc,
                )

        installed = list_sql_server_drivers()
        raise RuntimeError(
            f"Cannot connect to SQL Server. "
            f"Attempted: {tried}. Installed: {installed}"
        ) from last_error

    # ───────────────────────────────────────────────────────────────────
    # Legacy driver — serialized (1 lane)
    # ───────────────────────────────────────────────────────────────────

    async def _start_legacy_serialized(
        self, *, dsn: str, driver: str
    ) -> None:
        log.warning(
            "Legacy SQL driver '%s' detected. Using SERIALIZED mode "
            "(1 connection, 1 thread, no parallelism). "
            "Set SQL_ALLOW_LEGACY_PARALLEL_QUERIES=true for 3-lane mode "
            "(better query routing, same ODBC serialization). "
            "Install 'ODBC Driver 17 for SQL Server' for true parallelism.",
            driver,
        )

        lane = _ConnectionLane(lane_id=0, name="serial")
        await lane.start(dsn=dsn)
        self._lanes = [lane]
        self._lane_count = 1
        self._query_semaphore = asyncio.Semaphore(1)

        log.info(
            "SQL ODBC driver selected configured=%s active=%s",
            self._configured_driver,
            driver,
        )
        log.info(
            "SQL query concurrency: SERIALIZED mode "
            "lanes=1 pool_maxsize=1 executor_workers=1 is_legacy=True",
        )

    # ───────────────────────────────────────────────────────────────────
    # Legacy driver — 3-lane safe (shared single executor)
    # ───────────────────────────────────────────────────────────────────

    async def _start_legacy_multi_lane(
        self, *, dsn: str, driver: str
    ) -> None:
        """
        3 logical lanes, 3 connections, but ONE shared OS thread.

        Why not 3 threads?
        ──────────────────
        SQLSRV32.DLL is loaded once per process.  Its internal state
        is not protected by any lock.  Two OS threads calling into
        the DLL at the same time corrupt memory → access violation.

        Having 3 *connections* on 1 *thread* is safe: each connection
        object is independent, and single-threaded access means the
        DLL never sees concurrent calls.
        """
        lane_count = self._LEGACY_LANE_COUNT

        log.warning(
            "Legacy SQL driver '%s' detected. Using 3-LANE SAFE mode "
            "(3 connections: ccu + fpc + ui, 1 shared ODBC thread). "
            "All ODBC calls serialised through a single OS thread to "
            "prevent access-violation crashes from the non-thread-safe "
            "driver DLL. "
            "Install 'ODBC Driver 17 for SQL Server' for true parallelism.",
            driver,
        )

        # ── single shared executor for ALL lanes ──────────────────────
        shared_executor = ThreadPoolExecutor(
            max_workers=1,
            thread_name_prefix="pyodbc-legacy",
        )
        self._shared_executor = shared_executor

        lane_names = ["ccu", "fpc", "ui"]
        lanes: list[_ConnectionLane] = []

        try:
            for i in range(lane_count):
                lane = _ConnectionLane(
                    lane_id=i,
                    name=lane_names[i],
                    shared_executor=shared_executor,
                )
                await lane.start(dsn=dsn)
                lanes.append(lane)
        except Exception:
            for lane in lanes:
                await lane.close()
            shared_executor.shutdown(wait=False)
            self._shared_executor = None
            raise

        self._lanes = lanes
        self._lane_count = lane_count
        self._query_semaphore = asyncio.Semaphore(lane_count)

        log.info(
            "SQL ODBC driver selected configured=%s active=%s",
            self._configured_driver,
            driver,
        )
        log.info(
            "SQL query concurrency: 3-LANE SAFE mode "
            "lanes=[ccu, fpc, ui] connections=3 "
            "shared_executor_threads=1 is_legacy=True "
            "(all ODBC calls serialised through 1 OS thread)",
        )

    # ───────────────────────────────────────────────────────────────────
    # Modern driver — full parallelism
    # ───────────────────────────────────────────────────────────────────

    async def _start_modern_lanes(
        self, *, dsn: str, driver: str, concurrency: int
    ) -> None:
        lane_count = max(2, min(concurrency, 8))

        log.info(
            "Modern SQL driver '%s': creating %d lanes.",
            driver,
            lane_count,
        )

        lanes: list[_ConnectionLane] = []
        try:
            for i in range(lane_count):
                lane = _ConnectionLane(lane_id=i, name=f"lane-{i}")
                await lane.start(dsn=dsn)
                lanes.append(lane)
        except Exception:
            for lane in lanes:
                await lane.close()
            raise

        self._lanes = lanes
        self._lane_count = lane_count
        self._query_semaphore = asyncio.Semaphore(lane_count)

        log.info(
            "SQL ODBC driver selected configured=%s active=%s",
            self._configured_driver,
            driver,
        )
        log.info(
            "SQL query concurrency: MODERN mode "
            "lanes=%d pool_maxsize=1/lane executor_workers=1/lane "
            "is_legacy=False",
            lane_count,
        )

    # ───────────────────────────────────────────────────────────────────
    # Cleanup
    # ───────────────────────────────────────────────────────────────────

    async def _cleanup_on_failure(self) -> None:
        if self._lanes is not None:
            for lane in self._lanes:
                await lane.close()
            self._lanes = None
        if self._shared_executor is not None:
            self._shared_executor.shutdown(wait=False)
            self._shared_executor = None

    async def close(self) -> None:
        if self._lanes is None:
            return
        log.info("Closing SQL pool")

        await self._log_stats()

        # Close all lanes first (releases connections)
        for lane in self._lanes:
            await lane.close()
        self._lanes = None

        # Then shutdown shared executor (lanes don't own it)
        if self._shared_executor is not None:
            self._shared_executor.shutdown(wait=True)
            self._shared_executor = None

        self._query_semaphore = None
        self._active_driver = ""
        log.info("SQL pool closed")

    # ═══════════════════════════════════════════════════════════════════
    # Query Execution
    # ═══════════════════════════════════════════════════════════════════

    async def query_rows(
        self,
        query: str,
        params: list[Any],
        *,
        query_name: str | None = None,
    ) -> list[dict[str, Any]]:
        if self._lanes is None:
            raise RuntimeError("SQL connection has not started")
        if self._query_semaphore is None:
            raise RuntimeError("SQL query semaphore is not initialized")

        query_preview = _compact_sql(
            query, max_chars=settings.log_sql_preview_chars
        )
        query_label = (query_name or "sql.query").strip() or "sql.query"

        lane_idx = _LaneRouter.route(query_label, self._lane_count)
        lane = self._lanes[min(lane_idx, len(self._lanes) - 1)]

        started_at = time.perf_counter()
        async with self._query_semaphore:
            acquired_at = time.perf_counter()
            semaphore_wait_ms = int((acquired_at - started_at) * 1000)
            rows, cols = await self._execute_with_timing(
                lane,
                query,
                params,
                query_label,
                query_preview,
                started_at,
                semaphore_wait_ms,
            )

        if not cols:
            return []
        return [dict(zip(cols, row, strict=True)) for row in rows]

    async def _execute_with_timing(
        self,
        lane: _ConnectionLane,
        query: str,
        params: list[Any],
        query_label: str,
        query_preview: str,
        started_at: float,
        semaphore_wait_ms: int,
    ) -> tuple[list[tuple], list[str]]:
        queue_wait_ms = semaphore_wait_ms
        is_error = False

        try:
            rows, cols, lane_wait_ms, db_exec_ms, fetch_ms = await lane.execute(
                query,
                params,
                settings.sql_query_timeout_seconds,
                query_name=query_label,
            )
            queue_wait_ms += lane_wait_ms
        except Exception:
            is_error = True
            elapsed_ms = int((time.perf_counter() - started_at) * 1000)
            await self._record_stats(
                query_label,
                elapsed_ms,
                False,
                True,
                queue_wait_ms=queue_wait_ms,
                db_exec_ms=0,
                fetch_ms=0,
            )
            log.exception(
                "SQL query failed name=%s lane=%s total_ms=%s "
                "queue_wait_ms=%s sql=%s",
                query_label,
                lane.name,
                elapsed_ms,
                queue_wait_ms,
                query_preview,
            )
            raise

        elapsed_ms = queue_wait_ms + db_exec_ms + fetch_ms
        row_count = len(rows)
        slow_threshold_ms = self._slow_threshold_for_query(query_label)
        is_slow = elapsed_ms >= slow_threshold_ms

        await self._record_stats(
            query_label,
            elapsed_ms,
            is_slow,
            is_error,
            queue_wait_ms=queue_wait_ms,
            db_exec_ms=db_exec_ms,
            fetch_ms=fetch_ms,
        )

        if is_slow:
            log.warning(
                "SQL query slow name=%s lane=%s total_ms=%s "
                "slow_threshold_ms=%s queue_wait_ms=%s db_exec_ms=%s fetch_ms=%s "
                "rows=%s params=%s sql=%s",
                query_label,
                lane.name,
                elapsed_ms,
                slow_threshold_ms,
                queue_wait_ms,
                db_exec_ms,
                fetch_ms,
                row_count,
                len(params),
                query_preview,
            )
        else:
            log.debug(
                (
                    "SQL query done name=%s lane=%s total_ms=%s "
                    "queue_wait_ms=%s db_exec_ms=%s fetch_ms=%s rows=%s"
                ),
                query_label,
                lane.name,
                elapsed_ms,
                queue_wait_ms,
                db_exec_ms,
                fetch_ms,
                row_count,
            )

        return rows, cols

    def _slow_threshold_for_query(self, query_name: str) -> int:
        name = (query_name or "").lower()
        if "fetch_range" in name:
            return self._slow_query_threshold_fetch_range_ms
        if "tray_cells" in name:
            return self._slow_query_threshold_tray_cells_ms
        if name.endswith(".peek") or ".peek_" in name:
            return self._slow_query_threshold_peek_ms
        return self._slow_query_threshold_ms

    # ═══════════════════════════════════════════════════════════════════
    # Stats
    # ═══════════════════════════════════════════════════════════════════

    async def _record_stats(
        self,
        query_name: str,
        elapsed_ms: int,
        is_slow: bool,
        is_error: bool,
        *,
        queue_wait_ms: int = 0,
        db_exec_ms: int = 0,
        fetch_ms: int = 0,
    ) -> None:
        async with self._stats_lock:
            if query_name not in self._stats:
                self._stats[query_name] = QueryStats()
            self._stats[query_name].record(
                elapsed_ms,
                is_slow,
                is_error,
                queue_wait_ms=queue_wait_ms,
                db_exec_ms=db_exec_ms,
                fetch_ms=fetch_ms,
            )

    async def _log_stats(self) -> None:
        async with self._stats_lock:
            if not self._stats:
                return
            log.info("SQL query statistics:")
            for name, stats in sorted(self._stats.items()):
                log.info("  %s: %s", name, stats.to_dict())

    async def get_stats(self) -> dict[str, dict[str, Any]]:
        async with self._stats_lock:
            return {
                name: stats.to_dict()
                for name, stats in self._stats.items()
            }

    @property
    def active_driver(self) -> str:
        return self._active_driver

    @property
    def is_legacy(self) -> bool:
        return self._is_legacy_driver

    @staticmethod
    def table_name(schema: str, table: str) -> str:
        if not _VALID_SQL_IDENT.match(schema) or not _VALID_SQL_IDENT.match(
            table
        ):
            raise ValueError("Invalid schema/table name")
        return f"[{schema}].[{table}]"

    @staticmethod
    def column_name(name: str) -> str:
        if not _VALID_SQL_IDENT.match(name):
            raise ValueError(f"Invalid column name: {name}")
        return f"[{name}]"


# ═══════════════════════════════════════════════════════════════════════════
# Driver Discovery
# ═══════════════════════════════════════════════════════════════════════════


def list_sql_server_drivers() -> list[str]:
    try:
        import pyodbc
    except Exception:
        return []
    return [driver for driver in pyodbc.drivers() if "SQL Server" in driver]


def build_driver_candidates(preferred_driver: str) -> list[str]:
    preferred = preferred_driver.strip()
    installed = sorted(
        list_sql_server_drivers(), key=_driver_sort_key, reverse=True
    )
    installed = _prioritize_modern_drivers(installed)

    preferred_norm = preferred.lower()
    if preferred_norm in _AUTO_DRIVER_KEYWORDS:
        return installed

    if preferred in installed:
        if _is_legacy_driver(preferred):
            return installed
        return [preferred, *[d for d in installed if d != preferred]]

    if not preferred:
        return installed

    return [preferred, *installed]


def _driver_sort_key(driver: str) -> tuple[int, str]:
    match = _DRIVER_VERSION_PATTERN.match(driver.strip())
    if match:
        return int(match.group(1)), driver
    return -1, driver


def _is_legacy_driver(driver: str) -> bool:
    return driver.strip().lower() == _LEGACY_SQL_DRIVER_NAME


def _prioritize_modern_drivers(drivers: list[str]) -> list[str]:
    modern = [d for d in drivers if not _is_legacy_driver(d)]
    legacy = [d for d in drivers if _is_legacy_driver(d)]
    return [*modern, *legacy]


def _compact_sql(sql: str, *, max_chars: int) -> str:
    single_line = " ".join(sql.split())
    if len(single_line) <= max_chars:
        return single_line
    return f"{single_line[:max_chars - 3]}..."
