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
    slow_queries: int = 0
    errors: int = 0
    recent_times: deque = field(default_factory=lambda: deque(maxlen=100))

    def record(self, elapsed_ms: int, is_slow: bool, is_error: bool) -> None:
        self.total_queries += 1
        self.total_time_ms += elapsed_ms
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
        return {
            "total_queries": self.total_queries,
            "total_time_ms": self.total_time_ms,
            "slow_queries": self.slow_queries,
            "errors": self.errors,
            "avg_recent_ms": round(self.avg_time_ms(), 1),
        }


# ═══════════════════════════════════════════════════════════════════════════
# Connection Lane — isolated (pool=1, executor=1) pair
# ═══════════════════════════════════════════════════════════════════════════


class _ConnectionLane:
    """
    An isolated (pool=1, executor=1) pair.
    
    Thread safety: Each lane has exactly ONE connection accessed by
    exactly ONE dedicated thread. No cross-thread access possible.
    """

    def __init__(self, lane_id: int, name: str = "") -> None:
        self.lane_id = lane_id
        self.name = name or f"lane-{lane_id}"
        self.pool: aioodbc.Pool | None = None
        self._executor: ThreadPoolExecutor | None = None
        self._lock: asyncio.Lock | None = None
        self._query_count: int = 0
        self._active_query: str | None = None

    async def start(self, *, dsn: str) -> None:
        self._executor = ThreadPoolExecutor(
            max_workers=1,
            thread_name_prefix=f"pyodbc-{self.name}",
        )
        self._lock = asyncio.Lock()
        self.pool = await aioodbc.create_pool(
            dsn=dsn,
            autocommit=True,
            minsize=1,
            maxsize=1,
            executor=self._executor,
        )
        log.debug("Lane '%s' (id=%d) started", self.name, self.lane_id)

    async def close(self) -> None:
        if self.pool is not None:
            self.pool.close()
            await self.pool.wait_closed()
            self.pool = None
        if self._executor is not None:
            self._executor.shutdown(wait=True, cancel_futures=False)
            self._executor = None
        self._lock = None
        log.debug(
            "Lane '%s' (id=%d) closed queries_executed=%d",
            self.name, self.lane_id, self._query_count,
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
    ) -> tuple[list[tuple], list[str]]:
        """Execute query on this lane with serialization."""
        if self.pool is None or self._lock is None:
            raise RuntimeError(f"Lane '{self.name}' not started")

        async with self._lock:
            self._query_count += 1
            self._active_query = query_name or "unknown"
            try:
                async with self.pool.acquire() as conn:
                    async with conn.cursor() as cur:
                        cur.timeout = timeout
                        await cur.execute(query, params)

                        rows: list[tuple[Any, ...]] = []
                        cols: list[str] = []

                        while True:
                            if cur.description is not None:
                                cols = [col[0] for col in cur.description]
                                rows = await cur.fetchall()
                                break
                            has_next = await cur.nextset()
                            if not has_next:
                                break

                        return rows, cols
            finally:
                self._active_query = None


# ═══════════════════════════════════════════════════════════════════════════
# Lane Router — maps query names to lanes
# ═══════════════════════════════════════════════════════════════════════════


class _LaneRouter:
    """
    Routes queries to appropriate lanes based on query name.
    
    3-Lane legacy mode routing:
      - Lane 0 (ccu): ccu.fetch_range, ccu.backfill_*
      - Lane 1 (fpc): fpc.fetch_range, fpc.density_probe, fpc.backfill_*
      - Lane 2 (ui):  ccu.fetch_tray_cells, dashboard.*, fallback
    """

    # Query prefix → lane index mapping for 3-lane mode
    _ROUTING_3LANE: dict[str, int] = {
        # CCU bulk operations → lane 0
        "ccu.fetch_range": 0,
        "ccu.backfill": 0,
        
        # FPC bulk operations → lane 1
        "fpc.fetch_range": 1,
        "fpc.density_probe": 1,
        "fpc.backfill": 1,
        
        # UI/interactive operations → lane 2
        "ccu.fetch_tray_cells": 2,
        "dashboard": 2,
    }

    # 2-lane mode: CCU vs FPC
    _ROUTING_2LANE: dict[str, int] = {
        "fpc.": 1,  # All FPC → lane 1
        # Everything else → lane 0
    }

    @classmethod
    def route(cls, query_name: str, lane_count: int) -> int:
        """Select lane index for given query name."""
        name_lower = (query_name or "").lower().strip()

        if lane_count >= 3:
            # 3-lane mode: specific routing
            for prefix, lane_idx in cls._ROUTING_3LANE.items():
                if name_lower.startswith(prefix):
                    return min(lane_idx, lane_count - 1)
            # Default to UI lane for unknown queries
            return min(2, lane_count - 1)

        elif lane_count == 2:
            # 2-lane mode: CCU vs FPC
            if name_lower.startswith("fpc."):
                return 1
            return 0

        else:
            # 1-lane mode: everything on lane 0
            return 0


# ═══════════════════════════════════════════════════════════════════════════
# Connection Pool
# ═══════════════════════════════════════════════════════════════════════════


class SQLServerConnection:
    """
    SQL Server connection with thread-safety fix for legacy ODBC driver.

    LEGACY DRIVER MODES:
    ────────────────────
    1. SERIALIZED (SQL_ALLOW_LEGACY_PARALLEL_QUERIES=false):
       - 1 lane, 1 connection, 1 thread
       - All queries run sequentially
       - Safest but slowest

    2. MULTI-LANE (SQL_ALLOW_LEGACY_PARALLEL_QUERIES=true):
       - 3 lanes: CCU + FPC + UI
       - CCU refresh, FPC refresh, and UI queries can run in parallel
       - Queries within each lane are serialized
       - Much faster, but some crash risk with legacy driver

    For modern drivers (ODBC Driver 17/18):
    ───────────────────────────────────────
    Multiple lanes with full concurrent execution.
    """

    # Lane count for legacy parallel mode
    _LEGACY_LANE_COUNT = 3

    def __init__(self) -> None:
        self._lanes: list[_ConnectionLane] | None = None
        self._lane_count: int = 0

        self._query_semaphore: asyncio.Semaphore | None = None
        self._slow_query_threshold_ms: int = settings.sql_slow_query_threshold_ms
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
                        getattr(settings, "sql_allow_legacy_parallel_queries", False)
                    )
                    self._legacy_parallel_enabled = allow_parallel

                    if allow_parallel:
                        await self._start_legacy_multi_lane(dsn=dsn, driver=driver)
                    else:
                        await self._start_legacy_serialized(dsn=dsn, driver=driver)
                else:
                    concurrency = max(1, int(settings.sql_max_concurrent_queries))
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

    async def _start_legacy_serialized(
        self, *, dsn: str, driver: str
    ) -> None:
        """
        LEGACY DRIVER - SERIALIZED MODE: 1 lane, fully sequential.
        """
        log.warning(
            "Legacy SQL driver '%s' detected. Using SERIALIZED mode "
            "(1 connection, 1 thread, no parallelism). "
            "Set SQL_ALLOW_LEGACY_PARALLEL_QUERIES=true for 3-lane mode. "
            "Install 'ODBC Driver 17 for SQL Server' for best performance.",
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

    async def _start_legacy_multi_lane(
        self, *, dsn: str, driver: str
    ) -> None:
        """
        LEGACY DRIVER - 3-LANE MODE: CCU + FPC + UI lanes.
        
        Lane routing:
          - Lane 0 (ccu): ccu.fetch_range, ccu.backfill_*
          - Lane 1 (fpc): fpc.fetch_range, fpc.density_probe
          - Lane 2 (ui):  ccu.fetch_tray_cells, dashboard.*, user queries
        
        This allows:
          - CCU and FPC initial load to run in parallel
          - UI tray detail to run without waiting for refresh
          - Dashboard writes to not block data fetches
        
        WARNING: 3 concurrent connections with legacy driver has some crash risk.
        If crashes occur, set SQL_ALLOW_LEGACY_PARALLEL_QUERIES=false.
        """
        lane_count = self._LEGACY_LANE_COUNT

        log.warning(
            "Legacy SQL driver '%s' detected. Using 3-LANE mode "
            "(3 connections: ccu + fpc + ui). "
            "Queries can run in parallel across lanes. "
            "If crashes occur, set SQL_ALLOW_LEGACY_PARALLEL_QUERIES=false. "
            "Install 'ODBC Driver 17 for SQL Server' for best performance.",
            driver,
        )

        lane_names = ["ccu", "fpc", "ui"]
        lanes: list[_ConnectionLane] = []

        try:
            for i in range(lane_count):
                lane = _ConnectionLane(lane_id=i, name=lane_names[i])
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
            "SQL query concurrency: 3-LANE mode "
            "lanes=[ccu(bulk), fpc(bulk), ui(interactive)] "
            "pool_maxsize=1/lane executor_workers=1/lane is_legacy=True",
        )

    async def _start_modern_lanes(
        self, *, dsn: str, driver: str, concurrency: int
    ) -> None:
        """
        MODERN DRIVER: Multiple lanes with full parallelism.
        """
        lane_count = min(concurrency, 8)
        lane_count = max(lane_count, 2)

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
            "lanes=%d pool_maxsize=1/lane executor_workers=1/lane is_legacy=False",
            lane_count,
        )

    async def _cleanup_on_failure(self) -> None:
        """Clean up partially-created resources on startup failure."""
        if self._lanes is not None:
            for lane in self._lanes:
                await lane.close()
            self._lanes = None

    async def close(self) -> None:
        if self._lanes is None:
            return
        log.info("Closing SQL pool")

        await self._log_stats()

        for lane in self._lanes:
            await lane.close()
        self._lanes = None

        self._query_semaphore = None
        self._active_driver = ""
        log.info("SQL pool closed")

    # ═══════════════════════════════════════════════════════════════════════
    # Query Execution
    # ═══════════════════════════════════════════════════════════════════════

    async def query_rows(
        self,
        query: str,
        params: list[Any],
        *,
        query_name: str | None = None,
    ) -> list[dict[str, Any]]:
        """Execute query with timing, lane selection, and stats."""
        if self._lanes is None:
            raise RuntimeError("SQL connection has not started")
        if self._query_semaphore is None:
            raise RuntimeError("SQL query semaphore is not initialized")

        query_preview = _compact_sql(
            query, max_chars=settings.log_sql_preview_chars
        )
        query_label = (query_name or "sql.query").strip() or "sql.query"

        # Route query to appropriate lane
        lane_idx = _LaneRouter.route(query_label, self._lane_count)
        lane = self._lanes[min(lane_idx, len(self._lanes) - 1)]

        started_at = time.perf_counter()

        rows, cols = await self._execute_with_timing(
            lane, query, params, query_label, query_preview, started_at
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
    ) -> tuple[list[tuple], list[str]]:
        """Execute query with timing, logging, and stats."""
        exec_started_at = time.perf_counter()
        queue_wait_ms = int((exec_started_at - started_at) * 1000)
        is_error = False

        try:
            rows, cols = await lane.execute(
                query,
                params,
                settings.sql_query_timeout_seconds,
                query_name=query_label,
            )
        except Exception:
            is_error = True
            elapsed_ms = int((time.perf_counter() - started_at) * 1000)
            await self._record_stats(query_label, elapsed_ms, False, True)
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

        exec_done_at = time.perf_counter()
        db_exec_ms = int((exec_done_at - exec_started_at) * 1000)
        elapsed_ms = int((exec_done_at - started_at) * 1000)
        row_count = len(rows)
        is_slow = elapsed_ms >= self._slow_query_threshold_ms

        await self._record_stats(query_label, elapsed_ms, is_slow, is_error)

        if is_slow:
            log.warning(
                "SQL query slow name=%s lane=%s total_ms=%s queue_wait_ms=%s "
                "db_exec_ms=%s rows=%s params=%s sql=%s",
                query_label,
                lane.name,
                elapsed_ms,
                queue_wait_ms,
                db_exec_ms,
                row_count,
                len(params),
                query_preview,
            )
        else:
            log.debug(
                "SQL query done name=%s lane=%s total_ms=%s rows=%s",
                query_label,
                lane.name,
                elapsed_ms,
                row_count,
            )

        return rows, cols

    # ═══════════════════════════════════════════════════════════════════════
    # Stats
    # ═══════════════════════════════════════════════════════════════════════

    async def _record_stats(
        self,
        query_name: str,
        elapsed_ms: int,
        is_slow: bool,
        is_error: bool,
    ) -> None:
        async with self._stats_lock:
            if query_name not in self._stats:
                self._stats[query_name] = QueryStats()
            self._stats[query_name].record(elapsed_ms, is_slow, is_error)

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