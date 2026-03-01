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
# Connection Pool
# ═══════════════════════════════════════════════════════════════════════════


class SQLServerConnection:
    """
    SQL Server connection with thread-safety fix for legacy ODBC driver.

    CRITICAL FIX FOR LEGACY DRIVER:
    ───────────────────────────────
    The legacy 'SQL Server' ODBC driver (SQLSRV32.dll) has GLOBAL internal
    state that is NOT thread-safe. Even with separate connections on separate
    threads, concurrent execution can cause access violations.

    Solution for legacy driver:
    - Single pool with maxsize=1 (one connection)
    - Single executor with max_workers=1 (one thread)
    - Global asyncio.Lock() to serialize ALL query_rows() calls
    - No parallel SQL operations allowed

    This is slower but prevents crashes. Install ODBC Driver 17+ for
    better concurrency.

    For modern drivers (ODBC Driver 17/18):
    ───────────────────────────────────────
    Multiple lanes with concurrent execution, since modern drivers
    handle concurrent access properly.
    """

    def __init__(self) -> None:
        # Modern driver: multiple lanes
        self._pool: aioodbc.Pool | None = None
        self._pyodbc_executor: ThreadPoolExecutor | None = None
        self._lanes: list[_ConnectionLane] | None = None

        # Legacy driver: single pool + global serialization lock
        self._legacy_serial_lock: asyncio.Lock | None = None

        self._query_semaphore: asyncio.Semaphore | None = None
        self._slow_query_threshold_ms: int = settings.sql_slow_query_threshold_ms
        self._configured_driver: str = settings.sql_driver
        self._active_driver: str = ""
        self._is_legacy_driver: bool = False
        self._lane_count: int = 0

        # Performance monitoring
        self._stats: dict[str, QueryStats] = {}
        self._stats_lock = asyncio.Lock()

    async def start(self) -> None:
        if self._pool is not None or self._lanes is not None:
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
                    await self._start_legacy_serialized(dsn=dsn, driver=driver)
                else:
                    concurrency_limit = max(1, int(settings.sql_max_concurrent_queries))
                    await self._start_modern_pool(
                        dsn=dsn, driver=driver, concurrency_limit=concurrency_limit
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
        LEGACY DRIVER FIX: Single connection, fully serialized.
        
        The legacy SQLSRV32.dll driver crashes with concurrent connections.
        We use:
        - 1 pool with maxsize=1
        - 1 executor with 1 thread
        - 1 asyncio.Lock to serialize ALL operations
        
        This ensures only one SQL operation runs at any time.
        """
        log.warning(
            "Legacy SQL driver '%s' detected. Using FULLY SERIALIZED mode "
            "(1 connection, 1 thread, no parallelism). "
            "Install 'ODBC Driver 17 for SQL Server' for better performance "
            "and to prevent access violation crashes.",
            driver,
        )

        self._pyodbc_executor = ThreadPoolExecutor(
            max_workers=1,
            thread_name_prefix="pyodbc-legacy-serial",
        )

        self._pool = await aioodbc.create_pool(
            dsn=dsn,
            autocommit=True,
            minsize=1,
            maxsize=1,
            executor=self._pyodbc_executor,
        )

        # Global lock ensures only one query runs at a time
        self._legacy_serial_lock = asyncio.Lock()
        self._query_semaphore = asyncio.Semaphore(1)
        self._lane_count = 1
        self._lanes = None

        log.info(
            "SQL ODBC driver selected configured=%s active=%s",
            self._configured_driver,
            driver,
        )
        log.info(
            "SQL query concurrency SERIALIZED mode "
            "pool_maxsize=1 executor_workers=1 is_legacy=True "
            "global_lock=enabled (access violation prevention)",
        )

    async def _start_modern_pool(
        self, *, dsn: str, driver: str, concurrency_limit: int
    ) -> None:
        """Create lane-based pool for modern ODBC drivers."""
        # For modern drivers, use isolated lanes for better parallelism
        lane_count = min(concurrency_limit, 8)
        lane_count = max(lane_count, 2)

        log.info(
            "Modern SQL driver '%s': creating %d isolated lanes "
            "(each lane = 1 connection + 1 thread).",
            driver,
            lane_count,
        )

        lanes: list[_ConnectionLane] = []
        try:
            for i in range(lane_count):
                lane = _ConnectionLane(lane_id=i)
                await lane.start(dsn=dsn)
                lanes.append(lane)
        except Exception:
            for lane in lanes:
                await lane.close()
            raise

        self._lanes = lanes
        self._lane_count = lane_count
        self._query_semaphore = asyncio.Semaphore(lane_count)
        self._legacy_serial_lock = None

        log.info(
            "SQL ODBC driver selected configured=%s active=%s",
            self._configured_driver,
            driver,
        )
        log.info(
            "SQL query concurrency configured limit=%d "
            "lanes=%d (each: pool_maxsize=1, executor_workers=1) "
            "is_legacy=False",
            lane_count,
            lane_count,
        )

    async def _cleanup_on_failure(self) -> None:
        """Clean up partially-created resources on startup failure."""
        if self._lanes is not None:
            for lane in self._lanes:
                await lane.close()
            self._lanes = None
        if self._pool is not None:
            self._pool.close()
            await self._pool.wait_closed()
            self._pool = None
        if self._pyodbc_executor is not None:
            self._pyodbc_executor.shutdown(wait=False)
            self._pyodbc_executor = None
        self._legacy_serial_lock = None

    async def close(self) -> None:
        if self._pool is None and self._lanes is None:
            return
        log.info("Closing SQL pool")

        await self._log_stats()

        if self._lanes is not None:
            for lane in self._lanes:
                await lane.close()
            self._lanes = None
            log.debug("All %d lanes closed", self._lane_count)

        if self._pool is not None:
            self._pool.close()
            await self._pool.wait_closed()
            self._pool = None

        if self._pyodbc_executor is not None:
            self._pyodbc_executor.shutdown(wait=True, cancel_futures=False)
            self._pyodbc_executor = None

        self._query_semaphore = None
        self._legacy_serial_lock = None
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
        """Execute query with timing, semaphore control, and stats."""
        if self._pool is None and self._lanes is None:
            raise RuntimeError("SQL connection has not started")
        if self._query_semaphore is None:
            raise RuntimeError("SQL query semaphore is not initialized")

        query_preview = _compact_sql(
            query, max_chars=settings.log_sql_preview_chars
        )
        query_label = (query_name or "sql.query").strip() or "sql.query"

        started_at = time.perf_counter()

        # LEGACY DRIVER: Use global serialization lock
        if self._is_legacy_driver and self._legacy_serial_lock is not None:
            rows, cols = await self._query_legacy_serialized(
                query, params, query_label, query_preview, started_at
            )
        elif self._lanes is not None:
            # Modern driver with lanes
            rows, cols = await self._query_via_lane(
                query, params, query_label, query_preview, started_at
            )
        else:
            # Fallback: modern driver with single pool
            rows, cols = await self._query_via_pool(
                query, params, query_label, query_preview, started_at
            )

        if not cols:
            return []
        return [dict(zip(cols, row, strict=True)) for row in rows]

    async def _query_legacy_serialized(
        self,
        query: str,
        params: list[Any],
        query_label: str,
        query_preview: str,
        started_at: float,
    ) -> tuple[list[tuple], list[str]]:
        """
        LEGACY DRIVER: Fully serialized query execution.
        
        Uses global lock to ensure only one query runs at a time,
        preventing access violations in SQLSRV32.dll.
        """
        assert self._pool is not None
        assert self._legacy_serial_lock is not None

        async with self._legacy_serial_lock:
            return await self._execute_on_connection(
                self._pool,
                query,
                params,
                query_label,
                query_preview,
                started_at,
            )

    async def _query_via_lane(
        self,
        query: str,
        params: list[Any],
        query_label: str,
        query_preview: str,
        started_at: float,
    ) -> tuple[list[tuple], list[str]]:
        """Execute on one of the isolated lanes (modern driver)."""
        assert self._lanes is not None
        assert self._query_semaphore is not None

        async with self._query_semaphore:
            lane = await self._acquire_lane()
            try:
                return await self._execute_on_connection(
                    lane.pool,
                    query,
                    params,
                    query_label,
                    query_preview,
                    started_at,
                )
            finally:
                lane.release()

    async def _acquire_lane(self) -> _ConnectionLane:
        """Find an available lane. Semaphore guarantees one exists."""
        assert self._lanes is not None
        for _ in range(len(self._lanes) * 2):
            for lane in self._lanes:
                if lane.try_acquire():
                    return lane
            await asyncio.sleep(0)
        self._lanes[0].force_acquire()
        return self._lanes[0]

    async def _query_via_pool(
        self,
        query: str,
        params: list[Any],
        query_label: str,
        query_preview: str,
        started_at: float,
    ) -> tuple[list[tuple], list[str]]:
        """Execute via the shared pool (fallback for modern driver)."""
        assert self._pool is not None
        assert self._query_semaphore is not None

        async with self._query_semaphore:
            return await self._execute_on_connection(
                self._pool,
                query,
                params,
                query_label,
                query_preview,
                started_at,
            )

    async def _execute_on_connection(
        self,
        pool: aioodbc.Pool,
        query: str,
        params: list[Any],
        query_label: str,
        query_preview: str,
        started_at: float,
    ) -> tuple[list[tuple], list[str]]:
        """Core query execution with timing and logging."""
        exec_started_at = time.perf_counter()
        queue_wait_ms = int((exec_started_at - started_at) * 1000)
        execute_done_at = exec_started_at
        fetch_done_at = exec_started_at
        is_error = False

        async with pool.acquire() as conn:
            try:
                async with conn.cursor() as cur:
                    cur.timeout = settings.sql_query_timeout_seconds

                    await cur.execute(query, params)
                    execute_done_at = time.perf_counter()

                    rows: list[tuple[Any, ...]] = []
                    cols: list[str] = []

                    while True:
                        if cur.description is not None:
                            cols = [col[0] for col in cur.description]
                            rows = await cur.fetchall()
                            fetch_done_at = time.perf_counter()
                            break
                        has_next = await cur.nextset()
                        if not has_next:
                            fetch_done_at = time.perf_counter()
                            break

            except Exception:
                is_error = True
                failure_at = time.perf_counter()
                execute_done_at = max(execute_done_at, exec_started_at)
                fetch_done_at = max(fetch_done_at, execute_done_at)
                elapsed_ms = int((failure_at - started_at) * 1000)

                await self._record_stats(
                    query_label, elapsed_ms, False, True
                )

                log.exception(
                    "SQL query failed name=%s total_ms=%s "
                    "queue_wait_ms=%s sql=%s",
                    query_label,
                    elapsed_ms,
                    queue_wait_ms,
                    query_preview,
                )
                raise

        execute_ms = int((execute_done_at - exec_started_at) * 1000)
        fetch_ms = int((fetch_done_at - execute_done_at) * 1000)
        db_exec_ms = execute_ms + fetch_ms
        elapsed_ms = int((fetch_done_at - started_at) * 1000)
        row_count = len(rows)
        is_slow = elapsed_ms >= self._slow_query_threshold_ms

        await self._record_stats(
            query_label, elapsed_ms, is_slow, is_error
        )

        if is_slow:
            log.warning(
                "SQL query slow name=%s total_ms=%s queue_wait_ms=%s "
                "db_exec_ms=%s execute_ms=%s fetch_ms=%s rows=%s "
                "params=%s sql=%s",
                query_label,
                elapsed_ms,
                queue_wait_ms,
                db_exec_ms,
                execute_ms,
                fetch_ms,
                row_count,
                len(params),
                query_preview,
            )
        else:
            log.debug(
                "SQL query done name=%s total_ms=%s rows=%s",
                query_label,
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
# Connection Lane — one connection pinned to one thread (modern driver only)
# ═══════════════════════════════════════════════════════════════════════════


class _ConnectionLane:
    """
    An isolated (pool=1, executor=1) pair for modern ODBC drivers.
    
    NOT used for legacy driver - legacy uses single serialized pool.
    """

    def __init__(self, lane_id: int) -> None:
        self.lane_id = lane_id
        self.pool: aioodbc.Pool | None = None
        self._executor: ThreadPoolExecutor | None = None
        self._in_use = False

    async def start(self, *, dsn: str) -> None:
        self._executor = ThreadPoolExecutor(
            max_workers=1,
            thread_name_prefix=f"pyodbc-lane-{self.lane_id}",
        )
        self.pool = await aioodbc.create_pool(
            dsn=dsn,
            autocommit=True,
            minsize=1,
            maxsize=1,
            executor=self._executor,
        )
        log.debug("Lane %d started", self.lane_id)

    async def close(self) -> None:
        if self.pool is not None:
            self.pool.close()
            await self.pool.wait_closed()
            self.pool = None
        if self._executor is not None:
            self._executor.shutdown(wait=True, cancel_futures=False)
            self._executor = None
        log.debug("Lane %d closed", self.lane_id)

    def try_acquire(self) -> bool:
        if self._in_use:
            return False
        self._in_use = True
        return True

    def force_acquire(self) -> None:
        self._in_use = True

    def release(self) -> None:
        self._in_use = False


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