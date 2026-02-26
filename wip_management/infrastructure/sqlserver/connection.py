from __future__ import annotations

import asyncio
import logging
import re
import time
from typing import Any

import aioodbc

from wip_management.config import settings

log = logging.getLogger(__name__)
_DRIVER_VERSION_PATTERN = re.compile(r"^ODBC Driver (\d+) for SQL Server$", re.IGNORECASE)
_VALID_SQL_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class SQLServerConnection:
    def __init__(self) -> None:
        self._pool: aioodbc.pool.Pool | None = None
        self._query_semaphore: asyncio.Semaphore | None = None

    async def start(self) -> None:
        if self._pool is not None:
            log.debug("SQL pool already started")
            return
        candidates = build_driver_candidates(settings.sql_driver)
        if not candidates:
            raise RuntimeError(
                "No SQL Server ODBC driver detected. "
                "Use built-in 'SQL Server' driver or set SQL_DRIVER to an installed SQL Server driver."
            )
        log.info("Starting SQL pool with %s driver candidate(s): %s", len(candidates), candidates)

        last_error: Exception | None = None
        tried: list[str] = []
        for driver in candidates:
            tried.append(driver)
            try:
                log.debug("Trying SQL driver=%s", driver)
                concurrency_limit = max(1, int(settings.sql_max_concurrent_queries))
                # Legacy "SQL Server" driver is not stable under high concurrency.
                if driver.strip().lower() == "sql server":
                    if concurrency_limit != 1:
                        log.warning(
                            "Forcing SQL max concurrent queries to 1 for legacy driver '%s' (configured=%s)",
                            driver,
                            concurrency_limit,
                        )
                    concurrency_limit = 1
                pool_size = max(1, min(8, concurrency_limit))
                self._pool = await aioodbc.create_pool(
                    dsn=settings.build_odbc_dsn(driver=driver),
                    autocommit=True,
                    minsize=1,
                    maxsize=pool_size,
                )
                self._query_semaphore = asyncio.Semaphore(concurrency_limit)
                if driver != settings.sql_driver:
                    log.warning(
                        "Configured SQL_DRIVER='%s' not usable, fallback to '%s'",
                        settings.sql_driver,
                        driver,
                    )
                else:
                    log.info("SQL ODBC driver in use: %s", driver)
                log.info(
                    "SQL query concurrency configured limit=%s pool_maxsize=%s",
                    concurrency_limit,
                    pool_size,
                )
                return
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                log.warning("Failed to create SQL pool with driver=%s error=%r", driver, exc)

        installed = list_sql_server_drivers()
        tried_text = ", ".join(tried)
        installed_text = ", ".join(installed) if installed else "<none>"
        raise RuntimeError(
            "Cannot connect to SQL Server with available ODBC drivers. "
            f"Attempted: [{tried_text}]. Installed SQL drivers: [{installed_text}]"
        ) from last_error

    async def close(self) -> None:
        if self._pool is None:
            log.debug("SQL pool close skipped because pool is None")
            return
        log.info("Closing SQL pool")
        self._pool.close()
        await self._pool.wait_closed()
        self._pool = None
        self._query_semaphore = None
        log.info("SQL pool closed")

    async def query_rows(self, query: str, params: list[Any]) -> list[dict[str, Any]]:
        if self._pool is None:
            raise RuntimeError("SQL connection has not started")
        if self._query_semaphore is None:
            raise RuntimeError("SQL query semaphore is not initialized")
        query_preview = _compact_sql(query, max_chars=settings.log_sql_preview_chars)
        started_at = time.perf_counter()
        log.debug("SQL query start params=%s sql=%s", len(params), query_preview)
        async with self._query_semaphore:
            async with self._pool.acquire() as conn:
                try:
                    async with conn.cursor() as cur:
                        cur.timeout = settings.sql_query_timeout_seconds
                        await cur.execute(query, params)
                        rows = await cur.fetchall()
                        cols = [col[0] for col in cur.description]
                except Exception:  # noqa: BLE001
                    elapsed_ms = int((time.perf_counter() - started_at) * 1000)
                    log.exception(
                        "SQL query failed elapsed_ms=%s params=%s sql=%s",
                        elapsed_ms,
                        len(params),
                        query_preview,
                    )
                    raise
        elapsed_ms = int((time.perf_counter() - started_at) * 1000)
        row_count = len(rows)
        if elapsed_ms >= 2000:
            log.warning(
                "SQL query slow elapsed_ms=%s rows=%s params=%s sql=%s",
                elapsed_ms,
                row_count,
                len(params),
                query_preview,
            )
        else:
            log.debug(
                "SQL query done elapsed_ms=%s rows=%s params=%s",
                elapsed_ms,
                row_count,
                len(params),
            )
        return [dict(zip(cols, row, strict=True)) for row in rows]

    @staticmethod
    def table_name(schema: str, table: str) -> str:
        if not _VALID_SQL_IDENT.match(schema) or not _VALID_SQL_IDENT.match(table):
            raise ValueError("Invalid schema/table name")
        return f"[{schema}].[{table}]"

    @staticmethod
    def column_name(name: str) -> str:
        if not _VALID_SQL_IDENT.match(name):
            raise ValueError(f"Invalid column name: {name}")
        return f"[{name}]"


def list_sql_server_drivers() -> list[str]:
    try:
        import pyodbc
    except Exception:
        return []
    return [driver for driver in pyodbc.drivers() if "SQL Server" in driver]


def build_driver_candidates(preferred_driver: str) -> list[str]:
    preferred = preferred_driver.strip()
    installed = sorted(list_sql_server_drivers(), key=_driver_sort_key, reverse=True)
    out: list[str] = []
    if preferred:
        out.append(preferred)
    for driver in installed:
        if driver not in out:
            out.append(driver)
    return out


def _driver_sort_key(driver: str) -> tuple[int, str]:
    match = _DRIVER_VERSION_PATTERN.match(driver.strip())
    if match:
        return int(match.group(1)), driver
    return -1, driver


def _compact_sql(sql: str, *, max_chars: int) -> str:
    single_line = " ".join(sql.split())
    if len(single_line) <= max_chars:
        return single_line
    return f"{single_line[: max_chars - 3]}..."
