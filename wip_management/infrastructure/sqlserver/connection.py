from __future__ import annotations

import logging
import re
from typing import Any

import aioodbc

from wip_management.config import settings

log = logging.getLogger(__name__)
_DRIVER_VERSION_PATTERN = re.compile(r"^ODBC Driver (\d+) for SQL Server$", re.IGNORECASE)
_VALID_SQL_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class SQLServerConnection:
    def __init__(self) -> None:
        self._pool: aioodbc.pool.Pool | None = None

    async def start(self) -> None:
        if self._pool is not None:
            return
        candidates = build_driver_candidates(settings.sql_driver)
        if not candidates:
            raise RuntimeError(
                "No SQL Server ODBC driver detected. "
                "Use built-in 'SQL Server' driver or set SQL_DRIVER to an installed SQL Server driver."
            )

        last_error: Exception | None = None
        tried: list[str] = []
        for driver in candidates:
            tried.append(driver)
            try:
                self._pool = await aioodbc.create_pool(
                    dsn=settings.build_odbc_dsn(driver=driver),
                    autocommit=True,
                    minsize=1,
                    maxsize=16,
                )
                if driver != settings.sql_driver:
                    log.warning(
                        "Configured SQL_DRIVER='%s' not usable, fallback to '%s'",
                        settings.sql_driver,
                        driver,
                    )
                else:
                    log.info("SQL ODBC driver in use: %s", driver)
                return
            except Exception as exc:  # noqa: BLE001
                last_error = exc

        installed = list_sql_server_drivers()
        tried_text = ", ".join(tried)
        installed_text = ", ".join(installed) if installed else "<none>"
        raise RuntimeError(
            "Cannot connect to SQL Server with available ODBC drivers. "
            f"Attempted: [{tried_text}]. Installed SQL drivers: [{installed_text}]"
        ) from last_error

    async def close(self) -> None:
        if self._pool is None:
            return
        self._pool.close()
        await self._pool.wait_closed()
        self._pool = None

    async def query_rows(self, query: str, params: list[Any]) -> list[dict[str, Any]]:
        if self._pool is None:
            raise RuntimeError("SQL connection has not started")
        async with self._pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query, params)
                rows = await cur.fetchall()
                cols = [col[0] for col in cur.description]
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
