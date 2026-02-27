from __future__ import annotations

import logging
import re
from datetime import datetime
from typing import Any

from wip_management.config import settings
from wip_management.domain.models.tray import SignalSource, TraySignal
from wip_management.infrastructure.sqlserver.connection import SQLServerConnection

log = logging.getLogger(__name__)

_NON_ALNUM = re.compile(r"[^A-Za-z0-9]+")


class DashboardRepo:
    def __init__(self, connection: SQLServerConnection) -> None:
        self._conn = connection
        self._event_table = SQLServerConnection.table_name(settings.sql_schema, settings.tray_event_norm_table)
        self._session_table = SQLServerConnection.table_name(settings.sql_schema, settings.trolley_session_table)
        self._timeline_table = SQLServerConnection.table_name(settings.sql_schema, settings.tray_timeline_table)
        self._tray_map_table = SQLServerConnection.table_name(settings.sql_schema, settings.tray_trolley_map_table)
        self._event_obj = f"{settings.sql_schema}.{settings.tray_event_norm_table}"
        self._session_obj = f"{settings.sql_schema}.{settings.trolley_session_table}"
        self._timeline_obj = f"{settings.sql_schema}.{settings.tray_timeline_table}"
        self._tray_map_obj = f"{settings.sql_schema}.{settings.tray_trolley_map_table}"
        self._write_enabled = True
        self._schema_available = True

    async def start(self) -> None:
        await self._conn.start()
        is_read_only = await self._is_database_read_only()
        if is_read_only:
            self._write_enabled = False
            self._schema_available = await self._check_schema_available()
            log.warning(
                "Dashboard repo detected read-only database; skip ensure_schema schema_available=%s",
                self._schema_available,
            )
            return
        try:
            await self._ensure_schema()
            self._write_enabled = True
            self._schema_available = True
            return
        except Exception as exc:  # noqa: BLE001
            if not _is_read_only_error(exc):
                raise
            self._write_enabled = False
            self._schema_available = await self._check_schema_available()
            log.warning(
                "Dashboard repo switched to read-only mode schema_available=%s reason=%s",
                self._schema_available,
                exc,
            )

    async def close(self) -> None:
        await self._conn.close()

    async def ingest_signals(self, signals: list[TraySignal]) -> None:
        if not self._write_enabled:
            return
        if not signals:
            return
        normalized_rows: list[tuple[str, str | None, datetime, str, str]] = []
        for signal in signals:
            tray_id_norm = _normalize_tray_id(str(signal.tray_id))
            if not tray_id_norm:
                continue
            event_source = signal.source.value.upper()
            event_kind = "CCU_OUT" if signal.source is SignalSource.CCU else "FPC_IN"
            normalized_rows.append(
                (
                    tray_id_norm,
                    _extract_trolley_id(signal.payload),
                    signal.collected_time,
                    event_source,
                    event_kind,
                )
            )
        if not normalized_rows:
            return

        chunk_size = 300
        inserted_total = 0
        for chunk in _chunked(normalized_rows, chunk_size):
            inserted_total += await self._insert_event_chunk(chunk)
            await self._upsert_timeline_chunk(chunk)
        log.debug(
            "Dashboard ingest_signals done input=%s normalized=%s inserted=%s",
            len(signals),
            len(normalized_rows),
            inserted_total,
        )

    async def map_trays_to_open_session(self, *, trolley_id: str, tray_ids: list[str]) -> str | None:
        if not self._write_enabled:
            return None
        trolley_key = trolley_id.strip()
        if not trolley_key:
            return None
        normalized_trays = sorted({_normalize_tray_id(tray_id) for tray_id in tray_ids if tray_id and tray_id.strip()})
        normalized_trays = [tray_id for tray_id in normalized_trays if tray_id]
        if not normalized_trays:
            return await self._ensure_open_session(trolley_key)

        session_id = await self._ensure_open_session(trolley_key)
        if not session_id:
            return None
        for chunk in _chunked(normalized_trays, 300):
            await self._upsert_tray_map_chunk(session_id=session_id, tray_ids=chunk)
        return session_id

    async def remove_tray_mappings(self, *, tray_ids: list[str]) -> int:
        if not self._write_enabled:
            return 0
        normalized_trays = sorted({_normalize_tray_id(tray_id) for tray_id in tray_ids if tray_id and tray_id.strip()})
        normalized_trays = [tray_id for tray_id in normalized_trays if tray_id]
        if not normalized_trays:
            return 0
        deleted = 0
        for chunk in _chunked(normalized_trays, 500):
            placeholders = ", ".join("?" for _ in chunk)
            query = (
                f"DELETE FROM {self._tray_map_table} "
                f"WHERE tray_id_norm IN ({placeholders}); "
                "SELECT @@ROWCOUNT AS deleted;"
            )
            rows = await self._conn.query_rows(
                query,
                list(chunk),
                query_name="dashboard.remove_tray_mappings",
            )
            deleted += _as_int(rows[0].get("deleted")) if rows else 0
        return deleted

    async def close_open_session(self, *, trolley_id: str) -> str | None:
        if not self._write_enabled:
            return None
        trolley_key = trolley_id.strip()
        if not trolley_key:
            return None
        query = (
            f";WITH target AS ("
            f"SELECT TOP 1 session_id FROM {self._session_table} "
            "WHERE trolley_id = ? AND status = 'OPEN' "
            "ORDER BY open_time DESC"
            ") "
            f"UPDATE tgt "
            "SET close_time = SYSUTCDATETIME(), "
            "status = 'CLOSED', "
            "last_update_time = SYSUTCDATETIME() "
            "OUTPUT CONVERT(NVARCHAR(36), inserted.session_id) AS session_id "
            f"FROM {self._session_table} AS tgt "
            "INNER JOIN target ON target.session_id = tgt.session_id;"
        )
        rows = await self._conn.query_rows(
            query,
            [trolley_key],
            query_name="dashboard.close_open_session",
        )
        if not rows:
            return None
        return str(rows[0].get("session_id") or "").strip() or None

    async def rename_open_session_trolley(self, *, old_trolley_id: str, new_trolley_id: str) -> int:
        if not self._write_enabled:
            return 0
        old_key = old_trolley_id.strip()
        new_key = new_trolley_id.strip()
        if not old_key or not new_key or old_key == new_key:
            return 0
        query = (
            "IF EXISTS ("
            f"SELECT 1 FROM {self._session_table} "
            "WHERE trolley_id = ? AND status = 'OPEN'"
            ") "
            "BEGIN "
            "SELECT CAST(0 AS INT) AS updated; "
            "END "
            "ELSE "
            "BEGIN "
            f"UPDATE {self._session_table} "
            "SET trolley_id = ?, last_update_time = SYSUTCDATETIME() "
            "WHERE trolley_id = ? AND status = 'OPEN'; "
            "SELECT @@ROWCOUNT AS updated; "
            "END"
        )
        rows = await self._conn.query_rows(
            query,
            [new_key, new_key, old_key],
            query_name="dashboard.rename_open_session_trolley",
        )
        if not rows:
            return 0
        return _as_int(rows[0].get("updated"))

    async def dashboard_sessions(
        self,
        *,
        include_closed: bool = False,
        only_wip: bool = False,
    ) -> list[dict[str, Any]]:
        if not self._schema_available:
            return []
        query = (
            "SELECT "
            "CONVERT(NVARCHAR(36), s.session_id) AS session_id, "
            "s.trolley_id AS trolley_id, "
            "s.open_time AS open_time, "
            "s.close_time AS close_time, "
            "s.status AS status, "
            "m.tray_id_norm AS tray_id_norm, "
            "m.assigned_time AS assigned_time, "
            "t.last_ccu_out_time AS last_ccu_out_time, "
            "t.last_fpc_in_time AS last_fpc_in_time "
            f"FROM {self._session_table} AS s "
            f"LEFT JOIN {self._tray_map_table} AS m ON m.session_id = s.session_id "
            f"LEFT JOIN {self._timeline_table} AS t ON t.tray_id_norm = m.tray_id_norm "
            "WHERE (? = 1 OR s.status = 'OPEN') "
            "AND (? = 0 OR ("
            "t.last_ccu_out_time IS NOT NULL AND "
            "(t.last_fpc_in_time IS NULL OR t.last_fpc_in_time < t.last_ccu_out_time)"
            ")) "
            "ORDER BY s.open_time DESC, s.trolley_id ASC, m.assigned_time ASC, m.tray_id_norm ASC"
        )
        try:
            rows = await self._conn.query_rows(
                query,
                [1 if include_closed else 0, 1 if only_wip else 0],
                query_name="dashboard.sessions",
            )
        except Exception as exc:  # noqa: BLE001
            if _is_missing_object_error(exc):
                self._schema_available = False
                log.warning("Dashboard sessions disabled because schema is missing error=%s", exc)
                return []
            raise
        grouped: dict[str, dict[str, Any]] = {}
        ordered_session_ids: list[str] = []
        for row in rows:
            session_id = str(row.get("session_id") or "").strip()
            if not session_id:
                continue
            item = grouped.get(session_id)
            if item is None:
                item = {
                    "session_id": session_id,
                    "trolley_id": str(row.get("trolley_id") or "").strip(),
                    "open_time": _to_iso(row.get("open_time")),
                    "close_time": _to_iso(row.get("close_time")),
                    "status": str(row.get("status") or "").strip().upper(),
                    "trays": [],
                }
                grouped[session_id] = item
                ordered_session_ids.append(session_id)

            tray_id = str(row.get("tray_id_norm") or "").strip()
            if not tray_id:
                continue
            last_ccu_out = _to_iso(row.get("last_ccu_out_time"))
            last_fpc_in = _to_iso(row.get("last_fpc_in_time"))
            is_wip = bool(
                row.get("last_ccu_out_time") is not None
                and (row.get("last_fpc_in_time") is None or row.get("last_fpc_in_time") < row.get("last_ccu_out_time"))
            )
            item["trays"].append(
                {
                    "tray_id": tray_id,
                    "last_ccu_out_time": last_ccu_out,
                    "last_fpc_in_time": last_fpc_in,
                    "is_wip": is_wip,
                }
            )

        out: list[dict[str, Any]] = []
        for session_id in ordered_session_ids:
            session = grouped[session_id]
            trays = session["trays"]
            session["tray_count"] = len(trays)
            session["wip_count"] = sum(1 for row in trays if row.get("is_wip"))
            out.append(session)
        return out

    async def _ensure_schema(self) -> None:
        query = f"""
IF OBJECT_ID('{self._event_obj}', 'U') IS NULL
BEGIN
    CREATE TABLE {self._event_table} (
        event_id BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        tray_id_norm NVARCHAR(64) NOT NULL,
        trolley_id NVARCHAR(32) NULL,
        event_time DATETIME2(3) NOT NULL,
        event_source NVARCHAR(16) NOT NULL,
        event_kind NVARCHAR(16) NOT NULL,
        created_at DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME()
    );
END;

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE object_id = OBJECT_ID('{self._event_obj}')
      AND name = 'UX_WIP_TRAY_EVENT_NORM_DEDUP'
)
BEGIN
    CREATE UNIQUE INDEX [UX_WIP_TRAY_EVENT_NORM_DEDUP]
        ON {self._event_table}(tray_id_norm, event_time, event_source, event_kind);
END;

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE object_id = OBJECT_ID('{self._event_obj}')
      AND name = 'IX_WIP_TRAY_EVENT_NORM_TRAY_TIME'
)
BEGIN
    CREATE INDEX [IX_WIP_TRAY_EVENT_NORM_TRAY_TIME]
        ON {self._event_table}(tray_id_norm, event_time DESC);
END;

IF OBJECT_ID('{self._session_obj}', 'U') IS NULL
BEGIN
    CREATE TABLE {self._session_table} (
        session_id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
        trolley_id NVARCHAR(32) NOT NULL,
        open_time DATETIME2(3) NOT NULL,
        close_time DATETIME2(3) NULL,
        status VARCHAR(10) NOT NULL,
        last_update_time DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT CK_WIP_TROLLEY_SESSION_STATUS CHECK (status IN ('OPEN', 'CLOSED'))
    );
END;

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE object_id = OBJECT_ID('{self._session_obj}')
      AND name = 'UX_WIP_TROLLEY_SESSION_OPEN'
)
BEGIN
    CREATE UNIQUE INDEX [UX_WIP_TROLLEY_SESSION_OPEN]
        ON {self._session_table}(trolley_id)
        WHERE status = 'OPEN';
END;

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE object_id = OBJECT_ID('{self._session_obj}')
      AND name = 'IX_WIP_TROLLEY_SESSION_TROLLEY_TIME'
)
BEGIN
    CREATE INDEX [IX_WIP_TROLLEY_SESSION_TROLLEY_TIME]
        ON {self._session_table}(trolley_id, open_time DESC);
END;

IF OBJECT_ID('{self._timeline_obj}', 'U') IS NULL
BEGIN
    CREATE TABLE {self._timeline_table} (
        tray_id_norm NVARCHAR(64) NOT NULL PRIMARY KEY,
        last_ccu_out_time DATETIME2(3) NULL,
        last_fpc_in_time DATETIME2(3) NULL,
        last_update_time DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME()
    );
END;

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE object_id = OBJECT_ID('{self._timeline_obj}')
      AND name = 'IX_WIP_TRAY_TIMELINE_CCU_FPC'
)
BEGIN
    CREATE INDEX [IX_WIP_TRAY_TIMELINE_CCU_FPC]
        ON {self._timeline_table}(last_ccu_out_time DESC, last_fpc_in_time DESC);
END;

IF OBJECT_ID('{self._tray_map_obj}', 'U') IS NULL
BEGIN
    CREATE TABLE {self._tray_map_table} (
        tray_id_norm NVARCHAR(64) NOT NULL PRIMARY KEY,
        session_id UNIQUEIDENTIFIER NOT NULL,
        assigned_time DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT FK_WIP_TRAY_TROLLEY_MAP_SESSION
            FOREIGN KEY (session_id) REFERENCES {self._session_table}(session_id)
    );
END;

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE object_id = OBJECT_ID('{self._tray_map_obj}')
      AND name = 'IX_WIP_TRAY_TROLLEY_MAP_SESSION'
)
BEGIN
    CREATE INDEX [IX_WIP_TRAY_TROLLEY_MAP_SESSION]
        ON {self._tray_map_table}(session_id, assigned_time ASC);
END;

SELECT 1 AS ok;
"""
        await self._conn.query_rows(
            query,
            [],
            query_name="dashboard.ensure_schema",
        )

    async def _check_schema_available(self) -> bool:
        query = (
            "SELECT "
            "CASE WHEN OBJECT_ID(?, 'U') IS NOT NULL THEN 1 ELSE 0 END AS has_event, "
            "CASE WHEN OBJECT_ID(?, 'U') IS NOT NULL THEN 1 ELSE 0 END AS has_session, "
            "CASE WHEN OBJECT_ID(?, 'U') IS NOT NULL THEN 1 ELSE 0 END AS has_timeline, "
            "CASE WHEN OBJECT_ID(?, 'U') IS NOT NULL THEN 1 ELSE 0 END AS has_map"
        )
        rows = await self._conn.query_rows(
            query,
            [
                self._event_obj,
                self._session_obj,
                self._timeline_obj,
                self._tray_map_obj,
            ],
            query_name="dashboard.check_schema_available",
        )
        if not rows:
            return False
        row = rows[0]
        return (
            _as_int(row.get("has_event")) == 1
            and _as_int(row.get("has_session")) == 1
            and _as_int(row.get("has_timeline")) == 1
            and _as_int(row.get("has_map")) == 1
        )

    async def _is_database_read_only(self) -> bool:
        query = (
            "SELECT CONVERT(NVARCHAR(60), DATABASEPROPERTYEX(DB_NAME(), 'Updateability')) AS updateability"
        )
        rows = await self._conn.query_rows(
            query,
            [],
            query_name="dashboard.db_updateability",
        )
        if not rows:
            return False
        updateability = str(rows[0].get("updateability") or "").strip().upper()
        return updateability == "READ_ONLY"

    async def _insert_event_chunk(self, rows: list[tuple[str, str | None, datetime, str, str]]) -> int:
        values_sql = ", ".join("(?, ?, ?, ?, ?)" for _ in rows)
        params: list[Any] = []
        for row in rows:
            params.extend(row)
        query = (
            "WITH src(tray_id_norm, trolley_id, event_time, event_source, event_kind) AS ("
            f"SELECT * FROM (VALUES {values_sql}) AS v(tray_id_norm, trolley_id, event_time, event_source, event_kind)"
            ") "
            f"INSERT INTO {self._event_table}(tray_id_norm, trolley_id, event_time, event_source, event_kind) "
            "SELECT s.tray_id_norm, s.trolley_id, s.event_time, s.event_source, s.event_kind "
            "FROM src AS s "
            "WHERE NOT EXISTS ("
            f"SELECT 1 FROM {self._event_table} AS e "
            "WHERE e.tray_id_norm = s.tray_id_norm "
            "AND e.event_time = s.event_time "
            "AND e.event_source = s.event_source "
            "AND e.event_kind = s.event_kind"
            "); "
            "SELECT @@ROWCOUNT AS inserted;"
        )
        inserted_rows = await self._conn.query_rows(
            query,
            params,
            query_name="dashboard.insert_event_chunk",
        )
        if not inserted_rows:
            return 0
        return _as_int(inserted_rows[0].get("inserted"))

    async def _upsert_timeline_chunk(self, rows: list[tuple[str, str | None, datetime, str, str]]) -> None:
        values_sql = ", ".join("(?, ?, ?)" for _ in rows)
        params: list[Any] = []
        for tray_id_norm, _, event_time, _, event_kind in rows:
            params.extend([tray_id_norm, event_kind, event_time])
        query = (
            "WITH src(tray_id_norm, event_kind, event_time) AS ("
            f"SELECT * FROM (VALUES {values_sql}) AS v(tray_id_norm, event_kind, event_time)"
            "), agg AS ("
            "SELECT "
            "tray_id_norm, "
            "MAX(CASE WHEN event_kind = 'CCU_OUT' THEN event_time END) AS max_ccu_out_time, "
            "MAX(CASE WHEN event_kind = 'FPC_IN' THEN event_time END) AS max_fpc_in_time "
            "FROM src "
            "GROUP BY tray_id_norm"
            ") "
            f"MERGE {self._timeline_table} AS tgt "
            "USING agg AS s ON s.tray_id_norm = tgt.tray_id_norm "
            "WHEN MATCHED THEN UPDATE SET "
            "last_ccu_out_time = CASE "
            "WHEN s.max_ccu_out_time IS NULL THEN tgt.last_ccu_out_time "
            "WHEN tgt.last_ccu_out_time IS NULL THEN s.max_ccu_out_time "
            "WHEN s.max_ccu_out_time > tgt.last_ccu_out_time THEN s.max_ccu_out_time "
            "ELSE tgt.last_ccu_out_time END, "
            "last_fpc_in_time = CASE "
            "WHEN s.max_fpc_in_time IS NULL THEN tgt.last_fpc_in_time "
            "WHEN tgt.last_fpc_in_time IS NULL THEN s.max_fpc_in_time "
            "WHEN s.max_fpc_in_time > tgt.last_fpc_in_time THEN s.max_fpc_in_time "
            "ELSE tgt.last_fpc_in_time END, "
            "last_update_time = SYSUTCDATETIME() "
            "WHEN NOT MATCHED THEN INSERT "
            "(tray_id_norm, last_ccu_out_time, last_fpc_in_time, last_update_time) "
            "VALUES (s.tray_id_norm, s.max_ccu_out_time, s.max_fpc_in_time, SYSUTCDATETIME()); "
            "SELECT 1 AS ok;"
        )
        await self._conn.query_rows(
            query,
            params,
            query_name="dashboard.upsert_timeline_chunk",
        )

    async def _ensure_open_session(self, trolley_id: str) -> str | None:
        query = (
            "DECLARE @session_id UNIQUEIDENTIFIER; "
            f"SELECT TOP 1 @session_id = session_id FROM {self._session_table} "
            "WHERE trolley_id = ? AND status = 'OPEN' "
            "ORDER BY open_time DESC; "
            "IF @session_id IS NULL "
            "BEGIN "
            "SET @session_id = NEWSEQUENTIALID(); "
            f"INSERT INTO {self._session_table} "
            "(session_id, trolley_id, open_time, close_time, status, last_update_time) "
            "VALUES (@session_id, ?, SYSUTCDATETIME(), NULL, 'OPEN', SYSUTCDATETIME()); "
            "END; "
            "SELECT CONVERT(NVARCHAR(36), @session_id) AS session_id;"
        )
        rows = await self._conn.query_rows(
            query,
            [trolley_id, trolley_id],
            query_name="dashboard.ensure_open_session",
        )
        if not rows:
            return None
        return str(rows[0].get("session_id") or "").strip() or None

    async def _upsert_tray_map_chunk(self, *, session_id: str, tray_ids: list[str]) -> None:
        values_sql = ", ".join("(?, ?)" for _ in tray_ids)
        params: list[Any] = []
        for tray_id in tray_ids:
            params.extend([tray_id, session_id])
        query = (
            "WITH src(tray_id_norm, session_id) AS ("
            f"SELECT * FROM (VALUES {values_sql}) AS v(tray_id_norm, session_id)"
            ") "
            f"MERGE {self._tray_map_table} AS tgt "
            "USING src AS s ON s.tray_id_norm = tgt.tray_id_norm "
            "WHEN MATCHED AND tgt.session_id <> CONVERT(UNIQUEIDENTIFIER, s.session_id) THEN "
            "UPDATE SET session_id = CONVERT(UNIQUEIDENTIFIER, s.session_id), assigned_time = SYSUTCDATETIME() "
            "WHEN NOT MATCHED THEN "
            "INSERT (tray_id_norm, session_id, assigned_time) "
            "VALUES (s.tray_id_norm, CONVERT(UNIQUEIDENTIFIER, s.session_id), SYSUTCDATETIME()); "
            "SELECT 1 AS ok;"
        )
        await self._conn.query_rows(
            query,
            params,
            query_name="dashboard.upsert_tray_map_chunk",
        )


def _normalize_tray_id(raw_value: str) -> str:
    text = raw_value.strip().strip("\"'[]()")
    if not text:
        return ""
    return _NON_ALNUM.sub("", text).upper()


def _extract_trolley_id(payload: dict[str, Any]) -> str | None:
    if not payload:
        return None
    for key in ("trolley_id", "trolleyId", "TROLLEY_ID", "trolley"):
        value = payload.get(key)
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return None


def _to_iso(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    text = str(value).strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).isoformat()
    except ValueError:
        return text


def _as_int(value: Any) -> int:
    if value is None:
        return 0
    if isinstance(value, int):
        return value
    try:
        return int(str(value).strip())
    except Exception:  # noqa: BLE001
        return 0


def _chunked(rows: list[Any], chunk_size: int) -> list[list[Any]]:
    if chunk_size <= 0:
        return [rows]
    out: list[list[Any]] = []
    for idx in range(0, len(rows), chunk_size):
        out.append(rows[idx : idx + chunk_size])
    return out


def _is_read_only_error(exc: Exception) -> bool:
    text = str(exc).upper()
    return ("READ-ONLY" in text) or ("(3906)" in text)


def _is_missing_object_error(exc: Exception) -> bool:
    text = str(exc).upper()
    return ("INVALID OBJECT NAME" in text) or ("(208)" in text)
