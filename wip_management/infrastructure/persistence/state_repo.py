from __future__ import annotations

import asyncio
import json
import logging
import os
import time
import uuid
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)


class SharedGroupingStateRepository:
    def __init__(self, directory: str, file_name: str) -> None:
        self._dir = Path(directory)
        self._path = self._dir / file_name
        self._lock_path = self._path.with_suffix(f"{self._path.suffix}.lock")

    async def load_manual_assignments(self) -> dict[str, tuple[str, str, str]]:
        document = await asyncio.to_thread(self._read_document_sync)
        raw = document.get("manual_assignments")
        if not isinstance(raw, dict):
            return {}
        out: dict[str, tuple[str, str, str]] = {}
        for tray_id, item in raw.items():
            if not isinstance(item, dict):
                continue
            tray_key = str(tray_id).strip()
            column = str(item.get("column", "")).strip()
            trolley_id = str(item.get("trolley_id", "")).strip()
            mode = str(item.get("mode", "manual")).strip().lower() or "manual"
            if not tray_key or not column or not trolley_id:
                continue
            out[tray_key] = (column, trolley_id, mode)
        return out

    async def load_projection(self) -> dict[str, Any]:
        document = await asyncio.to_thread(self._read_document_sync)
        raw = document.get("last_projection")
        if not isinstance(raw, dict):
            return {}
        return dict(raw)

    async def set_manual_assignment(self, tray_id: str, column: str, trolley_id: str, mode: str = "manual") -> None:
        tray_key = tray_id.strip()
        column_key = column.strip()
        trolley_key = trolley_id.strip()
        mode_key = str(mode).strip().lower() or "manual"
        if not tray_key or not column_key or not trolley_key:
            raise ValueError("tray_id, column, trolley_id must not be empty")
        await asyncio.to_thread(
            self._update_document_sync,
            _set_assignment_mutator(tray_key, column_key, trolley_key, mode_key),
        )

    async def remove_manual_assignment(self, tray_id: str) -> None:
        tray_key = tray_id.strip()
        if not tray_key:
            return
        await asyncio.to_thread(self._update_document_sync, _remove_assignment_mutator(tray_key))

    async def replace_manual_assignments(self, assignments: dict[str, tuple[str, str, str]]) -> None:
        sanitized: dict[str, tuple[str, str, str]] = {}
        for tray_id, item in assignments.items():
            tray_key = str(tray_id).strip()
            column, trolley_id, mode = item
            column_key = str(column).strip()
            trolley_key = str(trolley_id).strip()
            mode_key = str(mode).strip().lower() or "manual"
            if not tray_key or not column_key or not trolley_key:
                continue
            sanitized[tray_key] = (column_key, trolley_key, mode_key)
        await asyncio.to_thread(
            self._update_document_sync,
            _replace_assignments_mutator(sanitized),
        )

    async def save_projection(self, projection: dict[str, Any]) -> None:
        await asyncio.to_thread(self._update_document_sync, _save_projection_mutator(projection))

    def _read_document_sync(self) -> dict[str, Any]:
        if not self._path.exists():
            return _empty_document()
        try:
            loaded = json.loads(self._path.read_text(encoding="utf-8"))
            if isinstance(loaded, dict):
                return loaded
        except Exception:  # noqa: BLE001
            log.exception("Failed to read shared grouping state path=%s", self._path)
        return _empty_document()

    def _update_document_sync(self, mutator) -> None:
        self._dir.mkdir(parents=True, exist_ok=True)
        with self._acquire_lock_sync():
            document = self._read_document_sync()
            mutator(document)
            document["version"] = 1
            document["updated_at"] = datetime.now().isoformat()
            self._atomic_write_json_sync(document)

    def _atomic_write_json_sync(self, payload: dict[str, Any]) -> None:
        tmp_name = f".{self._path.name}.{uuid.uuid4().hex}.tmp"
        tmp_path = self._path.with_name(tmp_name)
        tmp_path.write_text(
            json.dumps(payload, ensure_ascii=True, separators=(",", ":")),
            encoding="utf-8",
        )
        os.replace(tmp_path, self._path)
        log.debug("Shared grouping state saved path=%s", self._path)

    @contextmanager
    def _acquire_lock_sync(self):
        deadline = time.time() + 30.0
        while True:
            try:
                fd = os.open(self._lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                os.write(fd, f"{os.getpid()} {time.time()}".encode("ascii", errors="ignore"))
                os.close(fd)
                break
            except FileExistsError:
                if time.time() > deadline:
                    raise TimeoutError(f"Timed out waiting for lock file: {self._lock_path}")
                time.sleep(0.1)
        try:
            yield
        finally:
            try:
                self._lock_path.unlink(missing_ok=True)
            except Exception:  # noqa: BLE001
                log.exception("Failed to release lock file path=%s", self._lock_path)


def _empty_document() -> dict[str, Any]:
    return {"version": 1, "manual_assignments": {}, "last_projection": {}, "updated_at": None}


def _set_assignment_mutator(tray_id: str, column: str, trolley_id: str, mode: str = "manual"):
    def _mutate(document: dict[str, Any]) -> None:
        raw = document.get("manual_assignments")
        manual_assignments = raw if isinstance(raw, dict) else {}
        manual_assignments[tray_id] = {"column": column, "trolley_id": trolley_id, "mode": mode}
        document["manual_assignments"] = manual_assignments

    return _mutate


def _remove_assignment_mutator(tray_id: str):
    def _mutate(document: dict[str, Any]) -> None:
        raw = document.get("manual_assignments")
        manual_assignments = raw if isinstance(raw, dict) else {}
        manual_assignments.pop(tray_id, None)
        document["manual_assignments"] = manual_assignments

    return _mutate


def _save_projection_mutator(projection: dict[str, Any]):
    def _mutate(document: dict[str, Any]) -> None:
        document["last_projection"] = projection
        document["last_projection_updated_at"] = datetime.now().isoformat()

    return _mutate


def _replace_assignments_mutator(assignments: dict[str, tuple[str, str, str]]):
    def _mutate(document: dict[str, Any]) -> None:
        manual_assignments: dict[str, dict[str, str]] = {}
        for tray_id, (column, trolley_id, mode) in assignments.items():
            manual_assignments[tray_id] = {"column": column, "trolley_id": trolley_id, "mode": mode}
        document["manual_assignments"] = manual_assignments

    return _mutate
