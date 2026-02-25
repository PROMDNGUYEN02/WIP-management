from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from wip_management.application.state.snapshots import StoreSnapshot


class JsonStateRepository:
    def __init__(self, file_path: str) -> None:
        self._path = Path(file_path)

    async def save(self, snapshot: StoreSnapshot) -> None:
        payload = {
            "created_at": snapshot.created_at.isoformat(),
            "watermark": {
                "collected_time": snapshot.watermark.collected_time.isoformat(),
                "tray_id": snapshot.watermark.tray_id,
            }
            if snapshot.watermark
            else None,
            "trays": snapshot.trays,
        }
        self._path.write_text(json.dumps(payload, ensure_ascii=True), encoding="utf-8")

    async def load(self) -> dict[str, Any] | None:
        if not self._path.exists():
            return None
        try:
            return json.loads(self._path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return None

