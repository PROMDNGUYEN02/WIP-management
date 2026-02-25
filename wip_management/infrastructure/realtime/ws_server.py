from __future__ import annotations

import asyncio
import logging
from typing import Any

from fastapi import WebSocket

from wip_management.domain.events import SnapshotReady, TrolleyUpdated, TrayUpdated

log = logging.getLogger(__name__)


class WebSocketServerAdapter:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._connections: set[WebSocket] = set()

    async def connect(self, ws: WebSocket) -> None:
        await ws.accept()
        async with self._lock:
            self._connections.add(ws)

    async def disconnect(self, ws: WebSocket) -> None:
        async with self._lock:
            self._connections.discard(ws)

    async def broadcast(self, payload: dict[str, Any]) -> None:
        async with self._lock:
            targets = list(self._connections)
        dead: list[WebSocket] = []
        for ws in targets:
            try:
                await ws.send_json(payload)
            except Exception:  # noqa: BLE001
                dead.append(ws)
        if dead:
            async with self._lock:
                for ws in dead:
                    self._connections.discard(ws)

    async def event_pump(self, queue: asyncio.Queue[Any]) -> None:
        while True:
            event = await queue.get()
            payload = _event_to_payload(event)
            if payload is None:
                continue
            await self.broadcast(payload)


def _event_to_payload(event: Any) -> dict[str, Any] | None:
    if isinstance(event, TrayUpdated):
        return {"type": "TrayUpdated", "tray": event.tray.to_dict()}
    if isinstance(event, TrolleyUpdated):
        return {
            "type": "TrolleyUpdated",
            "trolleys": [t.to_dict() for t in event.trolleys],
            "assembly_trolleys": [t.to_dict() for t in event.assembly_trolleys],
            "queue_trolleys": [t.to_dict() for t in event.queue_trolleys],
            "precharge_trolleys": [t.to_dict() for t in event.precharge_trolleys],
            "assembly_ungrouped": [tray.to_dict() for tray in event.assembly_ungrouped],
        }
    if isinstance(event, SnapshotReady):
        return {
            "type": "SnapshotReady",
            "trays": [tray.to_dict() for tray in event.trays],
            "trolleys": [t.to_dict() for t in event.trolleys],
            "assembly_trolleys": [t.to_dict() for t in event.assembly_trolleys],
            "queue_trolleys": [t.to_dict() for t in event.queue_trolleys],
            "precharge_trolleys": [t.to_dict() for t in event.precharge_trolleys],
            "assembly_ungrouped": [tray.to_dict() for tray in event.assembly_ungrouped],
        }
    return None
