from __future__ import annotations

import asyncio
import contextlib
from typing import Any

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from wip_management.infrastructure.realtime.ws_server import WebSocketServerAdapter


class WsRuntime:
    def __init__(self, event_bus, ws_server: WebSocketServerAdapter, queue_size: int) -> None:
        self._event_bus = event_bus
        self._ws_server = ws_server
        self._queue_size = queue_size
        self._queue: asyncio.Queue[Any] | None = None
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        if self._task is not None:
            return
        self._queue = await self._event_bus.subscribe(maxsize=self._queue_size)
        self._task = asyncio.create_task(self._ws_server.event_pump(self._queue), name="ws-event-pump")

    async def stop(self) -> None:
        if self._task is None:
            return
        self._task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self._task
        self._task = None
        if self._queue is not None:
            await self._event_bus.unsubscribe(self._queue)
            self._queue = None


def build_ws_router(ws_server: WebSocketServerAdapter) -> APIRouter:
    router = APIRouter()

    @router.websocket("/ws/trays")
    async def ws_trays(ws: WebSocket) -> None:
        await ws_server.connect(ws)
        try:
            while True:
                await ws.receive_text()
        except WebSocketDisconnect:
            pass
        finally:
            await ws_server.disconnect(ws)

    return router

