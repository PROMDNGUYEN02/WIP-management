from __future__ import annotations

import asyncio
import contextlib
import logging
from typing import Any

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from wip_management.infrastructure.realtime.ws_server import WebSocketServerAdapter

log = logging.getLogger(__name__)


class WsRuntime:
    def __init__(self, event_bus, ws_server: WebSocketServerAdapter, queue_size: int) -> None:
        self._event_bus = event_bus
        self._ws_server = ws_server
        self._queue_size = queue_size
        self._queue: asyncio.Queue[Any] | None = None
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        if self._task is not None:
            log.debug("WsRuntime start skipped because task already exists")
            return
        self._queue = await self._event_bus.subscribe(maxsize=self._queue_size)
        self._task = asyncio.create_task(self._ws_server.event_pump(self._queue), name="ws-event-pump")
        log.info("WsRuntime started queue_size=%s", self._queue_size)

    async def stop(self) -> None:
        if self._task is None:
            log.debug("WsRuntime stop skipped because task is None")
            return
        log.info("WsRuntime stopping")
        self._task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self._task
        self._task = None
        if self._queue is not None:
            await self._event_bus.unsubscribe(self._queue)
            self._queue = None
        log.info("WsRuntime stopped")


def build_ws_router(ws_server: WebSocketServerAdapter) -> APIRouter:
    router = APIRouter()

    @router.websocket("/ws/trays")
    async def ws_trays(ws: WebSocket) -> None:
        log.info("WS /ws/trays connect request client=%s", ws.client)
        await ws_server.connect(ws)
        try:
            while True:
                await ws.receive_text()
        except WebSocketDisconnect:
            log.info("WS /ws/trays disconnected client=%s", ws.client)
            pass
        finally:
            await ws_server.disconnect(ws)

    return router
