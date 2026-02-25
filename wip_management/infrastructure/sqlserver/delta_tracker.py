from __future__ import annotations

import asyncio
import logging

from wip_management.domain.models.tray import Watermark

log = logging.getLogger(__name__)


class InMemoryDeltaTracker:
    def __init__(self) -> None:
        self._value: Watermark | None = None
        self._lock = asyncio.Lock()

    async def get(self) -> Watermark | None:
        async with self._lock:
            value = self._value
        log.debug(
            "Delta tracker get watermark=%s",
            value.collected_time.isoformat() if value else None,
        )
        return value

    async def set(self, watermark: Watermark) -> None:
        async with self._lock:
            self._value = watermark
        log.debug("Delta tracker set watermark=%s", watermark.collected_time.isoformat())
