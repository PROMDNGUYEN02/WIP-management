from __future__ import annotations

import asyncio

from wip_management.domain.models.tray import Watermark


class InMemoryDeltaTracker:
    def __init__(self) -> None:
        self._value: Watermark | None = None
        self._lock = asyncio.Lock()

    async def get(self) -> Watermark | None:
        async with self._lock:
            return self._value

    async def set(self, watermark: Watermark) -> None:
        async with self._lock:
            self._value = watermark

