from __future__ import annotations

import asyncio
import contextlib
from typing import Any


class AsyncEventBus:
    def __init__(self, default_queue_size: int) -> None:
        self._default_queue_size = default_queue_size
        self._subscribers: set[asyncio.Queue[Any]] = set()
        self._lock = asyncio.Lock()

    async def subscribe(self, maxsize: int | None = None) -> asyncio.Queue[Any]:
        queue = asyncio.Queue(maxsize=maxsize or self._default_queue_size)
        async with self._lock:
            self._subscribers.add(queue)
        return queue

    async def unsubscribe(self, queue: asyncio.Queue[Any]) -> None:
        async with self._lock:
            self._subscribers.discard(queue)

    async def publish(self, event: Any) -> None:
        async with self._lock:
            subscribers = list(self._subscribers)
        for queue in subscribers:
            try:
                queue.put_nowait(event)
            except asyncio.QueueFull:
                with contextlib.suppress(asyncio.QueueEmpty):
                    _ = queue.get_nowait()
                queue.put_nowait(event)

