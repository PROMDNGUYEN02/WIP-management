from __future__ import annotations

import asyncio
import contextlib
import logging
from typing import Any

log = logging.getLogger(__name__)


class AsyncEventBus:
    def __init__(self, default_queue_size: int) -> None:
        self._default_queue_size = default_queue_size
        self._subscribers: set[asyncio.Queue[Any]] = set()
        self._lock = asyncio.Lock()

    async def subscribe(self, maxsize: int | None = None) -> asyncio.Queue[Any]:
        queue = asyncio.Queue(maxsize=maxsize or self._default_queue_size)
        async with self._lock:
            self._subscribers.add(queue)
            count = len(self._subscribers)
        log.info("Event bus subscribe subscribers=%s queue_size=%s", count, queue.maxsize)
        return queue

    async def unsubscribe(self, queue: asyncio.Queue[Any]) -> None:
        async with self._lock:
            self._subscribers.discard(queue)
            count = len(self._subscribers)
        log.info("Event bus unsubscribe subscribers=%s", count)

    async def publish(self, event: Any) -> None:
        async with self._lock:
            subscribers = list(self._subscribers)
        dropped = 0
        for queue in subscribers:
            try:
                queue.put_nowait(event)
            except asyncio.QueueFull:
                with contextlib.suppress(asyncio.QueueEmpty):
                    _ = queue.get_nowait()
                queue.put_nowait(event)
                dropped += 1
        if dropped:
            log.warning(
                "Event bus publish dropped_oldest=%s event_type=%s subscribers=%s",
                dropped,
                type(event).__name__,
                len(subscribers),
            )
        else:
            log.debug("Event bus publish event_type=%s subscribers=%s", type(event).__name__, len(subscribers))
