from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Any

from wip_management.application.state.reducers import apply_signal_to_tray, tray_desc_sort_key
from wip_management.domain.models.tray import Tray, TrayId, TraySignal
from wip_management.domain.state_machine import TrayStateMachine

log = logging.getLogger(__name__)


@dataclass(slots=True, frozen=True)
class StoreApplyResult:
    changed: list[Tray]
    missing_ccu_tray_ids: set[str]


@dataclass(slots=True)
class _ApplySignalsCommand:
    signals: list[TraySignal]
    future: asyncio.Future[StoreApplyResult]


@dataclass(slots=True)
class _SnapshotCommand:
    limit: int | None
    future: asyncio.Future[list[Tray]]


@dataclass(slots=True)
class _SizeCommand:
    future: asyncio.Future[int]


class _StopCommand:
    pass


class SingleWriterStateStore:
    """
    Single writer store:
    every state mutation is serialized through one async command loop.
    """

    def __init__(self, queue_size: int) -> None:
        self._queue: asyncio.Queue[Any] = asyncio.Queue(maxsize=queue_size)
        self._task: asyncio.Task | None = None
        self._trays: dict[str, Tray] = {}
        self._state_machine = TrayStateMachine()

    async def start(self) -> None:
        if self._task is not None:
            log.debug("State store start skipped because writer loop already exists")
            return
        self._task = asyncio.create_task(self._writer_loop(), name="wip-single-writer-loop")
        log.info("State store writer loop started queue_size=%s", self._queue.maxsize)

    async def stop(self) -> None:
        if self._task is None:
            log.debug("State store stop skipped because writer loop is None")
            return
        log.info("State store stopping writer loop")
        await self._queue.put(_StopCommand())
        await self._task
        self._task = None
        log.info("State store writer loop stopped")

    async def apply_signals(self, signals: list[TraySignal]) -> StoreApplyResult:
        loop = asyncio.get_running_loop()
        fut: asyncio.Future[StoreApplyResult] = loop.create_future()
        log.debug("State store enqueue apply_signals count=%s", len(signals))
        await self._queue.put(_ApplySignalsCommand(signals=signals, future=fut))
        return await fut

    async def snapshot_desc(self, limit: int | None = None) -> list[Tray]:
        loop = asyncio.get_running_loop()
        fut: asyncio.Future[list[Tray]] = loop.create_future()
        log.debug("State store enqueue snapshot_desc limit=%s", limit)
        await self._queue.put(_SnapshotCommand(limit=limit, future=fut))
        return await fut

    async def size(self) -> int:
        loop = asyncio.get_running_loop()
        fut: asyncio.Future[int] = loop.create_future()
        log.debug("State store enqueue size")
        await self._queue.put(_SizeCommand(future=fut))
        return await fut

    async def _writer_loop(self) -> None:
        log.debug("State store writer loop entered")
        while True:
            cmd = await self._queue.get()
            try:
                if isinstance(cmd, _ApplySignalsCommand):
                    cmd.future.set_result(self._apply_signals(cmd.signals))
                    continue
                if isinstance(cmd, _SnapshotCommand):
                    cmd.future.set_result(self._snapshot_desc(cmd.limit))
                    continue
                if isinstance(cmd, _SizeCommand):
                    cmd.future.set_result(len(self._trays))
                    continue
                if isinstance(cmd, _StopCommand):
                    log.debug("State store writer loop received stop command")
                    break
            except Exception as exc:  # noqa: BLE001
                log.exception("State store writer command failed")
                if hasattr(cmd, "future") and not cmd.future.done():
                    cmd.future.set_exception(exc)

        while not self._queue.empty():
            pending = self._queue.get_nowait()
            if hasattr(pending, "future") and not pending.future.done():
                pending.future.set_exception(RuntimeError("State store stopped"))
        log.debug("State store writer loop drained pending queue and exited")

    def _apply_signals(self, signals: list[TraySignal]) -> StoreApplyResult:
        changed: list[Tray] = []
        missing_ccu_ids: set[str] = set()

        for signal in signals:
            tray_key = str(signal.tray_id)
            tray = self._trays.get(tray_key)
            if tray is None:
                tray = Tray(tray_id=TrayId(tray_key))
                self._trays[tray_key] = tray

            if apply_signal_to_tray(tray, signal, self._state_machine):
                changed.append(tray.clone())

            if tray.has_fpc and (not tray.has_ccu):
                missing_ccu_ids.add(tray_key)
            else:
                missing_ccu_ids.discard(tray_key)

        log.debug(
            "State store applied signals input=%s changed=%s total_trays=%s missing_ccu=%s",
            len(signals),
            len(changed),
            len(self._trays),
            len(missing_ccu_ids),
        )
        return StoreApplyResult(changed=changed, missing_ccu_tray_ids=missing_ccu_ids)

    def _snapshot_desc(self, limit: int | None = None) -> list[Tray]:
        rows = [tray.clone() for tray in self._trays.values()]
        rows.sort(key=tray_desc_sort_key, reverse=True)
        if limit is not None:
            rows = rows[:limit]
        log.debug("State store snapshot built rows=%s limit=%s", len(rows), limit)
        return rows
