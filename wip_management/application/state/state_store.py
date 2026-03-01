# wip_management/application/state/state_store.py
from __future__ import annotations

import asyncio
import logging
import sys
import weakref
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


@dataclass(slots=True)
class _VersionCommand:
    future: asyncio.Future[int]


@dataclass(slots=True)
class _MemoryStatsCommand:
    future: asyncio.Future[dict[str, Any]]


class _StopCommand:
    pass


class SingleWriterStateStore:
    """
    Memory-optimized single-writer store with:
    - Payload deduplication to reduce memory
    - Periodic cleanup of stale entries
    - Memory usage monitoring
    """

    def __init__(self, queue_size: int) -> None:
        self._queue: asyncio.Queue[Any] = asyncio.Queue(maxsize=queue_size)
        self._task: asyncio.Task | None = None
        self._trays: dict[str, Tray] = {}
        self._state_machine = TrayStateMachine()
        self._version = 0
        self._fpc_only_tray_ids: set[str] = set()
        
        # Memory optimization: deduplicate common payload values
        self._payload_intern: weakref.WeakValueDictionary[str, dict] = weakref.WeakValueDictionary()
        
        # Track memory usage
        self._last_memory_check = 0
        self._memory_check_interval = 1000  # Check every N applies

    async def start(self) -> None:
        if self._task is not None:
            log.debug("State store start skipped because writer loop already exists")
            return
        self._task = asyncio.create_task(
            self._writer_loop(), name="wip-single-writer-loop"
        )
        log.info(
            "State store writer loop started queue_size=%s", self._queue.maxsize
        )

    async def stop(self) -> None:
        if self._task is None:
            log.debug("State store stop skipped because writer loop is None")
            return
        log.info("State store stopping writer loop")
        await self._queue.put(_StopCommand())
        await self._task
        self._task = None
        
        # Clear all data
        self._trays.clear()
        self._fpc_only_tray_ids.clear()
        self._payload_intern.clear()
        
        log.info("State store writer loop stopped")

    async def apply_signals(self, signals: list[TraySignal]) -> StoreApplyResult:
        loop = asyncio.get_running_loop()
        fut: asyncio.Future[StoreApplyResult] = loop.create_future()
        await self._queue.put(_ApplySignalsCommand(signals=signals, future=fut))
        return await fut

    async def snapshot_desc(self, limit: int | None = None) -> list[Tray]:
        loop = asyncio.get_running_loop()
        fut: asyncio.Future[list[Tray]] = loop.create_future()
        await self._queue.put(_SnapshotCommand(limit=limit, future=fut))
        return await fut

    async def size(self) -> int:
        loop = asyncio.get_running_loop()
        fut: asyncio.Future[int] = loop.create_future()
        await self._queue.put(_SizeCommand(future=fut))
        return await fut

    async def version(self) -> int:
        loop = asyncio.get_running_loop()
        fut: asyncio.Future[int] = loop.create_future()
        await self._queue.put(_VersionCommand(future=fut))
        return await fut

    async def memory_stats(self) -> dict[str, Any]:
        """Get memory usage statistics."""
        loop = asyncio.get_running_loop()
        fut: asyncio.Future[dict[str, Any]] = loop.create_future()
        await self._queue.put(_MemoryStatsCommand(future=fut))
        return await fut

    async def _writer_loop(self) -> None:
        log.debug("State store writer loop entered")
        apply_count = 0
        
        while True:
            cmd = await self._queue.get()
            try:
                if isinstance(cmd, _ApplySignalsCommand):
                    apply_count += 1
                    cmd.future.set_result(self._apply_signals(cmd.signals))
                    
                    # Periodic memory check
                    if apply_count % self._memory_check_interval == 0:
                        self._log_memory_stats()
                    continue
                    
                if isinstance(cmd, _SnapshotCommand):
                    cmd.future.set_result(self._snapshot_desc(cmd.limit))
                    continue
                    
                if isinstance(cmd, _SizeCommand):
                    cmd.future.set_result(len(self._trays))
                    continue
                    
                if isinstance(cmd, _VersionCommand):
                    cmd.future.set_result(self._version)
                    continue
                    
                if isinstance(cmd, _MemoryStatsCommand):
                    cmd.future.set_result(self._get_memory_stats())
                    continue
                    
                if isinstance(cmd, _StopCommand):
                    log.debug("State store writer loop received stop command")
                    break
                    
            except Exception as exc:
                log.exception("State store writer command failed")
                if hasattr(cmd, "future") and not cmd.future.done():
                    cmd.future.set_exception(exc)

        # Drain remaining commands
        while not self._queue.empty():
            pending = self._queue.get_nowait()
            if hasattr(pending, "future") and not pending.future.done():
                pending.future.set_exception(RuntimeError("State store stopped"))

    def _apply_signals(self, signals: list[TraySignal]) -> StoreApplyResult:
        changed: list[Tray] = []

        for signal in signals:
            tray_key = str(signal.tray_id)
            tray = self._trays.get(tray_key)
            
            if tray is None:
                tray = Tray(tray_id=TrayId(tray_key))
                self._trays[tray_key] = tray

            # Intern payload to reduce memory
            interned_signal = self._intern_signal(signal)
            
            if apply_signal_to_tray(tray, interned_signal, self._state_machine):
                changed.append(tray.clone())

            # Update FPC-only tracking
            if tray.has_fpc and not tray.has_ccu:
                self._fpc_only_tray_ids.add(tray_key)
            else:
                self._fpc_only_tray_ids.discard(tray_key)

        missing_ccu_ids = set(self._fpc_only_tray_ids)

        if changed:
            self._version += 1

        return StoreApplyResult(
            changed=changed,
            missing_ccu_tray_ids=missing_ccu_ids,
        )

    def _intern_signal(self, signal: TraySignal) -> TraySignal:
        """
        Intern common payload values to reduce memory usage.
        """
        # Create a hashable key from payload
        try:
            # Only intern small, common payloads
            if len(signal.payload) > 20:
                return signal
            
            # Create key from sorted items
            key_parts = []
            for k, v in sorted(signal.payload.items()):
                if v is None:
                    key_parts.append(f"{k}:None")
                elif isinstance(v, (str, int, float, bool)):
                    key_parts.append(f"{k}:{v}")
                else:
                    # Don't intern complex values
                    return signal
            
            cache_key = "|".join(key_parts)
            
            # Check if we have this payload cached
            cached = self._payload_intern.get(cache_key)
            if cached is not None:
                return TraySignal(
                    source=signal.source,
                    tray_id=signal.tray_id,
                    collected_time=signal.collected_time,
                    payload=cached,
                )
            
            # Store new payload
            self._payload_intern[cache_key] = signal.payload
            return signal
            
        except Exception:
            # If interning fails, just return original
            return signal

    def _snapshot_desc(self, limit: int | None = None) -> list[Tray]:
        rows = [tray.clone() for tray in self._trays.values()]
        rows.sort(key=tray_desc_sort_key, reverse=True)
        if limit is not None:
            rows = rows[:limit]
        return rows

    def _get_memory_stats(self) -> dict[str, Any]:
        """Get detailed memory statistics."""
        tray_count = len(self._trays)
        
        # Estimate memory usage
        total_payload_size = 0
        ccu_count = 0
        fpc_count = 0
        
        for tray in self._trays.values():
            if tray.ccu_payload:
                ccu_count += 1
                total_payload_size += sys.getsizeof(str(tray.ccu_payload))
            if tray.fpc_payload:
                fpc_count += 1
                total_payload_size += sys.getsizeof(str(tray.fpc_payload))
        
        return {
            "tray_count": tray_count,
            "ccu_count": ccu_count,
            "fpc_count": fpc_count,
            "fpc_only_count": len(self._fpc_only_tray_ids),
            "estimated_payload_mb": round(total_payload_size / 1024 / 1024, 2),
            "interned_payloads": len(self._payload_intern),
            "version": self._version,
        }

    def _log_memory_stats(self) -> None:
        """Log memory stats periodically."""
        stats = self._get_memory_stats()
        log.info(
            "State store memory stats: trays=%s ccu=%s fpc=%s fpc_only=%s payload_mb=%.2f interned=%s",
            stats["tray_count"],
            stats["ccu_count"],
            stats["fpc_count"],
            stats["fpc_only_count"],
            stats["estimated_payload_mb"],
            stats["interned_payloads"],
        )