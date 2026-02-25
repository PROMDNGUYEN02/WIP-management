from __future__ import annotations

import asyncio
import contextlib
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta

from wip_management.application.ports import (
    CcuRepoPort,
    ClockPort,
    DeltaTrackerPort,
    EventBusPort,
    FpcRepoPort,
)
from wip_management.application.state.state_store import SingleWriterStateStore
from wip_management.application.use_cases.ingest_signals import IngestResult, IngestSignalsUseCase
from wip_management.application.use_cases.manual_group import ManualGroupUseCase
from wip_management.application.use_cases.recompute_projection import Projection, RecomputeProjectionUseCase
from wip_management.domain.events import SnapshotReady, TrolleyUpdated, TrayUpdated
from wip_management.domain.models.tray import TrayId, TraySignal, Watermark
from wip_management.domain.models.trolley import Column

log = logging.getLogger(__name__)


class OrchestratorService:
    def __init__(
        self,
        ccu_repo: CcuRepoPort,
        fpc_repo: FpcRepoPort,
        delta_tracker: DeltaTrackerPort,
        event_bus: EventBusPort,
        state_store: SingleWriterStateStore,
        clock: ClockPort,
        *,
        initial_load_start_hour: int,
        delta_poll_interval_seconds: float,
        backfill_cooldown_seconds: float,
        max_parallel_workers: int,
        snapshot_limit: int,
        max_trays_per_trolley: int,
        assembly_auto_trolley_count: int,
    ) -> None:
        self._ccu_repo = ccu_repo
        self._fpc_repo = fpc_repo
        self._delta_tracker = delta_tracker
        self._event_bus = event_bus
        self._store = state_store
        self._clock = clock

        self._initial_load_start_hour = initial_load_start_hour
        self._delta_poll_interval_seconds = delta_poll_interval_seconds
        self._backfill_cooldown = timedelta(seconds=backfill_cooldown_seconds)
        self._snapshot_limit = snapshot_limit
        self._max_trays_per_trolley = max_trays_per_trolley
        self._assembly_auto_trolley_count = assembly_auto_trolley_count

        self._ingest = IngestSignalsUseCase(store=state_store)
        self._recompute_projection = RecomputeProjectionUseCase()
        self._manual_group = ManualGroupUseCase()
        self._executor = ThreadPoolExecutor(max_workers=max_parallel_workers)

        self._running = False
        self._refresh_task: asyncio.Task | None = None
        self._last_backfill_attempt: dict[str, datetime] = {}

    async def start(self) -> None:
        if self._running:
            return
        await _maybe_start(self._ccu_repo)
        await _maybe_start(self._fpc_repo)
        await self._store.start()
        self._running = True
        await self._initial_load()
        self._refresh_task = asyncio.create_task(self._refresh_loop(), name="wip-delta-refresh-loop")

    async def stop(self) -> None:
        if not self._running:
            return
        self._running = False
        if self._refresh_task is not None:
            self._refresh_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._refresh_task
            self._refresh_task = None
        self._executor.shutdown(wait=True, cancel_futures=False)
        await self._store.stop()
        await _maybe_close(self._ccu_repo)
        await _maybe_close(self._fpc_repo)

    async def tray_snapshot(self, limit: int | None = None) -> list[dict]:
        rows = await self._store.snapshot_desc(limit=limit)
        return [tray.to_dict() for tray in rows]

    async def tray_count(self) -> int:
        return await self._store.size()

    async def current_watermark(self) -> Watermark | None:
        return await self._delta_tracker.get()

    async def projection_snapshot(self) -> dict[str, object]:
        projection = await self._compute_projection()
        return _projection_to_payload(projection)

    async def group_tray_manual(self, tray_id: str, trolley_id: str, column: Column) -> None:
        self._manual_group.group_to_trolley(TrayId(tray_id), trolley_id=trolley_id, column=column)
        projection = await self._compute_projection()
        await self._publish_trolley_updated(projection)

    async def ungroup_tray_manual(self, tray_id: str) -> None:
        self._manual_group.ungroup(TrayId(tray_id))
        projection = await self._compute_projection()
        await self._publish_trolley_updated(projection)

    async def manual_refresh(self, *, full_scan: bool) -> dict[str, object]:
        watermark = await self._delta_tracker.get()
        now = self._clock.now()
        if watermark is None:
            await self._initial_load()
            return {"mode": "bootstrap", "changed": True}

        if full_scan:
            result = await self._run_full_window_scan(now)
            changed = bool(result.store_result.changed)
            projection = await self._compute_projection()
            await self._publish_trolley_updated(projection)
            await self._set_refresh_watermark(now)
            return {"mode": "manual_full", "changed": changed, "merged": len(result.merged)}

        if not await self._has_new_data_since(watermark.collected_time, now):
            await self._set_refresh_watermark(now)
            return {"mode": "manual_quick", "changed": False, "merged": 0}

        result = await self._run_delta_scan(watermark=watermark, end_time=now)
        backfill_changed = await self._maybe_backfill_ccu(result.store_result.missing_ccu_tray_ids)
        changed = bool(result.store_result.changed or backfill_changed)
        if changed:
            for tray in result.store_result.changed:
                await self._event_bus.publish(TrayUpdated(tray=tray))
            projection = await self._compute_projection()
            await self._publish_trolley_updated(projection)
        await self._set_refresh_watermark(now)
        return {"mode": "manual_quick", "changed": changed, "merged": len(result.merged)}

    async def _initial_load(self) -> None:
        now = self._clock.now()
        result = await self._run_full_window_scan(now)
        await self._set_refresh_watermark(now)
        projection = await self._compute_projection()
        await self._event_bus.publish(
            SnapshotReady(
                trays=projection.trays,
                trolleys=projection.trolleys,
                assembly_trolleys=projection.assembly_trolleys,
                queue_trolleys=projection.queue_trolleys,
                precharge_trolleys=projection.precharge_trolleys,
                assembly_ungrouped=projection.assembly_ungrouped,
            )
        )
        log.info(
            "Initial load completed. merged=%s trays=%s watermark=%s",
            len(result.merged),
            len(projection.trays),
            now.isoformat(),
        )

    async def _refresh_loop(self) -> None:
        while self._running:
            try:
                await self._refresh_once()
            except Exception:  # noqa: BLE001
                log.exception("Refresh loop failed")
            await asyncio.sleep(self._delta_poll_interval_seconds)

    async def _refresh_once(self) -> None:
        watermark = await self._delta_tracker.get()
        if watermark is None:
            return
        now = self._clock.now()
        if not await self._has_new_data_since(watermark.collected_time, now):
            await self._set_refresh_watermark(now)
            return

        ingest_result = await self._run_delta_scan(watermark=watermark, end_time=now)
        backfill_changed = await self._maybe_backfill_ccu(ingest_result.store_result.missing_ccu_tray_ids)

        if ingest_result.store_result.changed:
            for tray in ingest_result.store_result.changed:
                await self._event_bus.publish(TrayUpdated(tray=tray))

        if ingest_result.store_result.changed or backfill_changed:
            projection = await self._compute_projection()
            await self._publish_trolley_updated(projection)

        await self._set_refresh_watermark(now)

    async def _run_full_window_scan(self, end_time: datetime) -> IngestResult:
        start = end_time.replace(
            hour=self._initial_load_start_hour,
            minute=0,
            second=0,
            microsecond=0,
        )
        ccu_rows, fpc_rows = await asyncio.gather(
            self._ccu_repo.fetch_initial(start_time=start, end_time=end_time),
            self._fpc_repo.fetch_initial(start_time=start, end_time=end_time),
        )
        norm_ccu, norm_fpc = await asyncio.gather(
            self._normalize_signals(ccu_rows),
            self._normalize_signals(fpc_rows),
        )
        previous = await self._delta_tracker.get()
        return await self._ingest.execute(norm_ccu, norm_fpc, previous_watermark=previous)

    async def _run_delta_scan(self, *, watermark: Watermark, end_time: datetime) -> IngestResult:
        ccu_rows, fpc_rows = await asyncio.gather(
            self._ccu_repo.fetch_delta(watermark=watermark, end_time=end_time),
            self._fpc_repo.fetch_delta(watermark=watermark, end_time=end_time),
        )
        norm_ccu, norm_fpc = await asyncio.gather(
            self._normalize_signals(ccu_rows),
            self._normalize_signals(fpc_rows),
        )
        return await self._ingest.execute(norm_ccu, norm_fpc, previous_watermark=watermark)

    async def _has_new_data_since(self, since: datetime, end_time: datetime) -> bool:
        ccu_latest, fpc_latest = await asyncio.gather(
            self._ccu_repo.peek_latest_signal_time(end_time=end_time),
            self._fpc_repo.peek_latest_signal_time(end_time=end_time),
        )
        if ccu_latest is not None and ccu_latest > since:
            return True
        if fpc_latest is not None and fpc_latest > since:
            return True
        return False

    async def _maybe_backfill_ccu(self, tray_ids: set[str]) -> bool:
        if not tray_ids:
            return False

        now = self._clock.now()
        eligible: list[str] = []
        for tray_id in tray_ids:
            last = self._last_backfill_attempt.get(tray_id)
            if last is None or (now - last) >= self._backfill_cooldown:
                eligible.append(tray_id)
                self._last_backfill_attempt[tray_id] = now
        if not eligible:
            return False

        ccu_rows = await self._ccu_repo.fetch_by_tray_ids(tray_ids=eligible, end_time=now)
        if not ccu_rows:
            return False
        norm_ccu = await self._normalize_signals(ccu_rows)
        current_wm = await self._delta_tracker.get()
        ingest_result = await self._ingest.execute(norm_ccu, [], previous_watermark=current_wm)
        if not ingest_result.store_result.changed:
            return False
        for tray in ingest_result.store_result.changed:
            await self._event_bus.publish(TrayUpdated(tray=tray))
        return True

    async def _compute_projection(self) -> Projection:
        snapshot = await self._store.snapshot_desc(limit=self._snapshot_limit)
        return self._recompute_projection.execute(
            snapshot,
            manual_assignments=self._manual_group.projection_assignments(),
            max_per_trolley=self._max_trays_per_trolley,
            assembly_auto_trolley_count=self._assembly_auto_trolley_count,
        )

    async def _publish_trolley_updated(self, projection: Projection) -> None:
        await self._event_bus.publish(
            TrolleyUpdated(
                trolleys=projection.trolleys,
                assembly_trolleys=projection.assembly_trolleys,
                queue_trolleys=projection.queue_trolleys,
                precharge_trolleys=projection.precharge_trolleys,
                assembly_ungrouped=projection.assembly_ungrouped,
            )
        )

    async def _set_refresh_watermark(self, refresh_time: datetime) -> None:
        await self._delta_tracker.set(Watermark(collected_time=refresh_time, tray_id=""))

    async def _normalize_signals(self, rows: list[TraySignal]) -> list[TraySignal]:
        if not rows:
            return []
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._executor, _normalize_signals_sync, rows)


def _normalize_signals_sync(rows: list[TraySignal]) -> list[TraySignal]:
    out: list[TraySignal] = []
    for row in rows:
        tray_id = str(row.tray_id).strip()
        if not tray_id:
            continue
        out.append(
            TraySignal(
                source=row.source,
                tray_id=TrayId(tray_id),
                collected_time=row.collected_time,
                payload={str(k): v for k, v in row.payload.items()},
            )
        )
    out.sort(key=lambda s: (s.collected_time, str(s.tray_id), s.source.value))
    return out


def _projection_to_payload(projection: Projection) -> dict[str, object]:
    return {
        "trays": [tray.to_dict() for tray in projection.trays],
        "assembly_trolleys": [trolley.to_dict() for trolley in projection.assembly_trolleys],
        "queue_trolleys": [trolley.to_dict() for trolley in projection.queue_trolleys],
        "precharge_trolleys": [trolley.to_dict() for trolley in projection.precharge_trolleys],
        "assembly_ungrouped": [tray.to_dict() for tray in projection.assembly_ungrouped],
    }


async def _maybe_start(obj: object) -> None:
    method = getattr(obj, "start", None)
    if callable(method):
        await method()


async def _maybe_close(obj: object) -> None:
    method = getattr(obj, "close", None)
    if callable(method):
        await method()
