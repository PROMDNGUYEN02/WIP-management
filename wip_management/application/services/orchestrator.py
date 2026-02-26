from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta

from wip_management.application.ports import (
    CcuRepoPort,
    ClockPort,
    DeltaTrackerPort,
    EventBusPort,
    FpcRepoPort,
    GroupingStateRepoPort,
)
from wip_management.application.state.state_store import SingleWriterStateStore
from wip_management.application.use_cases.ingest_signals import IngestResult, IngestSignalsUseCase
from wip_management.application.use_cases.manual_group import ManualGroupUseCase
from wip_management.application.use_cases.recompute_projection import Projection, RecomputeProjectionUseCase
from wip_management.config import settings
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
        delta_poll_idle_interval_seconds: float,
        backfill_cooldown_seconds: float,
        max_parallel_workers: int,
        snapshot_limit: int,
        max_trays_per_trolley: int,
        assembly_auto_trolley_count: int,
        auto_group_enabled: bool,
        grouping_sync_interval_seconds: float,
        grouping_state_repo: GroupingStateRepoPort | None = None,
    ) -> None:
        self._ccu_repo = ccu_repo
        self._fpc_repo = fpc_repo
        self._delta_tracker = delta_tracker
        self._event_bus = event_bus
        self._store = state_store
        self._clock = clock

        self._initial_load_start_hour = initial_load_start_hour
        self._delta_poll_interval_seconds = delta_poll_interval_seconds
        self._delta_poll_idle_interval_seconds = delta_poll_idle_interval_seconds
        self._backfill_cooldown = timedelta(seconds=backfill_cooldown_seconds)
        self._snapshot_limit = snapshot_limit
        self._max_trays_per_trolley = max_trays_per_trolley
        self._assembly_auto_trolley_count = assembly_auto_trolley_count
        self._auto_group_enabled = auto_group_enabled
        self._grouping_sync_interval = timedelta(seconds=grouping_sync_interval_seconds)
        self._grouping_state_repo = grouping_state_repo

        self._ingest = IngestSignalsUseCase(store=state_store)
        self._recompute_projection = RecomputeProjectionUseCase()
        self._manual_group = ManualGroupUseCase()
        self._executor = ThreadPoolExecutor(max_workers=max_parallel_workers)

        self._running = False
        self._refresh_task: asyncio.Task | None = None
        self._stop_event = asyncio.Event()
        self._last_backfill_attempt: dict[str, datetime] = {}
        self._refresh_iteration = 0
        self._last_grouping_sync_at: datetime | None = None
        self._bootstrap_event = asyncio.Event()
        self._bootstrap_in_progress = False
        self._bootstrap_error: Exception | None = None
        self._tray_cells_cache_ttl = timedelta(seconds=max(0.0, settings.tray_detail_cache_ttl_seconds))
        self._tray_cells_cache_max_entries = max(0, int(settings.tray_detail_cache_max_entries))
        self._tray_cells_cache: dict[str, tuple[datetime, list[dict[str, str | None]]]] = {}
        self._tray_cells_inflight: dict[str, asyncio.Task[list[dict[str, str | None]]]] = {}
        self._active_detail_queries = 0

    async def start(self) -> None:
        if self._running:
            log.warning("Orchestrator already running")
            return
        log.info("Orchestrator start begin")
        await _maybe_start(self._ccu_repo)
        await _maybe_start(self._fpc_repo)
        await self._store.start()
        await self._sync_manual_assignments_from_repo(force=True)
        self._running = True
        self._stop_event.clear()
        self._bootstrap_in_progress = True
        try:
            await self._initial_load()
            self._bootstrap_error = None
        except Exception as exc:  # noqa: BLE001
            self._bootstrap_error = exc
            raise
        finally:
            self._bootstrap_in_progress = False
            self._bootstrap_event.set()
        self._refresh_task = asyncio.create_task(self._refresh_loop(), name="wip-delta-refresh-loop")
        log.info("Orchestrator start completed")

    async def stop(self) -> None:
        if not self._running:
            log.debug("Orchestrator stop skipped because it is not running")
            return
        log.info("Orchestrator stop begin")
        self._running = False
        self._stop_event.set()
        if self._refresh_task is not None:
            if asyncio.current_task() is self._refresh_task:
                self._refresh_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await self._refresh_task
            else:
                await self._refresh_task
            self._refresh_task = None
        for task in list(self._tray_cells_inflight.values()):
            task.cancel()
        if self._tray_cells_inflight:
            await asyncio.gather(*self._tray_cells_inflight.values(), return_exceptions=True)
        self._tray_cells_inflight.clear()
        self._tray_cells_cache.clear()
        self._executor.shutdown(wait=True, cancel_futures=False)
        await self._store.stop()
        await _maybe_close(self._ccu_repo)
        await _maybe_close(self._fpc_repo)
        log.info("Orchestrator stop completed")

    async def tray_snapshot(self, limit: int | None = None) -> list[dict]:
        rows = await self._store.snapshot_desc(limit=limit)
        return [tray.to_dict() for tray in rows]

    async def tray_count(self) -> int:
        return await self._store.size()

    async def current_watermark(self) -> Watermark | None:
        return await self._delta_tracker.get()

    async def projection_snapshot(self) -> dict[str, object]:
        log.debug("Projection snapshot requested")
        projection = await self._compute_projection()
        return _projection_to_payload(projection)

    async def group_tray_manual(self, tray_id: str, trolley_id: str, column: Column) -> None:
        await self.group_trays_manual([tray_id], trolley_id=trolley_id, column=column)

    async def group_trays_manual(self, tray_ids: list[str], trolley_id: str, column: Column) -> dict[str, object]:
        target_column = Column.QUEUE
        if column is not Column.QUEUE:
            log.info(
                "Manual group requested non-queue column=%s; forcing Queue per business rule",
                column.value,
            )
        normalized_tray_ids = [tray_id.strip() for tray_id in tray_ids if tray_id and tray_id.strip()]
        if not normalized_tray_ids:
            raise ValueError("tray_ids must not be empty")
        normalized_trolley_id = trolley_id.strip()
        if not normalized_trolley_id:
            raise ValueError("trolley_id must not be empty")
        log.info(
            "Manual group apply tray_count=%s trolley_id=%s column=%s",
            len(normalized_tray_ids),
            normalized_trolley_id,
            target_column.value,
        )
        await self._sync_manual_assignments_from_repo(force=True)
        applied = self._manual_group.group_many_to_trolley(
            [TrayId(tray_id) for tray_id in normalized_tray_ids],
            trolley_id=normalized_trolley_id,
            column=target_column,
        )
        await self._persist_manual_assignments_to_repo()
        projection = await self._compute_projection()
        await self._publish_trolley_updated(projection)
        return {"grouped": applied, "trolley_id": normalized_trolley_id, "column": target_column.value}

    async def ungroup_tray_manual(self, tray_id: str) -> None:
        log.info("Manual ungroup apply tray_id=%s", tray_id)
        await self._sync_manual_assignments_from_repo(force=True)
        self._manual_group.ungroup(TrayId(tray_id))
        await self._persist_manual_assignments_to_repo()
        projection = await self._compute_projection()
        await self._publish_trolley_updated(projection)

    async def clear_trolley_manual(self, trolley_id: str) -> dict[str, object]:
        normalized_trolley_id = trolley_id.strip()
        if not normalized_trolley_id:
            raise ValueError("trolley_id must not be empty")
        log.info("Manual clear trolley apply trolley_id=%s", normalized_trolley_id)
        await self._sync_manual_assignments_from_repo(force=True)
        removed = self._manual_group.clear_trolley(normalized_trolley_id)
        if removed == 0:
            raise ValueError(
                f"No manual tray assignments found for trolley_id={normalized_trolley_id}",
            )
        await self._persist_manual_assignments_to_repo()
        projection = await self._compute_projection()
        await self._publish_trolley_updated(projection)
        return {"trolley_id": normalized_trolley_id, "removed": removed}

    async def delete_trolley_manual(self, trolley_id: str) -> dict[str, object]:
        normalized_trolley_id = trolley_id.strip()
        if not normalized_trolley_id:
            raise ValueError("trolley_id must not be empty")
        log.info("Manual delete trolley apply trolley_id=%s", normalized_trolley_id)
        removed = await self.clear_trolley_manual(normalized_trolley_id)
        removed["deleted"] = True
        return removed

    async def rename_trolley_manual(self, old_trolley_id: str, new_trolley_id: str) -> dict[str, object]:
        old_id = old_trolley_id.strip()
        new_id = new_trolley_id.strip()
        if not old_id or not new_id:
            raise ValueError("old_trolley_id and new_trolley_id must not be empty")
        log.info("Manual rename trolley apply old=%s new=%s", old_id, new_id)
        await self._sync_manual_assignments_from_repo(force=True)
        changed = self._manual_group.rename_trolley(old_id, new_id)
        if changed == 0:
            raise ValueError(f"No manual tray assignments found for trolley_id={old_id}")
        await self._persist_manual_assignments_to_repo()
        projection = await self._compute_projection()
        await self._publish_trolley_updated(projection)
        return {"old_trolley_id": old_id, "new_trolley_id": new_id, "updated": changed}

    async def set_auto_group_enabled(self, enabled: bool) -> dict[str, object]:
        enabled = bool(enabled)
        if self._auto_group_enabled == enabled:
            return {"changed": False, "auto_group_enabled": enabled}
        self._auto_group_enabled = enabled
        projection = await self._compute_projection()
        await self._publish_trolley_updated(projection)
        log.info("Auto group mode changed enabled=%s", enabled)
        return {"changed": True, "auto_group_enabled": enabled}

    async def update_grouping_settings(self, *, max_trays_per_trolley: int | None = None) -> dict[str, object]:
        changed = False
        payload: dict[str, object] = {}
        if max_trays_per_trolley is not None:
            normalized = int(max_trays_per_trolley)
            if normalized <= 0:
                raise ValueError("max_trays_per_trolley must be > 0")
            if self._max_trays_per_trolley != normalized:
                self._max_trays_per_trolley = normalized
                changed = True
            payload["max_trays_per_trolley"] = self._max_trays_per_trolley
        if changed:
            projection = await self._compute_projection()
            await self._publish_trolley_updated(projection)
        payload["changed"] = changed
        return payload

    async def fetch_tray_cells(self, tray_id: str) -> list[dict[str, str | None]]:
        tray_key = tray_id.strip()
        if not tray_key:
            return []
        await self._ensure_bootstrap_ready_for_detail()
        now = self._clock.now()
        cached = self._tray_cells_cache.get(tray_key)
        if cached is not None:
            cached_at, cached_rows = cached
            if self._tray_cells_cache_ttl.total_seconds() <= 0 or (now - cached_at) <= self._tray_cells_cache_ttl:
                log.debug("Tray detail cache hit tray_id=%s rows=%s", tray_key, len(cached_rows))
                return [dict(item) for item in cached_rows]

        inflight = self._tray_cells_inflight.get(tray_key)
        if inflight is not None:
            log.debug("Tray detail join inflight request tray_id=%s", tray_key)
            rows = await inflight
            return [dict(item) for item in rows]

        task = asyncio.create_task(self._fetch_tray_cells_uncached(tray_key), name=f"tray-detail-{tray_key}")
        self._tray_cells_inflight[tray_key] = task
        self._active_detail_queries += 1
        try:
            rows = await task
        finally:
            self._active_detail_queries = max(0, self._active_detail_queries - 1)
            if self._tray_cells_inflight.get(tray_key) is task:
                self._tray_cells_inflight.pop(tray_key, None)

        now = self._clock.now()
        copied = [dict(item) for item in rows]
        self._tray_cells_cache[tray_key] = (now, copied)
        self._trim_tray_detail_cache()
        return [dict(item) for item in copied]

    async def fetch_cell_owner(self, cell_id: str) -> dict[str, str | None] | None:
        cell_key = cell_id.strip()
        if not cell_key:
            return None
        await self._ensure_bootstrap_ready_for_detail()
        now = self._clock.now()
        day_start = now.replace(
            hour=self._initial_load_start_hour,
            minute=0,
            second=0,
            microsecond=0,
        )
        started_at = time.perf_counter()
        owner = await self._ccu_repo.fetch_cell_owner(
            cell_id=cell_key,
            start_time=day_start,
            end_time=now,
        )
        log.info(
            "Cell owner fetched cell_id=%s found=%s elapsed_ms=%s",
            cell_key,
            bool(owner),
            int((time.perf_counter() - started_at) * 1000),
        )
        return owner

    async def _fetch_tray_cells_uncached(self, tray_key: str) -> list[dict[str, str | None]]:
        now = self._clock.now()
        day_start = now.replace(
            hour=self._initial_load_start_hour,
            minute=0,
            second=0,
            microsecond=0,
        )
        started_at = time.perf_counter()
        rows = await self._ccu_repo.fetch_tray_cells(
            tray_id=tray_key,
            start_time=day_start,
            end_time=now,
        )
        log.info(
            "Tray detail fetched tray_id=%s rows=%s elapsed_ms=%s",
            tray_key,
            len(rows),
            int((time.perf_counter() - started_at) * 1000),
        )
        return rows

    async def _ensure_bootstrap_ready_for_detail(self) -> None:
        if self._bootstrap_in_progress:
            raise RuntimeError("Initial load is in progress. Please retry tray detail in a few seconds.")
        if not self._bootstrap_event.is_set():
            await self._bootstrap_event.wait()
        if self._bootstrap_error is not None:
            raise RuntimeError("Initial load failed; tray detail is unavailable.") from self._bootstrap_error

    def _trim_tray_detail_cache(self) -> None:
        if self._tray_cells_cache_max_entries <= 0:
            self._tray_cells_cache.clear()
            return
        overflow = len(self._tray_cells_cache) - self._tray_cells_cache_max_entries
        if overflow <= 0:
            return
        oldest = sorted(self._tray_cells_cache.items(), key=lambda item: item[1][0])[:overflow]
        for tray_id, _ in oldest:
            self._tray_cells_cache.pop(tray_id, None)

    async def manual_refresh(self, *, full_scan: bool) -> dict[str, object]:
        log.info("Manual refresh begin full_scan=%s", full_scan)
        if self._bootstrap_in_progress:
            log.info("Manual refresh skipped because bootstrap is in progress")
            return {"mode": "bootstrap_in_progress", "changed": False}
        if not self._bootstrap_event.is_set():
            log.info("Manual refresh waiting for bootstrap completion")
            await self._bootstrap_event.wait()
        if self._bootstrap_error is not None:
            raise RuntimeError("Bootstrap failed; cannot run manual refresh") from self._bootstrap_error
        watermark = await self._delta_tracker.get()
        now = self._clock.now()
        if watermark is None:
            await self._initial_load()
            log.info("Manual refresh bootstrap completed")
            return {"mode": "bootstrap", "changed": True}

        if full_scan:
            result = await self._run_full_window_scan(now)
            backfill_changed = await self._maybe_backfill_ccu(result.store_result.missing_ccu_tray_ids)
            changed = bool(result.store_result.changed or backfill_changed)
            projection = await self._compute_projection()
            await self._publish_trolley_updated(projection)
            await self._set_refresh_watermark(now)
            log.info(
                "Manual refresh full completed changed=%s merged=%s backfill_changed=%s",
                changed,
                len(result.merged),
                backfill_changed,
            )
            return {"mode": "manual_full", "changed": changed, "merged": len(result.merged)}

        if not await self._has_new_data_since(watermark.collected_time, now):
            synced_changed = await self._sync_manual_assignments_from_repo()
            if synced_changed:
                projection = await self._compute_projection()
                await self._publish_trolley_updated(projection)
            await self._set_refresh_watermark(now)
            log.info("Manual refresh quick no-new-data")
            return {"mode": "manual_quick", "changed": synced_changed, "merged": 0}

        result = await self._run_delta_scan(watermark=watermark, end_time=now)
        backfill_changed = await self._maybe_backfill_ccu(result.store_result.missing_ccu_tray_ids)
        changed = bool(result.store_result.changed or backfill_changed)
        if changed:
            for tray in result.store_result.changed:
                await self._event_bus.publish(TrayUpdated(tray=tray))
            projection = await self._compute_projection()
            await self._publish_trolley_updated(projection)
        else:
            synced_changed = await self._sync_manual_assignments_from_repo()
            if synced_changed:
                projection = await self._compute_projection()
                await self._publish_trolley_updated(projection)
                changed = True
        await self._set_refresh_watermark(now)
        log.info("Manual refresh quick completed changed=%s merged=%s", changed, len(result.merged))
        return {"mode": "manual_quick", "changed": changed, "merged": len(result.merged)}

    async def _initial_load(self) -> None:
        started_at = time.perf_counter()
        now = self._clock.now()
        start = now.replace(
            hour=self._initial_load_start_hour,
            minute=0,
            second=0,
            microsecond=0,
        )
        log.info(
            "Initial load window start=%s end=%s order=ASC",
            start.isoformat(),
            now.isoformat(),
        )
        previous = await self._delta_tracker.get()
        ccu_task = asyncio.create_task(
            self._ccu_repo.fetch_initial(start_time=start, end_time=now),
            name="initial-ccu-fetch",
        )
        fpc_task = asyncio.create_task(
            self._fpc_repo.fetch_initial(start_time=start, end_time=now),
            name="initial-fpc-fetch",
        )

        norm_ccu: list[TraySignal] = []
        norm_fpc: list[TraySignal] = []
        partial_source = "none"
        try:
            done, _ = await asyncio.wait({ccu_task, fpc_task}, return_when=asyncio.FIRST_COMPLETED)
            first_task = next(iter(done))
            if first_task is ccu_task:
                partial_source = "ccu"
                norm_ccu = await self._normalize_signals(ccu_task.result())
                partial_result = await self._ingest.execute(norm_ccu, [], previous_watermark=previous)
            else:
                partial_source = "fpc"
                norm_fpc = await self._normalize_signals(fpc_task.result())
                partial_result = await self._ingest.execute([], norm_fpc, previous_watermark=previous)
                await self._maybe_backfill_ccu(partial_result.store_result.missing_ccu_tray_ids)
            partial_projection = await self._compute_projection()
            await self._publish_snapshot_ready(partial_projection)
            await self._save_projection_to_repo(partial_projection)
            log.info(
                "Initial load partial published source=%s merged=%s changed=%s trays=%s",
                partial_source,
                len(partial_result.merged),
                len(partial_result.store_result.changed),
                len(partial_projection.trays),
            )
        except Exception:
            for task in (ccu_task, fpc_task):
                if not task.done():
                    task.cancel()
            raise

        if not ccu_task.done():
            norm_ccu = await self._normalize_signals(await ccu_task)
        elif not norm_ccu:
            norm_ccu = await self._normalize_signals(ccu_task.result())
        if not fpc_task.done():
            norm_fpc = await self._normalize_signals(await fpc_task)
        elif not norm_fpc:
            norm_fpc = await self._normalize_signals(fpc_task.result())

        ccu_tray_ids = {str(item.tray_id) for item in norm_ccu}
        fpc_tray_ids = {str(item.tray_id) for item in norm_fpc}
        raw_overlap = ccu_tray_ids & fpc_tray_ids
        if ccu_tray_ids and fpc_tray_ids and not raw_overlap:
            ccu_sample = sorted(ccu_tray_ids)[:3]
            fpc_sample = sorted(fpc_tray_ids)[:3]
            log.warning(
                "Initial load no tray overlap between CCU/FPC ccu_sample=%s fpc_sample=%s",
                ccu_sample,
                fpc_sample,
            )
        result = await self._ingest.execute(norm_ccu, norm_fpc, previous_watermark=previous)
        backfill_changed = await self._maybe_backfill_ccu(result.store_result.missing_ccu_tray_ids)
        await self._set_refresh_watermark(now)
        projection = await self._compute_projection()
        await self._publish_snapshot_ready(projection)
        await self._save_projection_to_repo(projection)
        log.info(
            "Initial load completed elapsed_ms=%s merged=%s trays=%s ccu=%s fpc=%s overlap=%s backfill_changed=%s watermark=%s partial_source=%s",
            int((time.perf_counter() - started_at) * 1000),
            len(result.merged),
            len(projection.trays),
            len(norm_ccu),
            len(norm_fpc),
            len(raw_overlap),
            backfill_changed,
            now.isoformat(),
            partial_source,
        )

    async def _refresh_loop(self) -> None:
        log.info("Refresh loop started interval_seconds=%s", self._delta_poll_interval_seconds)
        while self._running:
            self._refresh_iteration += 1
            had_change = False
            try:
                had_change = await self._refresh_once()
            except Exception:  # noqa: BLE001
                log.exception("Refresh loop failed")
            sleep_seconds = (
                self._delta_poll_interval_seconds
                if had_change
                else max(self._delta_poll_interval_seconds, self._delta_poll_idle_interval_seconds)
            )
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=sleep_seconds)
            except asyncio.TimeoutError:
                pass
        log.info("Refresh loop stopped")

    async def _refresh_once(self) -> bool:
        iteration = self._refresh_iteration
        started_at = time.perf_counter()
        if self._active_detail_queries > 0:
            log.debug(
                "Refresh #%s skipped while tray detail query in progress count=%s",
                iteration,
                self._active_detail_queries,
            )
            return False
        watermark = await self._delta_tracker.get()
        if watermark is None:
            log.debug("Refresh #%s skipped because watermark is None", iteration)
            return False
        now = self._clock.now()
        if not await self._has_new_data_since(watermark.collected_time, now):
            synced_changed = await self._sync_manual_assignments_from_repo()
            if synced_changed:
                projection = await self._compute_projection()
                await self._publish_trolley_updated(projection)
            await self._set_refresh_watermark(now)
            log.debug("Refresh #%s no new data", iteration)
            return synced_changed

        ingest_result = await self._run_delta_scan(watermark=watermark, end_time=now)
        backfill_changed = await self._maybe_backfill_ccu(ingest_result.store_result.missing_ccu_tray_ids)
        synced_changed = await self._sync_manual_assignments_from_repo()

        if ingest_result.store_result.changed:
            for tray in ingest_result.store_result.changed:
                await self._event_bus.publish(TrayUpdated(tray=tray))

        if ingest_result.store_result.changed or backfill_changed or synced_changed:
            projection = await self._compute_projection()
            await self._publish_trolley_updated(projection)

        await self._set_refresh_watermark(now)
        changed = bool(ingest_result.store_result.changed or backfill_changed or synced_changed)
        log.info(
            "Refresh #%s completed elapsed_ms=%s merged=%s changed=%s backfill_changed=%s",
            iteration,
            int((time.perf_counter() - started_at) * 1000),
            len(ingest_result.merged),
            len(ingest_result.store_result.changed),
            backfill_changed,
        )
        return changed

    async def _run_full_window_scan(self, end_time: datetime) -> IngestResult:
        started_at = time.perf_counter()
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
        ccu_tray_ids = {str(item.tray_id) for item in norm_ccu}
        fpc_tray_ids = {str(item.tray_id) for item in norm_fpc}
        raw_overlap = ccu_tray_ids & fpc_tray_ids
        if ccu_tray_ids and fpc_tray_ids and not raw_overlap:
            ccu_sample = sorted(ccu_tray_ids)[:3]
            fpc_sample = sorted(fpc_tray_ids)[:3]
            log.warning(
                "No tray overlap between CCU/FPC after normalization ccu_sample=%s fpc_sample=%s",
                ccu_sample,
                fpc_sample,
            )
        previous = await self._delta_tracker.get()
        result = await self._ingest.execute(norm_ccu, norm_fpc, previous_watermark=previous)
        log.info(
            "Full window scan done elapsed_ms=%s ccu=%s fpc=%s merged=%s ccu_unique=%s fpc_unique=%s raw_overlap=%s",
            int((time.perf_counter() - started_at) * 1000),
            len(norm_ccu),
            len(norm_fpc),
            len(result.merged),
            len(ccu_tray_ids),
            len(fpc_tray_ids),
            len(raw_overlap),
        )
        return result

    async def _run_delta_scan(self, *, watermark: Watermark, end_time: datetime) -> IngestResult:
        started_at = time.perf_counter()
        ccu_rows, fpc_rows = await asyncio.gather(
            self._ccu_repo.fetch_delta(watermark=watermark, end_time=end_time),
            self._fpc_repo.fetch_delta(watermark=watermark, end_time=end_time),
        )
        norm_ccu, norm_fpc = await asyncio.gather(
            self._normalize_signals(ccu_rows),
            self._normalize_signals(fpc_rows),
        )
        ccu_tray_ids = {str(item.tray_id) for item in norm_ccu}
        fpc_tray_ids = {str(item.tray_id) for item in norm_fpc}
        raw_overlap = ccu_tray_ids & fpc_tray_ids
        if ccu_tray_ids and fpc_tray_ids and not raw_overlap:
            ccu_sample = sorted(ccu_tray_ids)[:3]
            fpc_sample = sorted(fpc_tray_ids)[:3]
            log.debug(
                "Delta scan no overlap ccu_sample=%s fpc_sample=%s",
                ccu_sample,
                fpc_sample,
            )
        result = await self._ingest.execute(norm_ccu, norm_fpc, previous_watermark=watermark)
        log.debug(
            "Delta scan done elapsed_ms=%s ccu=%s fpc=%s merged=%s ccu_unique=%s fpc_unique=%s raw_overlap=%s",
            int((time.perf_counter() - started_at) * 1000),
            len(norm_ccu),
            len(norm_fpc),
            len(result.merged),
            len(ccu_tray_ids),
            len(fpc_tray_ids),
            len(raw_overlap),
        )
        return result

    async def _has_new_data_since(self, since: datetime, end_time: datetime) -> bool:
        ccu_latest, fpc_latest = await asyncio.gather(
            self._ccu_repo.peek_latest_signal_time(end_time=end_time),
            self._fpc_repo.peek_latest_signal_time(end_time=end_time),
        )
        log.debug(
            "Has-new-data check since=%s ccu_latest=%s fpc_latest=%s",
            since.isoformat(),
            ccu_latest.isoformat() if ccu_latest else None,
            fpc_latest.isoformat() if fpc_latest else None,
        )
        if ccu_latest is not None and ccu_latest > since:
            return True
        if fpc_latest is not None and fpc_latest > since:
            return True
        return False

    async def _maybe_backfill_ccu(self, tray_ids: set[str]) -> bool:
        if not tray_ids:
            log.debug("CCU backfill not needed")
            return False

        now = self._clock.now()
        eligible: list[str] = []
        for tray_id in tray_ids:
            last = self._last_backfill_attempt.get(tray_id)
            if last is None or (now - last) >= self._backfill_cooldown:
                eligible.append(tray_id)
                self._last_backfill_attempt[tray_id] = now
        if not eligible:
            log.debug("CCU backfill cooled down for all pending tray_ids=%s", len(tray_ids))
            return False

        log.info("CCU backfill fetch tray_ids=%s", len(eligible))
        ccu_rows = await self._ccu_repo.fetch_by_tray_ids(tray_ids=eligible, end_time=now)
        if not ccu_rows:
            log.info("CCU backfill returned no rows")
            return False
        norm_ccu = await self._normalize_signals(ccu_rows)
        current_wm = await self._delta_tracker.get()
        ingest_result = await self._ingest.execute(norm_ccu, [], previous_watermark=current_wm)
        if not ingest_result.store_result.changed:
            log.info("CCU backfill ingest produced no changes")
            return False
        for tray in ingest_result.store_result.changed:
            await self._event_bus.publish(TrayUpdated(tray=tray))
        log.info("CCU backfill changed trays=%s", len(ingest_result.store_result.changed))
        return True

    async def _compute_projection(self) -> Projection:
        started_at = time.perf_counter()
        # Always compute projection from full in-memory state to avoid truncating ungroup list.
        snapshot = await self._store.snapshot_desc(limit=None)
        projection = self._recompute_projection.execute(
            snapshot,
            manual_assignments=self._manual_group.projection_assignments(),
            max_per_trolley=self._max_trays_per_trolley,
            assembly_auto_trolley_count=self._assembly_auto_trolley_count,
            auto_group_enabled=self._auto_group_enabled,
        )
        log.debug(
            "Projection computed elapsed_ms=%s trays=%s assembly=%s queue=%s precharge=%s ungrouped=%s",
            int((time.perf_counter() - started_at) * 1000),
            len(projection.trays),
            len(projection.assembly_trolleys),
            len(projection.queue_trolleys),
            len(projection.precharge_trolleys),
            len(projection.assembly_ungrouped),
        )
        return projection

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
        await self._save_projection_to_repo(projection)
        log.debug(
            "Published trolley update assembly=%s queue=%s precharge=%s ungrouped=%s",
            len(projection.assembly_trolleys),
            len(projection.queue_trolleys),
            len(projection.precharge_trolleys),
            len(projection.assembly_ungrouped),
        )

    async def _publish_snapshot_ready(self, projection: Projection) -> None:
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

    async def _sync_manual_assignments_from_repo(self, *, force: bool = False) -> bool:
        if self._grouping_state_repo is None:
            return False
        now = self._clock.now()
        if not force and self._last_grouping_sync_at is not None:
            if (now - self._last_grouping_sync_at) < self._grouping_sync_interval:
                return False

        try:
            loaded = await self._grouping_state_repo.load_manual_assignments()
        except Exception:  # noqa: BLE001
            log.exception("Failed to load manual assignments from shared repo")
            self._last_grouping_sync_at = now
            return False
        parsed: dict[str, tuple[Column, str]] = {}
        for tray_id, (column_name, trolley_id) in loaded.items():
            try:
                parsed[tray_id] = (Column(column_name), trolley_id)
            except ValueError:
                log.warning(
                    "Skip invalid manual assignment tray_id=%s column=%s trolley_id=%s",
                    tray_id,
                    column_name,
                    trolley_id,
                )
        current = self._manual_group.projection_assignments()
        self._last_grouping_sync_at = now
        if parsed == current:
            return False
        self._manual_group.restore(parsed)
        log.info("Manual assignments synced from shared repo count=%s", len(parsed))
        return True

    async def _persist_manual_assignments_to_repo(self) -> None:
        if self._grouping_state_repo is None:
            return
        projection_assignments = self._manual_group.projection_assignments()
        payload: dict[str, tuple[str, str]] = {}
        for tray_id, (column, trolley_id) in projection_assignments.items():
            payload[tray_id] = (column.value, trolley_id)
        await self._grouping_state_repo.replace_manual_assignments(payload)
        self._last_grouping_sync_at = self._clock.now()
        log.debug("Manual assignments persisted to shared repo count=%s", len(payload))

    async def _save_projection_to_repo(self, projection: Projection) -> None:
        if self._grouping_state_repo is None:
            return
        payload = _projection_grouping_payload(projection)
        try:
            await self._grouping_state_repo.save_projection(payload)
        except Exception:  # noqa: BLE001
            log.exception("Failed to save projection to shared repo")

    async def _set_refresh_watermark(self, refresh_time: datetime) -> None:
        await self._delta_tracker.set(Watermark(collected_time=refresh_time, tray_id=""))
        log.debug("Watermark set to refresh_time=%s", refresh_time.isoformat())

    async def _normalize_signals(self, rows: list[TraySignal]) -> list[TraySignal]:
        if not rows:
            return []
        started_at = time.perf_counter()
        loop = asyncio.get_running_loop()
        normalized = await loop.run_in_executor(self._executor, _normalize_signals_sync, rows)
        log.debug(
            "Normalize signals done elapsed_ms=%s input=%s output=%s",
            int((time.perf_counter() - started_at) * 1000),
            len(rows),
            len(normalized),
        )
        return normalized


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


def _projection_grouping_payload(projection: Projection) -> dict[str, object]:
    return {
        "assembly_trolleys": [trolley.to_dict() for trolley in projection.assembly_trolleys],
        "queue_trolleys": [trolley.to_dict() for trolley in projection.queue_trolleys],
        "precharge_trolleys": [trolley.to_dict() for trolley in projection.precharge_trolleys],
        "assembly_ungrouped_tray_ids": [str(tray.tray_id) for tray in projection.assembly_ungrouped],
    }


async def _maybe_start(obj: object) -> None:
    method = getattr(obj, "start", None)
    if callable(method):
        await method()


async def _maybe_close(obj: object) -> None:
    method = getattr(obj, "close", None)
    if callable(method):
        await method()
