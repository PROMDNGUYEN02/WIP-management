from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta

from wip_management.application.ports import (
    CcuRepoPort,
    ClockPort,
    DashboardRepoPort,
    DeltaTrackerPort,
    EventBusPort,
    FpcRepoPort,
    GroupingStateRepoPort,
)
from wip_management.application.state.state_store import SingleWriterStateStore, StoreApplyResult
from wip_management.application.use_cases.ingest_signals import IngestResult, IngestSignalsUseCase
from wip_management.application.use_cases.manual_group import ManualGroupUseCase
from wip_management.application.use_cases.recompute_projection import Projection, RecomputeProjectionUseCase
from wip_management.config import settings
from wip_management.domain.events import SnapshotReady, TrolleyUpdated, TrayUpdated
from wip_management.domain.models.tray import SignalSource, Tray, TrayId, TraySignal, Watermark
from wip_management.domain.models.trolley import Column, TrolleyMode

log = logging.getLogger(__name__)


@dataclass(slots=True, frozen=True)
class _FpcInitialStreamResult:
    norm_signals: list[TraySignal]
    raw_rows: int
    chunks: int
    partial_published: bool
    stream_failed: bool = False
    pending_missing_ccu_tray_ids: set[str] = field(default_factory=set)


@dataclass(slots=True, frozen=True)
class _HasNewDataResult:
    has_new_data: bool
    was_fresh: bool
    ccu_latest: datetime | None = None
    fpc_latest: datetime | None = None


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
        initial_load_lookback_hours: float,
        ui_data_window_days: int,
        delta_poll_interval_seconds: float,
        delta_poll_idle_interval_seconds: float,
        backfill_cooldown_seconds: float,
        max_parallel_workers: int,
        snapshot_limit: int,
        max_trays_per_trolley: int,
        total_trolley_count: int,
        assembly_auto_trolley_count: int,
        auto_group_enabled: bool,
        grouping_sync_interval_seconds: float,
        refresh_peek_enabled: bool,
        ccu_backfill_allow_targeted_lookup: bool,
        grouping_state_repo: GroupingStateRepoPort | None = None,
        dashboard_repo: DashboardRepoPort | None = None,
    ) -> None:
        self._ccu_repo = ccu_repo
        self._fpc_repo = fpc_repo
        self._delta_tracker = delta_tracker
        self._event_bus = event_bus
        self._store = state_store
        self._clock = clock
        self._dashboard_repo = dashboard_repo

        self._initial_load_start_hour = initial_load_start_hour
        self._initial_load_lookback_hours = max(0.0, float(initial_load_lookback_hours))
        self._ui_data_window_days = _normalize_data_window_days(ui_data_window_days)
        self._delta_poll_interval_seconds = delta_poll_interval_seconds
        self._delta_poll_idle_interval_seconds = delta_poll_idle_interval_seconds
        self._backfill_cooldown = timedelta(seconds=backfill_cooldown_seconds)
        self._backfill_retry_delay = timedelta(
            seconds=max(5.0, float(settings.ccu_backfill_retry_seconds)),
        )
        self._snapshot_limit = snapshot_limit
        self._max_trays_per_trolley = max_trays_per_trolley
        self._total_trolley_count = max(1, int(total_trolley_count))
        self._assembly_auto_trolley_count = assembly_auto_trolley_count
        self._auto_group_enabled = auto_group_enabled
        self._grouping_sync_interval = timedelta(seconds=grouping_sync_interval_seconds)
        self._refresh_peek_enabled = bool(refresh_peek_enabled)
        self._ccu_backfill_allow_targeted_lookup = bool(ccu_backfill_allow_targeted_lookup)
        self._refresh_skip_while_backfill = bool(settings.refresh_skip_while_backfill)
        self._initial_fpc_publish_requires_ccu = bool(settings.initial_fpc_publish_requires_ccu)
        self._grouping_state_repo = grouping_state_repo

        self._ingest = IngestSignalsUseCase(store=state_store, dashboard_repo=dashboard_repo)
        self._recompute_projection = RecomputeProjectionUseCase()
        self._manual_group = ManualGroupUseCase()
        self._executor = ThreadPoolExecutor(max_workers=max_parallel_workers)

        self._running = False
        self._refresh_task: asyncio.Task | None = None
        self._stop_event = asyncio.Event()
        self._last_backfill_attempt: dict[str, datetime] = {}
        self._backfill_retry_not_before: dict[str, datetime] = {}
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
        self._initial_publish_lock = asyncio.Lock()
        self._peek_latest_cache_base_ttl = timedelta(seconds=max(0.0, settings.peek_latest_cache_ttl_seconds))
        self._peek_latest_cache_ttl = self._peek_latest_cache_base_ttl
        self._peek_latest_cache_ttl_max = timedelta(
            seconds=max(
                self._peek_latest_cache_base_ttl.total_seconds(),
                self._delta_poll_idle_interval_seconds * 4.0,
            )
        )
        self._peek_no_change_streak = 0
        self._peek_latest_cache: dict[str, tuple[datetime, datetime, datetime | None]] = {}
        self._pending_backfill_tray_ids: set[str] = set()
        self._last_backfill_ccu_latest_seen: datetime | None = None
        self._backfill_task: asyncio.Task[None] | None = None
        self._scheduled_backfill_latest_hint: datetime | None = None
        self._scheduled_backfill_allow_historical_scan = False
        self._scheduled_backfill_allow_targeted_lookup = False
        self._window_backfill_task: asyncio.Task[None] | None = None
        self._scheduled_window_backfill_start: datetime | None = None
        self._window_backfill_generation = 0
        self._forced_delta_next_at: datetime | None = None
        self._forced_delta_interval = timedelta(
            seconds=max(self._delta_poll_idle_interval_seconds, self._delta_poll_interval_seconds * 8.0),
        )
        self._projection_cache_key: tuple | None = None
        self._projection_cache_value: Projection | None = None
        self._loaded_window_start: datetime | None = None

    async def start(self) -> None:
        if self._running:
            log.warning("Orchestrator already running")
            return
        log.info("Orchestrator start begin")
        try:
            await _maybe_start(self._ccu_repo)
            await _maybe_start(self._fpc_repo)
            await _maybe_start(self._dashboard_repo)
        except Exception:  # noqa: BLE001
            log.exception("Orchestrator start failed while starting repositories")
            await _maybe_close(self._dashboard_repo)
            await _maybe_close(self._fpc_repo)
            await _maybe_close(self._ccu_repo)
            raise
        await self._store.start()
        await self._sync_manual_assignments_from_repo(force=True)
        self._peek_latest_cache.clear()
        self._peek_no_change_streak = 0
        self._peek_latest_cache_ttl = self._peek_latest_cache_base_ttl
        self._pending_backfill_tray_ids.clear()
        self._backfill_retry_not_before.clear()
        self._last_backfill_ccu_latest_seen = None
        self._scheduled_backfill_latest_hint = None
        self._scheduled_backfill_allow_historical_scan = False
        self._scheduled_backfill_allow_targeted_lookup = False
        self._window_backfill_task = None
        self._scheduled_window_backfill_start = None
        self._window_backfill_generation = 0
        self._forced_delta_next_at = None
        self._projection_cache_key = None
        self._projection_cache_value = None
        self._loaded_window_start = None
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
        if self._backfill_task is not None:
            self._backfill_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._backfill_task
            self._backfill_task = None
        if self._window_backfill_task is not None:
            self._window_backfill_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._window_backfill_task
            self._window_backfill_task = None
        for task in list(self._tray_cells_inflight.values()):
            task.cancel()
        if self._tray_cells_inflight:
            await asyncio.gather(*self._tray_cells_inflight.values(), return_exceptions=True)
        self._tray_cells_inflight.clear()
        self._tray_cells_cache.clear()
        self._peek_latest_cache.clear()
        self._pending_backfill_tray_ids.clear()
        self._backfill_retry_not_before.clear()
        self._last_backfill_ccu_latest_seen = None
        self._scheduled_backfill_latest_hint = None
        self._scheduled_backfill_allow_historical_scan = False
        self._scheduled_backfill_allow_targeted_lookup = False
        self._scheduled_window_backfill_start = None
        self._window_backfill_generation = 0
        self._forced_delta_next_at = None
        self._projection_cache_key = None
        self._projection_cache_value = None
        self._loaded_window_start = None
        self._executor.shutdown(wait=True, cancel_futures=False)
        await self._store.stop()
        await _maybe_close(self._dashboard_repo)
        await _maybe_close(self._ccu_repo)
        await _maybe_close(self._fpc_repo)
        log.info("Orchestrator stop completed")

    async def tray_snapshot(self, limit: int | None = None) -> list[dict]:
        rows = await self._store.snapshot_desc(limit=None)
        rows = self._filter_trays_for_data_window(rows, end_time=self._clock.now())
        if limit is not None:
            rows = rows[:limit]
        return [tray.to_dict() for tray in rows]

    async def tray_count(self) -> int:
        return await self._store.size()

    async def current_watermark(self) -> Watermark | None:
        return await self._delta_tracker.get()

    async def projection_snapshot(self) -> dict[str, object]:
        log.debug("Projection snapshot requested")
        projection = await self._compute_projection()
        return _projection_to_payload(projection)

    async def data_window_loading_status(self, *, data_window_days: int | None = None) -> dict[str, object]:
        now = self._clock.now()
        days = self._ui_data_window_days if data_window_days is None else _normalize_data_window_days(data_window_days)
        requested_start = self._window_start_for_days(end_time=now, days=days)
        loaded_start = self._loaded_window_start
        scheduled_start = self._scheduled_window_backfill_start
        task = self._window_backfill_task
        backfill_running = bool(task is not None and (not task.done()))
        ready = bool(loaded_start is not None and loaded_start <= requested_start)
        return {
            "data_window_days": days,
            "requested_start": requested_start.isoformat(),
            "loaded_start": loaded_start.isoformat() if loaded_start is not None else None,
            "scheduled_start": scheduled_start.isoformat() if scheduled_start is not None else None,
            "backfill_running": backfill_running,
            "ready": ready,
        }

    def _window_start(self, end_time: datetime) -> datetime:
        return self._window_start_for_days(end_time=end_time, days=self._ui_data_window_days)

    def _window_start_for_days(self, *, end_time: datetime, days: int) -> datetime:
        day_anchor = end_time.replace(
            hour=self._initial_load_start_hour,
            minute=0,
            second=0,
            microsecond=0,
        )
        day_offset = max(int(days) - 1, 0)
        return day_anchor - timedelta(days=day_offset)

    def _filter_trays_for_data_window(self, trays: list[Tray], *, end_time: datetime) -> list[Tray]:
        start = self._window_start(end_time)
        out: list[Tray] = []
        for tray in trays:
            latest = tray.latest_collected_time
            if latest is None:
                continue
            if latest >= start:
                out.append(tray)
        return out

    async def dashboard_sessions(self, *, include_closed: bool = False, only_wip: bool = False) -> dict[str, object]:
        if self._dashboard_repo is None:
            return {"count": 0, "sessions": []}
        sessions = await self._dashboard_repo.dashboard_sessions(
            include_closed=include_closed,
            only_wip=only_wip,
        )
        return {"count": len(sessions), "sessions": sessions}

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
        session_id: str | None = None
        if self._dashboard_repo is not None:
            session_id = await self._dashboard_repo.map_trays_to_open_session(
                trolley_id=normalized_trolley_id,
                tray_ids=normalized_tray_ids,
            )
        await self._persist_manual_assignments_to_repo()
        projection = await self._compute_projection()
        await self._publish_trolley_updated(projection)
        return {
            "grouped": applied,
            "trolley_id": normalized_trolley_id,
            "column": target_column.value,
            "session_id": session_id,
        }

    async def ungroup_tray_manual(self, tray_id: str) -> None:
        tray_key = tray_id.strip()
        log.info("Manual ungroup apply tray_id=%s", tray_key)
        await self._sync_manual_assignments_from_repo(force=True)
        self._manual_group.ungroup(TrayId(tray_key))
        if self._dashboard_repo is not None and tray_key:
            await self._dashboard_repo.remove_tray_mappings(tray_ids=[tray_key])
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
        closed_session_id: str | None = None
        if self._dashboard_repo is not None:
            closed_session_id = await self._dashboard_repo.close_open_session(trolley_id=normalized_trolley_id)
        if removed == 0 and closed_session_id is None:
            raise ValueError(
                f"No manual tray assignments found for trolley_id={normalized_trolley_id}",
            )
        await self._persist_manual_assignments_to_repo()
        projection = await self._compute_projection()
        await self._publish_trolley_updated(projection)
        return {
            "trolley_id": normalized_trolley_id,
            "removed": removed,
            "closed_session_id": closed_session_id,
        }

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
        promoted_from_auto = 0
        if changed == 0:
            projection = await self._compute_projection()
            promoted_from_auto = self._promote_auto_trolleys_to_manual(
                projection,
                source_trolley_id=old_id,
                target_trolley_id=new_id,
            )
            changed += promoted_from_auto
        session_updated = 0
        if self._dashboard_repo is not None:
            session_updated = await self._dashboard_repo.rename_open_session_trolley(
                old_trolley_id=old_id,
                new_trolley_id=new_id,
            )
        if changed == 0 and session_updated == 0:
            raise ValueError(f"No tray assignments found for trolley_id={old_id}")
        await self._persist_manual_assignments_to_repo()
        projection = await self._compute_projection()
        await self._publish_trolley_updated(projection)
        return {
            "old_trolley_id": old_id,
            "new_trolley_id": new_id,
            "updated": changed,
            "promoted_from_auto": promoted_from_auto,
            "session_updated": session_updated,
        }

    async def set_auto_group_enabled(self, enabled: bool) -> dict[str, object]:
        enabled = bool(enabled)
        if self._auto_group_enabled == enabled:
            return {"changed": False, "auto_group_enabled": enabled}
        promoted_auto = 0
        if not enabled:
            await self._sync_manual_assignments_from_repo(force=True)
            projection_before_disable = await self._compute_projection()
            promoted_auto = self._promote_auto_trolleys_to_manual(projection_before_disable)
            if promoted_auto > 0:
                await self._persist_manual_assignments_to_repo()
        self._auto_group_enabled = enabled
        projection = await self._compute_projection()
        await self._publish_trolley_updated(projection)
        log.info("Auto group mode changed enabled=%s promoted_auto=%s", enabled, promoted_auto)
        return {
            "changed": True,
            "auto_group_enabled": enabled,
            "promoted_auto": promoted_auto,
        }

    def _promote_auto_trolleys_to_manual(
        self,
        projection: Projection,
        *,
        source_trolley_id: str | None = None,
        target_trolley_id: str | None = None,
    ) -> int:
        source_id = str(source_trolley_id or "").strip()
        target_id = str(target_trolley_id or "").strip()
        promoted = 0
        for trolley in projection.trolleys:
            if trolley.mode is not TrolleyMode.AUTO:
                continue
            trolley_id = str(trolley.trolley_id).strip()
            if source_id and trolley_id != source_id:
                continue
            tray_ids = [str(tray_id).strip() for tray_id in trolley.tray_ids if str(tray_id).strip()]
            if not tray_ids:
                continue
            assign_trolley_id = target_id or trolley_id
            promoted += self._manual_group.group_many_to_trolley(
                [TrayId(tray_id) for tray_id in tray_ids],
                trolley_id=assign_trolley_id,
                column=trolley.column,
            )
        return promoted

    async def update_grouping_settings(
        self,
        *,
        max_trays_per_trolley: int | None = None,
        total_trolley_count: int | None = None,
        refresh_interval_seconds: float | None = None,
        data_window_days: int | None = None,
    ) -> dict[str, object]:
        changed = False
        payload: dict[str, object] = {}
        now = self._clock.now()
        if max_trays_per_trolley is not None:
            normalized = int(max_trays_per_trolley)
            if normalized <= 0:
                raise ValueError("max_trays_per_trolley must be > 0")
            if self._max_trays_per_trolley != normalized:
                self._max_trays_per_trolley = normalized
                changed = True
            payload["max_trays_per_trolley"] = self._max_trays_per_trolley
        if total_trolley_count is not None:
            normalized_total = int(total_trolley_count)
            if normalized_total <= 0:
                raise ValueError("total_trolley_count must be > 0")
            if self._total_trolley_count != normalized_total:
                self._total_trolley_count = normalized_total
                settings.total_trolley_count = normalized_total
                changed = True
            payload["total_trolley_count"] = self._total_trolley_count
        if refresh_interval_seconds is not None:
            refresh_interval = float(refresh_interval_seconds)
            if refresh_interval < 0.5 or refresh_interval > 60.0:
                raise ValueError("refresh_interval_seconds must be between 0.5 and 60.0")
            if self._delta_poll_interval_seconds != refresh_interval:
                self._delta_poll_interval_seconds = refresh_interval
                settings.delta_poll_interval_seconds = refresh_interval
                self._forced_delta_interval = timedelta(
                    seconds=max(self._delta_poll_idle_interval_seconds, self._delta_poll_interval_seconds * 8.0),
                )
                changed = True
            payload["refresh_interval_seconds"] = self._delta_poll_interval_seconds
        if data_window_days is not None:
            normalized_days = _normalize_data_window_days(data_window_days)
            if self._ui_data_window_days != normalized_days:
                previous_window_start = self._window_start(now)
                self._ui_data_window_days = normalized_days
                self._initial_load_lookback_hours = _lookback_hours_for_window_days(normalized_days)
                settings.ui_data_window_days = normalized_days
                settings.initial_load_lookback_hours = self._initial_load_lookback_hours
                settings.ccu_backfill_lookback_hours = self._initial_load_lookback_hours
                changed = True
                desired_window_start = self._window_start(now)
                if self._loaded_window_start is None:
                    # Do not run synchronous full window rescan on settings update because it blocks UI actions.
                    # Reuse current in-memory state immediately; background delta/backfill keeps filling missing data.
                    payload["window_reuse"] = True
                    payload["window_loaded_start"] = None
                    payload["window_scan_skipped"] = True
                elif desired_window_start < self._loaded_window_start:
                    payload["window_reuse"] = True
                    payload["window_backfill_scheduled"] = self._schedule_window_backfill(
                        requested_start=desired_window_start,
                        reason="settings-window-expand",
                    )
                    payload["window_requested_start"] = desired_window_start.isoformat()
                    payload["window_loaded_start"] = self._loaded_window_start.isoformat()
                else:
                    self._cancel_window_backfill(reason="settings-window-shrink-or-reuse")
                    payload["window_reuse"] = True
                    payload["window_previous_start"] = previous_window_start.isoformat()
                    payload["window_loaded_start"] = self._loaded_window_start.isoformat()
            payload["data_window_days"] = self._ui_data_window_days
            payload["initial_load_lookback_hours"] = self._initial_load_lookback_hours
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
        start = self._window_start(now)
        started_at = time.perf_counter()
        owner = await self._ccu_repo.fetch_cell_owner(
            cell_id=cell_key,
            start_time=start,
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
        start = self._window_start(now)
        started_at = time.perf_counter()
        rows = await self._ccu_repo.fetch_tray_cells(
            tray_id=tray_key,
            start_time=start,
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

    def _invalidate_tray_detail_cache_for_trays(self, trays: list[Tray]) -> None:
        if not trays:
            return
        invalidated = 0
        for tray in trays:
            tray_id = str(tray.tray_id)
            if tray.has_ccu:
                self._pending_backfill_tray_ids.discard(tray_id)
                self._last_backfill_attempt.pop(tray_id, None)
                self._backfill_retry_not_before.pop(tray_id, None)
            if tray_id in self._tray_cells_cache:
                self._tray_cells_cache.pop(tray_id, None)
                invalidated += 1
        if invalidated:
            log.debug("Tray detail cache invalidated trays=%s", invalidated)

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
            backfill_changed = await self._maybe_backfill_ccu(
                result.store_result.missing_ccu_tray_ids,
                allow_historical_scan=True,
                allow_targeted_lookup=self._ccu_backfill_allow_targeted_lookup,
            )
            changed = bool(result.store_result.changed or backfill_changed)
            self._invalidate_tray_detail_cache_for_trays(result.store_result.changed)
            projection = await self._compute_projection()
            await self._publish_trolley_updated(projection)
            await self._set_data_watermark(result.watermark)
            log.info(
                "Manual refresh full completed changed=%s merged=%s backfill_changed=%s",
                changed,
                len(result.merged),
                backfill_changed,
            )
            return {"mode": "manual_full", "changed": changed, "merged": len(result.merged)}

        has_new_data = await self._has_new_data_since(watermark.collected_time, now)
        if not has_new_data.has_new_data:
            synced_changed = await self._sync_manual_assignments_from_repo()
            backfill_scheduled = self._schedule_ccu_backfill(
                set(),
                ccu_latest_hint=has_new_data.ccu_latest,
                allow_historical_scan=True,
                allow_targeted_lookup=self._ccu_backfill_allow_targeted_lookup,
                reason="manual-refresh-no-new-data",
            )
            changed = bool(synced_changed)
            if synced_changed:
                projection = await self._compute_projection()
                await self._publish_trolley_updated(projection)
            if has_new_data.was_fresh:
                log.info("Manual refresh quick no-new-data")
            else:
                log.info("Manual refresh quick no-new-data (peek cached, watermark preserved)")
            return {
                "mode": "manual_quick",
                "changed": changed,
                "merged": 0,
                "backfill_scheduled": backfill_scheduled,
            }

        result = await self._run_delta_scan(watermark=watermark, end_time=now)
        backfill_scheduled = self._schedule_ccu_backfill(
            result.store_result.missing_ccu_tray_ids,
            ccu_latest_hint=has_new_data.ccu_latest,
            reason="manual-refresh-delta",
        )
        changed = bool(result.store_result.changed)
        self._invalidate_tray_detail_cache_for_trays(result.store_result.changed)
        if result.store_result.changed:
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
        await self._set_data_watermark(result.watermark)
        log.info(
            "Manual refresh quick completed changed=%s merged=%s backfill_scheduled=%s",
            changed,
            len(result.merged),
            backfill_scheduled,
        )
        return {
            "mode": "manual_quick",
            "changed": changed,
            "merged": len(result.merged),
            "backfill_scheduled": backfill_scheduled,
        }

    async def _initial_load(self) -> None:
        started_at = time.perf_counter()
        now = self._clock.now()
        start = self._window_start(now)
        log.info(
            "Initial load window start=%s end=%s order=ASC ui_days=%s lookback_hours=%.2f",
            start.isoformat(),
            now.isoformat(),
            self._ui_data_window_days,
            self._initial_load_lookback_hours,
        )
        warm_started = await self._warm_start_from_projection(window_start=start, now=now)
        if warm_started:
            log.info(
                "Initial load warm-start completed elapsed_ms=%s source=shared_projection",
                int((time.perf_counter() - started_at) * 1000),
            )
            return
        previous = await self._delta_tracker.get()
        if self._supports_fpc_initial_streaming():
            await self._initial_load_with_fpc_stream(
                started_at=started_at,
                start=start,
                end=now,
                previous=previous,
            )
            return

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
            enforce_ccu_first = first_task is fpc_task and self._initial_fpc_publish_requires_ccu
            if first_task is ccu_task or enforce_ccu_first:
                partial_source = "ccu"
                if ccu_task.done():
                    ccu_rows = ccu_task.result()
                else:
                    ccu_rows = await ccu_task
                norm_ccu = await self._normalize_signals(ccu_rows)
                partial_result = await self._ingest.execute(norm_ccu, [], previous_watermark=previous)
                if enforce_ccu_first:
                    log.info("Initial load partial FPC publish deferred until CCU partial is published")
                    norm_fpc = await self._normalize_signals(fpc_task.result())
            else:
                partial_source = "fpc"
                norm_fpc = await self._normalize_signals(fpc_task.result())
                partial_result = await self._ingest.execute([], norm_fpc, previous_watermark=previous)
                self._schedule_ccu_backfill(
                    partial_result.store_result.missing_ccu_tray_ids,
                    allow_historical_scan=True,
                    allow_targeted_lookup=self._ccu_backfill_allow_targeted_lookup,
                    reason="initial-load-partial-fpc",
                )
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
        backfill_scheduled = self._schedule_ccu_backfill(
            result.store_result.missing_ccu_tray_ids,
            allow_historical_scan=True,
            allow_targeted_lookup=self._ccu_backfill_allow_targeted_lookup,
            reason="initial-load-final",
        )
        if self._loaded_window_start is None or start < self._loaded_window_start:
            self._loaded_window_start = start
        self._invalidate_tray_detail_cache_for_trays(result.store_result.changed)
        await self._set_data_watermark(result.watermark, fallback_time=now)
        projection = await self._compute_projection()
        await self._publish_snapshot_ready(projection)
        await self._save_projection_to_repo(projection)
        log.info(
            "Initial load completed elapsed_ms=%s merged=%s trays=%s ccu=%s fpc=%s overlap=%s backfill_scheduled=%s watermark=%s partial_source=%s",
            int((time.perf_counter() - started_at) * 1000),
            len(result.merged),
            len(projection.trays),
            len(norm_ccu),
            len(norm_fpc),
            len(raw_overlap),
            backfill_scheduled,
            now.isoformat(),
            partial_source,
        )

    def _supports_fpc_initial_streaming(self) -> bool:
        stream_method = getattr(self._fpc_repo, "iter_initial_chunks", None)
        return callable(stream_method)

    async def _warm_start_from_projection(self, *, window_start: datetime, now: datetime) -> bool:
        if self._grouping_state_repo is None:
            return False
        try:
            cached_projection = await self._grouping_state_repo.load_projection()
        except Exception:  # noqa: BLE001
            log.exception("Warm start failed to load shared projection")
            return False
        tray_rows_raw = cached_projection.get("trays")
        if not isinstance(tray_rows_raw, list) or not tray_rows_raw:
            return False
        tray_rows = [row for row in tray_rows_raw if isinstance(row, dict)]
        if not tray_rows:
            return False

        ccu_signals, fpc_signals = _tray_rows_to_signals(tray_rows, fallback_time=now)
        if not ccu_signals and not fpc_signals:
            return False

        warm_watermark = _watermark_from_projection_payload(cached_projection, tray_rows)
        result = await self._ingest.execute(ccu_signals, fpc_signals, previous_watermark=warm_watermark)
        loaded_window_start = _parse_datetime_safe(cached_projection.get("loaded_window_start"))
        if loaded_window_start is None:
            loaded_window_start = window_start
        self._loaded_window_start = loaded_window_start
        self._invalidate_tray_detail_cache_for_trays(result.store_result.changed)
        await self._set_data_watermark(warm_watermark or result.watermark, fallback_time=now)
        self._schedule_ccu_backfill(
            result.store_result.missing_ccu_tray_ids,
            allow_historical_scan=True,
            allow_targeted_lookup=self._ccu_backfill_allow_targeted_lookup,
            reason="warm-start-shared-projection",
        )
        projection = await self._compute_projection()
        await self._publish_snapshot_ready(projection)
        await self._save_projection_to_repo(projection)
        log.info(
            "Warm start applied trays=%s ccu=%s fpc=%s watermark=%s loaded_window_start=%s",
            len(tray_rows),
            len(ccu_signals),
            len(fpc_signals),
            (warm_watermark.collected_time.isoformat() if warm_watermark else None),
            loaded_window_start.isoformat(),
        )
        return True

    async def _initial_load_with_fpc_stream(
        self,
        *,
        started_at: float,
        start: datetime,
        end: datetime,
        previous: Watermark | None,
    ) -> None:
        log.info("Initial load using FPC stream mode")
        fpc_publish_gate = asyncio.Event()
        if not self._initial_fpc_publish_requires_ccu:
            fpc_publish_gate.set()
        ccu_task = asyncio.create_task(
            self._load_initial_ccu_partial(
                start_time=start,
                end_time=end,
                previous=previous,
                publish_gate=fpc_publish_gate,
            ),
            name="initial-ccu-partial",
        )
        fpc_task = asyncio.create_task(
            self._load_initial_fpc_stream_partial(
                start_time=start,
                end_time=end,
                previous=previous,
                publish_gate=fpc_publish_gate,
            ),
            name="initial-fpc-stream",
        )
        ccu_out, fpc_out = await asyncio.gather(ccu_task, fpc_task, return_exceptions=True)
        ccu_failed = isinstance(ccu_out, Exception)
        fpc_failed = isinstance(fpc_out, Exception)
        if ccu_failed:
            fpc_publish_gate.set()
        if ccu_failed and fpc_failed:
            raise RuntimeError("Initial load failed for both CCU and FPC stream") from ccu_out

        if ccu_failed:
            log.exception("Initial load CCU partial failed, continue with available FPC stream", exc_info=ccu_out)
            norm_ccu = []
        else:
            norm_ccu = ccu_out

        if fpc_failed:
            log.exception("Initial load FPC stream failed, continue with available CCU", exc_info=fpc_out)
            fpc_result = _FpcInitialStreamResult(
                norm_signals=[],
                raw_rows=0,
                chunks=0,
                partial_published=False,
                stream_failed=True,
            )
        else:
            fpc_result = fpc_out

        deferred_backfill_scheduled = False
        if (not ccu_failed) and fpc_result.pending_missing_ccu_tray_ids:
            deferred_backfill_scheduled = self._schedule_ccu_backfill(
                fpc_result.pending_missing_ccu_tray_ids,
                allow_historical_scan=True,
                allow_targeted_lookup=self._ccu_backfill_allow_targeted_lookup,
                reason="initial-load-fpc-stream",
            )

        ccu_tray_ids = {str(item.tray_id) for item in norm_ccu}
        fpc_tray_ids = {str(item.tray_id) for item in fpc_result.norm_signals}
        raw_overlap = ccu_tray_ids & fpc_tray_ids
        if ccu_tray_ids and fpc_tray_ids and not raw_overlap:
            ccu_sample = sorted(ccu_tray_ids)[:3]
            fpc_sample = sorted(fpc_tray_ids)[:3]
            log.warning(
                "Initial load (stream) no tray overlap between CCU/FPC ccu_sample=%s fpc_sample=%s",
                ccu_sample,
                fpc_sample,
            )

        merged_count = len({item.dedup_key for item in [*norm_ccu, *fpc_result.norm_signals]})
        initial_watermark = _watermark_from_signals([*norm_ccu, *fpc_result.norm_signals], previous=previous)
        if (not ccu_failed) and (not fpc_failed):
            if self._loaded_window_start is None or start < self._loaded_window_start:
                self._loaded_window_start = start
        await self._set_data_watermark(initial_watermark, fallback_time=end)
        projection = await self._publish_initial_partial_snapshot(
            source="final",
            merged=merged_count,
            changed=0,
        )
        log.info(
            "Initial load completed elapsed_ms=%s merged=%s trays=%s ccu=%s fpc=%s overlap=%s fpc_chunks=%s fpc_raw_rows=%s fpc_stream_failed=%s deferred_missing_ccu=%s deferred_backfill_scheduled=%s watermark=%s partial_source=%s",
            int((time.perf_counter() - started_at) * 1000),
            merged_count,
            len(projection.trays),
            len(norm_ccu),
            len(fpc_result.norm_signals),
            len(raw_overlap),
            fpc_result.chunks,
            fpc_result.raw_rows,
            fpc_result.stream_failed,
            len(fpc_result.pending_missing_ccu_tray_ids),
            deferred_backfill_scheduled,
            end.isoformat(),
            "ccu+fpc-stream",
        )

    async def _load_initial_ccu_partial(
        self,
        *,
        start_time: datetime,
        end_time: datetime,
        previous: Watermark | None,
        publish_gate: asyncio.Event | None = None,
    ) -> list[TraySignal]:
        rows = await self._ccu_repo.fetch_initial(start_time=start_time, end_time=end_time)
        norm_ccu = await self._normalize_signals(rows)
        result = await self._ingest.execute(norm_ccu, [], previous_watermark=previous)
        backfill_scheduled = self._schedule_ccu_backfill(
            result.store_result.missing_ccu_tray_ids,
            allow_historical_scan=True,
            allow_targeted_lookup=self._ccu_backfill_allow_targeted_lookup,
            reason="initial-load-ccu-partial",
        )
        changed = len(result.store_result.changed) + (1 if backfill_scheduled else 0)
        await self._publish_initial_partial_snapshot(
            source="ccu",
            merged=len(result.merged),
            changed=changed,
        )
        if publish_gate is not None and (not publish_gate.is_set()):
            publish_gate.set()
        return norm_ccu

    async def _load_initial_fpc_stream_partial(
        self,
        *,
        start_time: datetime,
        end_time: datetime,
        previous: Watermark | None,
        publish_gate: asyncio.Event | None = None,
    ) -> _FpcInitialStreamResult:
        stream_method = getattr(self._fpc_repo, "iter_initial_chunks", None)
        if not callable(stream_method):
            try:
                rows = await self._fpc_repo.fetch_initial(start_time=start_time, end_time=end_time)
                norm_fpc = await self._normalize_signals(rows)
                result = await self._ingest.execute([], norm_fpc, previous_watermark=previous)
                self._schedule_ccu_backfill(
                    set(result.store_result.missing_ccu_tray_ids),
                    allow_historical_scan=True,
                    allow_targeted_lookup=self._ccu_backfill_allow_targeted_lookup,
                    reason="initial-load-fpc-fallback",
                )
                changed = len(result.store_result.changed)
                can_publish = (publish_gate is None) or publish_gate.is_set()
                if can_publish:
                    await self._publish_initial_partial_snapshot(
                        source="fpc",
                        merged=len(result.merged),
                        changed=changed,
                    )
                else:
                    log.info(
                        "Initial load FPC partial publish deferred until CCU is published changed=%s",
                        changed,
                    )
            except Exception:  # noqa: BLE001
                log.exception("Initial load fallback FPC fetch failed")
                return _FpcInitialStreamResult(
                    norm_signals=[],
                    raw_rows=0,
                    chunks=0,
                    partial_published=False,
                    stream_failed=True,
                )
            return _FpcInitialStreamResult(
                norm_signals=norm_fpc,
                raw_rows=len(rows),
                chunks=1 if rows else 0,
                partial_published=(publish_gate is None) or publish_gate.is_set(),
                stream_failed=False,
                pending_missing_ccu_tray_ids=set(result.store_result.missing_ccu_tray_ids),
            )

        all_norm_fpc: list[TraySignal] = []
        raw_rows_total = 0
        chunk_count = 0
        partial_published = False
        stream_failed = False
        last_publish_at = 0.0
        publish_interval_seconds = max(0.1, float(settings.initial_partial_publish_max_interval_seconds))
        min_changed_to_publish = max(1, int(settings.initial_partial_publish_min_changed))
        pending_changed = 0
        pending_missing_ccu_tray_ids: set[str] = set()
        scheduled_missing_ccu_tray_ids: set[str] = set()

        try:
            async for chunk in stream_method(start_time=start_time, end_time=end_time):
                if isinstance(chunk, tuple) and len(chunk) == 2:
                    chunk_signals = list(chunk[0] or [])
                    raw_rows = int(chunk[1] or 0)
                else:
                    chunk_signals = list(chunk or [])
                    raw_rows = len(chunk_signals)
                raw_rows_total += max(0, raw_rows)
                chunk_count += 1
                norm_chunk = await self._normalize_signals(chunk_signals)
                if norm_chunk:
                    all_norm_fpc.extend(norm_chunk)
                result = await self._ingest.execute([], norm_chunk, previous_watermark=previous)
                chunk_missing_ccu = set(result.store_result.missing_ccu_tray_ids)
                pending_missing_ccu_tray_ids.update(chunk_missing_ccu)
                new_missing_ccu = chunk_missing_ccu.difference(scheduled_missing_ccu_tray_ids)
                if new_missing_ccu:
                    self._schedule_ccu_backfill(
                        new_missing_ccu,
                        allow_historical_scan=True,
                        allow_targeted_lookup=self._ccu_backfill_allow_targeted_lookup,
                        reason=f"initial-load-fpc-stream#{chunk_count}",
                    )
                    scheduled_missing_ccu_tray_ids.update(new_missing_ccu)
                changed = len(result.store_result.changed)
                pending_changed += changed

                now_perf = time.perf_counter()
                should_publish_first = (not partial_published) and pending_changed > 0
                should_publish_count = pending_changed >= min_changed_to_publish
                should_publish_time = pending_changed > 0 and (now_perf - last_publish_at) >= publish_interval_seconds
                can_publish = (publish_gate is None) or publish_gate.is_set()
                should_publish = should_publish_first or should_publish_count or (partial_published and should_publish_time)
                if should_publish:
                    if can_publish:
                        await self._publish_initial_partial_snapshot(
                            source=f"fpc-stream#{chunk_count}",
                            merged=len(result.merged),
                            changed=pending_changed,
                        )
                        partial_published = True
                        pending_changed = 0
                        last_publish_at = now_perf
                    else:
                        log.debug(
                            "Initial load FPC stream publish deferred chunk=%s pending_changed=%s waiting_for_ccu=True",
                            chunk_count,
                            pending_changed,
                        )
                log.info(
                    "Initial load FPC stream chunk=%s raw_rows=%s signals=%s merged=%s changed=%s pending_changed=%s pending_missing_ccu=%s total_signals=%s",
                    chunk_count,
                    raw_rows,
                    len(norm_chunk),
                    len(result.merged),
                    changed,
                    pending_changed,
                    len(pending_missing_ccu_tray_ids),
                    len(all_norm_fpc),
                )
        except Exception:  # noqa: BLE001
            stream_failed = True
            log.exception("Initial load FPC stream interrupted, continue with partial data")

        if pending_changed > 0 and ((publish_gate is None) or publish_gate.is_set()):
            await self._publish_initial_partial_snapshot(
                source=f"fpc-stream#{chunk_count}-final",
                merged=0,
                changed=pending_changed,
            )
            partial_published = True

        return _FpcInitialStreamResult(
            norm_signals=all_norm_fpc,
            raw_rows=raw_rows_total,
            chunks=chunk_count,
            partial_published=partial_published,
            stream_failed=stream_failed,
            pending_missing_ccu_tray_ids=pending_missing_ccu_tray_ids,
        )

    async def _publish_initial_partial_snapshot(
        self,
        *,
        source: str,
        merged: int,
        changed: int,
    ) -> Projection:
        async with self._initial_publish_lock:
            projection = await self._compute_projection()
            await self._publish_snapshot_ready(projection)
            await self._save_projection_to_repo(projection)
        log.info(
            "Initial load partial published source=%s merged=%s changed=%s trays=%s",
            source,
            merged,
            changed,
            len(projection.trays),
        )
        return projection

    async def _refresh_loop(self) -> None:
        log.info("Refresh loop started interval_seconds=%s", self._delta_poll_interval_seconds)
        first_wait = max(0.1, self._delta_poll_interval_seconds)
        try:
            await asyncio.wait_for(self._stop_event.wait(), timeout=first_wait)
            log.info("Refresh loop stopped before first tick")
            return
        except asyncio.TimeoutError:
            pass

        while self._running:
            if self._stop_event.is_set():
                break
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
        if self._stop_event.is_set() or (not self._running):
            log.debug("Refresh #%s skipped because stop was requested", iteration)
            return False
        if self._active_detail_queries > 0:
            log.debug(
                "Refresh #%s skipped while tray detail query in progress count=%s",
                iteration,
                self._active_detail_queries,
            )
            return False
        if self._refresh_skip_while_backfill and self._backfill_task is not None and (not self._backfill_task.done()):
            log.debug(
                "Refresh #%s skipped while CCU backfill worker is running pending=%s",
                iteration,
                len(self._pending_backfill_tray_ids),
            )
            return False
        watermark = await self._delta_tracker.get()
        if watermark is None:
            log.debug("Refresh #%s skipped because watermark is None", iteration)
            return False
        now = self._clock.now()
        has_new_data = await self._has_new_data_since(watermark.collected_time, now)
        if not has_new_data.has_new_data:
            synced_changed = await self._sync_manual_assignments_from_repo()
            backfill_scheduled = self._schedule_ccu_backfill(
                set(),
                ccu_latest_hint=has_new_data.ccu_latest,
                allow_historical_scan=True,
                allow_targeted_lookup=self._ccu_backfill_allow_targeted_lookup,
                reason=f"refresh#{iteration}-no-new-data",
            )
            if synced_changed:
                projection = await self._compute_projection()
                await self._publish_trolley_updated(projection)
            if has_new_data.was_fresh:
                log.debug("Refresh #%s no new data backfill_scheduled=%s", iteration, backfill_scheduled)
            else:
                log.debug(
                    "Refresh #%s no new data (peek cached, watermark preserved) backfill_scheduled=%s",
                    iteration,
                    backfill_scheduled,
                )
            return bool(synced_changed)

        ingest_result = await self._run_delta_scan(watermark=watermark, end_time=now)
        backfill_scheduled = self._schedule_ccu_backfill(
            ingest_result.store_result.missing_ccu_tray_ids,
            ccu_latest_hint=has_new_data.ccu_latest,
            reason=f"refresh#{iteration}-delta",
        )
        synced_changed = await self._sync_manual_assignments_from_repo()
        self._invalidate_tray_detail_cache_for_trays(ingest_result.store_result.changed)

        if ingest_result.store_result.changed:
            for tray in ingest_result.store_result.changed:
                await self._event_bus.publish(TrayUpdated(tray=tray))

        if ingest_result.store_result.changed or synced_changed:
            projection = await self._compute_projection()
            await self._publish_trolley_updated(projection)

        await self._set_data_watermark(ingest_result.watermark)
        changed = bool(ingest_result.store_result.changed or synced_changed)
        log.info(
            "Refresh #%s completed elapsed_ms=%s merged=%s changed=%s backfill_scheduled=%s",
            iteration,
            int((time.perf_counter() - started_at) * 1000),
            len(ingest_result.merged),
            len(ingest_result.store_result.changed),
            backfill_scheduled,
        )
        return changed

    async def _run_full_window_scan(self, end_time: datetime) -> IngestResult:
        started_at = time.perf_counter()
        start = self._window_start(end_time)
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
        if self._loaded_window_start is None or start < self._loaded_window_start:
            self._loaded_window_start = start
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

    async def _backfill_window_range(
        self,
        *,
        start_time: datetime,
        end_time: datetime,
        expected_generation: int | None = None,
    ) -> IngestResult:
        if expected_generation is not None and expected_generation != self._window_backfill_generation:
            log.info(
                "Window backfill skip obsolete generation expected=%s current=%s start=%s end=%s",
                expected_generation,
                self._window_backfill_generation,
                start_time.isoformat(),
                end_time.isoformat(),
            )
            previous = await self._delta_tracker.get()
            return IngestResult(
                merged=[],
                store_result=StoreApplyResult(changed=[], missing_ccu_tray_ids=set()),
                watermark=previous,
            )
        if end_time <= start_time:
            previous = await self._delta_tracker.get()
            return IngestResult(
                merged=[],
                store_result=StoreApplyResult(changed=[], missing_ccu_tray_ids=set()),
                watermark=previous,
            )
        started_at = time.perf_counter()
        ccu_rows, fpc_rows = await asyncio.gather(
            self._ccu_repo.fetch_initial(start_time=start_time, end_time=end_time),
            self._fpc_repo.fetch_initial(start_time=start_time, end_time=end_time),
        )
        if expected_generation is not None and expected_generation != self._window_backfill_generation:
            log.info(
                "Window backfill discard fetched rows due obsolete generation expected=%s current=%s start=%s end=%s ccu_rows=%s fpc_rows=%s",
                expected_generation,
                self._window_backfill_generation,
                start_time.isoformat(),
                end_time.isoformat(),
                len(ccu_rows),
                len(fpc_rows),
            )
            previous = await self._delta_tracker.get()
            return IngestResult(
                merged=[],
                store_result=StoreApplyResult(changed=[], missing_ccu_tray_ids=set()),
                watermark=previous,
            )
        required_start = self._window_start(self._clock.now())
        if start_time < required_start:
            log.info(
                "Window backfill discard obsolete range start=%s required_start=%s end=%s",
                start_time.isoformat(),
                required_start.isoformat(),
                end_time.isoformat(),
            )
            previous = await self._delta_tracker.get()
            return IngestResult(
                merged=[],
                store_result=StoreApplyResult(changed=[], missing_ccu_tray_ids=set()),
                watermark=previous,
            )
        norm_ccu, norm_fpc = await asyncio.gather(
            self._normalize_signals(ccu_rows),
            self._normalize_signals(fpc_rows),
        )
        previous = await self._delta_tracker.get()
        result = await self._ingest.execute(norm_ccu, norm_fpc, previous_watermark=previous)
        if self._loaded_window_start is None or start_time < self._loaded_window_start:
            self._loaded_window_start = start_time
        log.info(
            "Window backfill done elapsed_ms=%s start=%s end=%s ccu=%s fpc=%s merged=%s changed=%s",
            int((time.perf_counter() - started_at) * 1000),
            start_time.isoformat(),
            end_time.isoformat(),
            len(norm_ccu),
            len(norm_fpc),
            len(result.merged),
            len(result.store_result.changed),
        )
        return result

    def _schedule_window_backfill(self, *, requested_start: datetime, reason: str) -> bool:
        if not self._running:
            return False
        if self._loaded_window_start is None:
            return False
        if requested_start >= self._loaded_window_start:
            return False
        if self._scheduled_window_backfill_start is None or requested_start < self._scheduled_window_backfill_start:
            self._scheduled_window_backfill_start = requested_start
        if self._window_backfill_task is not None and (not self._window_backfill_task.done()):
            log.info(
                "Window backfill schedule merged reason=%s requested_start=%s loaded_start=%s generation=%s",
                reason,
                requested_start.isoformat(),
                self._loaded_window_start.isoformat(),
                self._window_backfill_generation,
            )
            return True
        self._window_backfill_generation += 1
        generation = self._window_backfill_generation
        self._window_backfill_task = asyncio.create_task(
            self._run_scheduled_window_backfill(reason=reason, generation=generation),
            name="wip-window-backfill-worker",
        )
        log.info(
            "Window backfill scheduled reason=%s requested_start=%s loaded_start=%s generation=%s",
            reason,
            requested_start.isoformat(),
            self._loaded_window_start.isoformat(),
            generation,
        )
        return True

    def _cancel_window_backfill(self, *, reason: str) -> None:
        self._scheduled_window_backfill_start = None
        self._window_backfill_generation += 1
        if self._window_backfill_task is None or self._window_backfill_task.done():
            return
        self._window_backfill_task.cancel()
        log.info(
            "Window backfill cancel requested reason=%s generation=%s",
            reason,
            self._window_backfill_generation,
        )

    async def _run_scheduled_window_backfill(self, *, reason: str, generation: int) -> None:
        try:
            while self._running:
                if generation != self._window_backfill_generation:
                    log.info(
                        "Window backfill worker stop obsolete reason=%s worker_generation=%s current_generation=%s",
                        reason,
                        generation,
                        self._window_backfill_generation,
                    )
                    break
                target_start = self._scheduled_window_backfill_start
                loaded_start = self._loaded_window_start
                self._scheduled_window_backfill_start = None
                if target_start is None or loaded_start is None or target_start >= loaded_start:
                    break
                required_start = self._window_start(self._clock.now())
                if target_start < required_start:
                    log.info(
                        "Window backfill clamp target_start from %s to %s reason=%s generation=%s",
                        target_start.isoformat(),
                        required_start.isoformat(),
                        reason,
                        generation,
                    )
                    target_start = required_start
                if target_start >= loaded_start:
                    break
                result = await self._backfill_window_range(
                    start_time=target_start,
                    end_time=loaded_start,
                    expected_generation=generation,
                )
                if generation != self._window_backfill_generation:
                    log.info(
                        "Window backfill worker ignore result obsolete reason=%s worker_generation=%s current_generation=%s",
                        reason,
                        generation,
                        self._window_backfill_generation,
                    )
                    break
                self._invalidate_tray_detail_cache_for_trays(result.store_result.changed)
                await self._set_data_watermark(result.watermark, fallback_time=self._clock.now())
                if result.store_result.changed:
                    # Window backfill can touch many historical trays; publish projection only to avoid UI event flood.
                    projection = await self._compute_projection()
                    await self._publish_trolley_updated(projection)
                log.info(
                    "Window backfill worker step reason=%s target_start=%s loaded_start=%s merged=%s changed=%s generation=%s",
                    reason,
                    target_start.isoformat(),
                    loaded_start.isoformat(),
                    len(result.merged),
                    len(result.store_result.changed),
                    generation,
                )
            log.info("Window backfill worker completed reason=%s generation=%s", reason, generation)
        except asyncio.CancelledError:
            log.info("Window backfill worker cancelled reason=%s generation=%s", reason, generation)
            raise
        except Exception:  # noqa: BLE001
            log.exception("Window backfill worker failed reason=%s generation=%s", reason, generation)
        finally:
            if self._window_backfill_task is not None and self._window_backfill_task.done():
                self._window_backfill_task = None

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

    async def _has_new_data_since(self, since: datetime, end_time: datetime) -> _HasNewDataResult:
        if not self._refresh_peek_enabled:
            now = self._clock.now()
            next_scan_at = self._forced_delta_next_at
            if next_scan_at is not None and now < next_scan_at:
                log.debug(
                    "Peek latest disabled; skip forced delta scan until=%s",
                    next_scan_at.isoformat(),
                )
                return _HasNewDataResult(has_new_data=False, was_fresh=False)
            self._forced_delta_next_at = now + self._forced_delta_interval
            log.debug(
                "Peek latest disabled; forcing delta scan next_at=%s",
                self._forced_delta_next_at.isoformat(),
            )
            return _HasNewDataResult(has_new_data=True, was_fresh=False)

        (ccu_latest, ccu_fresh), (fpc_latest, fpc_fresh) = await asyncio.gather(
            self._peek_latest_signal_time_cached(
                cache_key="ccu",
                end_time=end_time,
                fetcher=self._ccu_repo.peek_latest_signal_time,
            ),
            self._peek_latest_signal_time_cached(
                cache_key="fpc",
                end_time=end_time,
                fetcher=self._fpc_repo.peek_latest_signal_time,
            ),
        )
        was_fresh = ccu_fresh and fpc_fresh
        has_new = False
        if ccu_latest is not None and ccu_latest > since:
            has_new = True
        if fpc_latest is not None and fpc_latest > since:
            has_new = True

        if has_new:
            self._peek_no_change_streak = 0
            self._peek_latest_cache_ttl = self._peek_latest_cache_base_ttl
        elif was_fresh and self._peek_latest_cache_base_ttl.total_seconds() > 0:
            self._peek_no_change_streak += 1
            grown_seconds = max(
                self._peek_latest_cache_base_ttl.total_seconds(),
                self._peek_latest_cache_ttl.total_seconds() * 1.6,
            )
            self._peek_latest_cache_ttl = timedelta(
                seconds=min(self._peek_latest_cache_ttl_max.total_seconds(), grown_seconds),
            )

        log.debug(
            "Has-new-data check since=%s ccu_latest=%s fpc_latest=%s fresh=%s has_new=%s peek_ttl_s=%.2f streak=%s",
            since.isoformat(),
            ccu_latest.isoformat() if ccu_latest else None,
            fpc_latest.isoformat() if fpc_latest else None,
            was_fresh,
            has_new,
            self._peek_latest_cache_ttl.total_seconds(),
            self._peek_no_change_streak,
        )
        return _HasNewDataResult(
            has_new_data=has_new,
            was_fresh=was_fresh,
            ccu_latest=ccu_latest,
            fpc_latest=fpc_latest,
        )

    async def _peek_latest_signal_time_cached(
        self,
        *,
        cache_key: str,
        end_time: datetime,
        fetcher: Callable[[datetime], Awaitable[datetime | None]],
    ) -> tuple[datetime | None, bool]:
        now = self._clock.now()
        if self._peek_latest_cache_ttl.total_seconds() > 0:
            cached = self._peek_latest_cache.get(cache_key)
            if cached is not None:
                cached_at, cached_end_time, cached_latest = cached
                age = now - cached_at
                if age <= self._peek_latest_cache_ttl and end_time >= cached_end_time:
                    log.debug(
                        "Peek latest cache hit source=%s age_ms=%s",
                        cache_key,
                        int(age.total_seconds() * 1000),
                    )
                    return cached_latest, False

        latest = await fetcher(end_time=end_time)
        fetched_at = self._clock.now()
        self._peek_latest_cache[cache_key] = (fetched_at, end_time, latest)
        return latest, True

    def _schedule_ccu_backfill(
        self,
        tray_ids: set[str] | None = None,
        *,
        ccu_latest_hint: datetime | None = None,
        allow_historical_scan: bool = False,
        allow_targeted_lookup: bool | None = None,
        reason: str,
    ) -> bool:
        incoming = {tray_id.strip() for tray_id in (tray_ids or set()) if tray_id and tray_id.strip()}
        if incoming:
            self._pending_backfill_tray_ids.update(incoming)
            for tray_id in incoming:
                self._backfill_retry_not_before.pop(tray_id, None)
        if not self._pending_backfill_tray_ids:
            return False

        if ccu_latest_hint is not None:
            if self._scheduled_backfill_latest_hint is None or ccu_latest_hint > self._scheduled_backfill_latest_hint:
                self._scheduled_backfill_latest_hint = ccu_latest_hint

        self._scheduled_backfill_allow_historical_scan = (
            self._scheduled_backfill_allow_historical_scan or bool(allow_historical_scan)
        )
        effective_targeted_lookup = (
            self._ccu_backfill_allow_targeted_lookup
            if allow_targeted_lookup is None
            else bool(allow_targeted_lookup)
        )
        self._scheduled_backfill_allow_targeted_lookup = (
            self._scheduled_backfill_allow_targeted_lookup or effective_targeted_lookup
        )

        if self._backfill_task is not None and not self._backfill_task.done():
            log.debug(
                "CCU backfill schedule merged into existing task reason=%s pending=%s",
                reason,
                len(self._pending_backfill_tray_ids),
            )
            return False

        self._backfill_task = asyncio.create_task(
            self._run_scheduled_backfill(reason=reason),
            name="wip-ccu-backfill",
        )
        log.info(
            "CCU backfill scheduled reason=%s pending=%s historical=%s targeted=%s",
            reason,
            len(self._pending_backfill_tray_ids),
            self._scheduled_backfill_allow_historical_scan,
            self._scheduled_backfill_allow_targeted_lookup,
        )
        return True

    async def _run_scheduled_backfill(self, *, reason: str) -> None:
        try:
            while self._running and self._pending_backfill_tray_ids:
                ccu_latest_hint = self._scheduled_backfill_latest_hint
                allow_historical_scan = self._scheduled_backfill_allow_historical_scan
                allow_targeted_lookup = self._scheduled_backfill_allow_targeted_lookup
                self._scheduled_backfill_latest_hint = None
                self._scheduled_backfill_allow_historical_scan = False
                self._scheduled_backfill_allow_targeted_lookup = False

                changed = await self._maybe_backfill_ccu(
                    set(),
                    ccu_latest_hint=ccu_latest_hint,
                    allow_historical_scan=allow_historical_scan,
                    allow_targeted_lookup=allow_targeted_lookup,
                )
                synced_changed = await self._sync_manual_assignments_from_repo()
                if changed or synced_changed:
                    projection = await self._compute_projection()
                    await self._publish_trolley_updated(projection)

                if (
                    self._scheduled_backfill_latest_hint is None
                    and not self._scheduled_backfill_allow_historical_scan
                    and not self._scheduled_backfill_allow_targeted_lookup
                ):
                    wait_seconds = self._next_backfill_wait_seconds()
                    if wait_seconds is None:
                        break
                    if wait_seconds > 0:
                        log.debug(
                            "CCU backfill worker idle-wait seconds=%.2f pending=%s",
                            wait_seconds,
                            len(self._pending_backfill_tray_ids),
                        )
                        try:
                            await asyncio.wait_for(self._stop_event.wait(), timeout=wait_seconds)
                        except asyncio.TimeoutError:
                            pass
                    continue
            log.info(
                "CCU backfill worker completed reason=%s pending=%s",
                reason,
                len(self._pending_backfill_tray_ids),
            )
        except asyncio.CancelledError:
            log.info("CCU backfill worker cancelled reason=%s", reason)
            raise
        except Exception:  # noqa: BLE001
            log.exception("CCU backfill worker failed reason=%s", reason)
        finally:
            self._backfill_task = None

    def _defer_backfill_retry(self, tray_ids: set[str], *, delay: timedelta, reason: str) -> None:
        if not tray_ids:
            return
        retry_at = self._clock.now() + delay
        for tray_id in tray_ids:
            previous = self._backfill_retry_not_before.get(tray_id)
            if previous is None or retry_at > previous:
                self._backfill_retry_not_before[tray_id] = retry_at
        log.info(
            "CCU backfill deferred tray_ids=%s retry_after_seconds=%s reason=%s",
            len(tray_ids),
            int(delay.total_seconds()),
            reason,
        )

    def _next_backfill_wait_seconds(self) -> float | None:
        if not self._pending_backfill_tray_ids:
            return None
        now = self._clock.now()
        wait_values: list[float] = []
        for tray_id in self._pending_backfill_tray_ids:
            retry_not_before = self._backfill_retry_not_before.get(tray_id)
            if retry_not_before is not None and now < retry_not_before:
                wait_values.append((retry_not_before - now).total_seconds())
                continue
            last_attempt = self._last_backfill_attempt.get(tray_id)
            if last_attempt is not None:
                ready_at = last_attempt + self._backfill_cooldown
                if now < ready_at:
                    wait_values.append((ready_at - now).total_seconds())
                    continue
            wait_values.append(0.0)
        if not wait_values:
            return None
        return max(0.0, min(wait_values))

    async def _maybe_backfill_ccu(
        self,
        tray_ids: set[str] | None = None,
        *,
        ccu_latest_hint: datetime | None = None,
        allow_historical_scan: bool = False,
        allow_targeted_lookup: bool | None = None,
    ) -> bool:
        incoming = {tray_id.strip() for tray_id in (tray_ids or set()) if tray_id and tray_id.strip()}
        if incoming:
            self._pending_backfill_tray_ids.update(incoming)
        if not self._pending_backfill_tray_ids:
            log.debug("CCU backfill not needed")
            return False
        effective_targeted_lookup = (
            self._ccu_backfill_allow_targeted_lookup
            if allow_targeted_lookup is None
            else bool(allow_targeted_lookup)
        )

        now = self._clock.now()
        force_retry_without_latest_advance = False
        stale_retry_interval = max(
            self._backfill_cooldown * 4,
            self._backfill_retry_delay * 2,
            timedelta(seconds=60),
        )
        for tray_id in self._pending_backfill_tray_ids:
            last_attempt = self._last_backfill_attempt.get(tray_id)
            retry_not_before = self._backfill_retry_not_before.get(tray_id)
            last_seen = last_attempt
            if retry_not_before is not None and (last_seen is None or retry_not_before > last_seen):
                last_seen = retry_not_before
            if last_seen is None or (now - last_seen) >= stale_retry_interval:
                force_retry_without_latest_advance = True
                break

        if ccu_latest_hint is not None and self._last_backfill_ccu_latest_seen is not None:
            if ccu_latest_hint <= self._last_backfill_ccu_latest_seen and not force_retry_without_latest_advance:
                log.debug(
                    "CCU backfill skipped because latest timestamp did not advance pending=%s latest=%s",
                    len(self._pending_backfill_tray_ids),
                    ccu_latest_hint.isoformat(),
                )
                return False

        eligible: list[str] = []
        for tray_id in sorted(self._pending_backfill_tray_ids):
            retry_not_before = self._backfill_retry_not_before.get(tray_id)
            if retry_not_before is not None and now < retry_not_before:
                continue
            last_attempt = self._last_backfill_attempt.get(tray_id)
            if last_attempt is None or (now - last_attempt) >= self._backfill_cooldown:
                eligible.append(tray_id)
                self._last_backfill_attempt[tray_id] = now
                self._backfill_retry_not_before.pop(tray_id, None)
        if not eligible:
            log.debug(
                "CCU backfill cooled down pending=%s",
                len(self._pending_backfill_tray_ids),
            )
            return False

        log.info(
            "CCU backfill fetch tray_ids=%s pending=%s historical=%s targeted=%s",
            len(eligible),
            len(self._pending_backfill_tray_ids),
            allow_historical_scan,
            effective_targeted_lookup,
        )
        ccu_rows = await self._ccu_repo.fetch_by_tray_ids(
            tray_ids=eligible,
            end_time=now,
            allow_historical_scan=allow_historical_scan,
            allow_targeted_lookup=effective_targeted_lookup,
        )
        if ccu_latest_hint is not None:
            self._last_backfill_ccu_latest_seen = ccu_latest_hint
        if not ccu_rows:
            self._defer_backfill_retry(
                set(eligible),
                delay=self._backfill_retry_delay,
                reason="no_rows",
            )
            log.info("CCU backfill returned no rows pending=%s", len(self._pending_backfill_tray_ids))
            return False
        norm_ccu = await self._normalize_signals(ccu_rows)
        if ccu_latest_hint is None and norm_ccu:
            self._last_backfill_ccu_latest_seen = max(item.collected_time for item in norm_ccu)
        resolved = {str(item.tray_id) for item in norm_ccu}
        unresolved = set(eligible).difference(resolved)
        if resolved:
            self._pending_backfill_tray_ids.difference_update(resolved)
            for tray_id in resolved:
                self._last_backfill_attempt.pop(tray_id, None)
                self._backfill_retry_not_before.pop(tray_id, None)
        if unresolved:
            self._defer_backfill_retry(
                unresolved,
                delay=self._backfill_retry_delay,
                reason="unresolved",
            )
        current_wm = await self._delta_tracker.get()
        ingest_result = await self._ingest.execute(norm_ccu, [], previous_watermark=current_wm)
        self._invalidate_tray_detail_cache_for_trays(ingest_result.store_result.changed)
        await self._set_data_watermark(ingest_result.watermark)
        if not ingest_result.store_result.changed:
            log.info(
                "CCU backfill ingest produced no changes resolved=%s pending=%s",
                len(resolved),
                len(self._pending_backfill_tray_ids),
            )
            return False
        for tray in ingest_result.store_result.changed:
            await self._event_bus.publish(TrayUpdated(tray=tray))
        log.info(
            "CCU backfill changed trays=%s resolved=%s pending=%s",
            len(ingest_result.store_result.changed),
            len(resolved),
            len(self._pending_backfill_tray_ids),
        )
        return True

    async def _compute_projection(self) -> Projection:
        started_at = time.perf_counter()
        now = self._clock.now()
        window_start = self._window_start(now)
        manual_assignments = self._manual_group.projection_assignments()
        manual_signature = tuple(
            sorted(
                (tray_id, column.value, trolley_id)
                for tray_id, (column, trolley_id) in manual_assignments.items()
            )
        )
        store_version = await self._store.version()
        cache_key = (
            store_version,
            manual_signature,
            self._max_trays_per_trolley,
            self._total_trolley_count,
            self._assembly_auto_trolley_count,
            self._auto_group_enabled,
            self._ui_data_window_days,
            window_start.isoformat(),
        )
        if self._projection_cache_key == cache_key and self._projection_cache_value is not None:
            log.debug(
                "Projection cache hit version=%s elapsed_ms=%s",
                store_version,
                int((time.perf_counter() - started_at) * 1000),
            )
            return self._projection_cache_value

        # Always compute projection from full in-memory state to avoid truncating ungroup list.
        snapshot = await self._store.snapshot_desc(limit=None)
        snapshot = self._filter_trays_for_data_window(snapshot, end_time=now)
        projection = self._recompute_projection.execute(
            snapshot,
            manual_assignments=manual_assignments,
            max_per_trolley=self._max_trays_per_trolley,
            total_trolley_count=self._total_trolley_count,
            assembly_auto_trolley_count=self._assembly_auto_trolley_count,
            auto_group_enabled=self._auto_group_enabled,
        )
        self._projection_cache_key = cache_key
        self._projection_cache_value = projection
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
        watermark = await self._delta_tracker.get()
        payload = _projection_grouping_payload(
            projection,
            watermark=watermark,
            loaded_window_start=self._loaded_window_start,
            data_window_days=self._ui_data_window_days,
        )
        try:
            await self._grouping_state_repo.save_projection(payload)
        except Exception:  # noqa: BLE001
            log.exception("Failed to save projection to shared repo")

    async def _set_data_watermark(
        self,
        next_watermark: Watermark | None,
        *,
        fallback_time: datetime | None = None,
    ) -> None:
        current = await self._delta_tracker.get()
        target = next_watermark or current
        if target is None and fallback_time is not None:
            target = Watermark(collected_time=fallback_time, tray_id="")
        if target is None:
            return
        if current is not None and current.collected_time > target.collected_time:
            target = current
        if (
            current is not None
            and current.collected_time == target.collected_time
            and current.tray_id == target.tray_id
        ):
            return
        await self._delta_tracker.set(target)
        log.debug(
            "Watermark set to data_time=%s tray_id=%s",
            target.collected_time.isoformat(),
            target.tray_id,
        )

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


def _watermark_from_signals(signals: list[TraySignal], *, previous: Watermark | None) -> Watermark | None:
    if not signals:
        return previous
    last = max(signals, key=lambda item: (item.collected_time, str(item.tray_id), item.source.value))
    return Watermark(collected_time=last.collected_time, tray_id=str(last.tray_id))


def _normalize_data_window_days(value: int) -> int:
    days = int(value)
    if days < 1:
        return 1
    if days > 3:
        return 3
    return days


def _lookback_hours_for_window_days(days: int) -> float:
    normalized_days = _normalize_data_window_days(days)
    return float((normalized_days + 1) * 24)


def _projection_to_payload(projection: Projection) -> dict[str, object]:
    return {
        "trays": [tray.to_dict() for tray in projection.trays],
        "assembly_trolleys": [trolley.to_dict() for trolley in projection.assembly_trolleys],
        "queue_trolleys": [trolley.to_dict() for trolley in projection.queue_trolleys],
        "precharge_trolleys": [trolley.to_dict() for trolley in projection.precharge_trolleys],
        "assembly_ungrouped": [tray.to_dict() for tray in projection.assembly_ungrouped],
    }


def _projection_grouping_payload(
    projection: Projection,
    *,
    watermark: Watermark | None,
    loaded_window_start: datetime | None,
    data_window_days: int,
) -> dict[str, object]:
    return {
        "trays": [tray.to_dict() for tray in projection.trays],
        "assembly_trolleys": [trolley.to_dict() for trolley in projection.assembly_trolleys],
        "queue_trolleys": [trolley.to_dict() for trolley in projection.queue_trolleys],
        "precharge_trolleys": [trolley.to_dict() for trolley in projection.precharge_trolleys],
        "assembly_ungrouped": [tray.to_dict() for tray in projection.assembly_ungrouped],
        "assembly_ungrouped_tray_ids": [str(tray.tray_id) for tray in projection.assembly_ungrouped],
        "data_window_days": int(data_window_days),
        "loaded_window_start": loaded_window_start.isoformat() if loaded_window_start is not None else None,
        "data_watermark": (
            {
                "collected_time": watermark.collected_time.isoformat(),
                "tray_id": watermark.tray_id,
            }
            if watermark is not None
            else None
        ),
    }


def _watermark_from_projection_payload(
    payload: dict[str, Any],
    tray_rows: list[dict[str, Any]],
) -> Watermark | None:
    raw = payload.get("data_watermark")
    if isinstance(raw, dict):
        collected_time = _parse_datetime_safe(raw.get("collected_time"))
        if collected_time is not None:
            tray_id = str(raw.get("tray_id") or "").strip()
            return Watermark(collected_time=collected_time, tray_id=tray_id)

    latest_time: datetime | None = None
    latest_tray_id = ""
    for row in tray_rows:
        collected = _parse_datetime_safe(row.get("latest_collected_time"))
        if collected is None:
            continue
        tray_id = str(row.get("tray_id") or "").strip()
        if latest_time is None or collected > latest_time:
            latest_time = collected
            latest_tray_id = tray_id
    if latest_time is None:
        return None
    return Watermark(collected_time=latest_time, tray_id=latest_tray_id)


def _tray_rows_to_signals(
    tray_rows: list[dict[str, Any]],
    *,
    fallback_time: datetime,
) -> tuple[list[TraySignal], list[TraySignal]]:
    ccu_signals: list[TraySignal] = []
    fpc_signals: list[TraySignal] = []
    for row in tray_rows:
        tray_id_text = str(row.get("tray_id") or "").strip()
        if not tray_id_text:
            continue
        tray_id = TrayId(tray_id_text)
        latest_time = _parse_datetime_safe(row.get("latest_collected_time"))

        ccu_payload_raw = row.get("ccu_payload")
        if isinstance(ccu_payload_raw, dict):
            ccu_payload = {str(k): v for k, v in ccu_payload_raw.items()}
            ccu_time = _signal_time_from_payload(
                payload=ccu_payload,
                preferred_keys=("end_time", "start_time"),
                fallback=latest_time or fallback_time,
            )
            ccu_signals.append(
                TraySignal(
                    source=SignalSource.CCU,
                    tray_id=tray_id,
                    collected_time=ccu_time,
                    payload=ccu_payload,
                )
            )

        fpc_payload_raw = row.get("fpc_payload")
        if isinstance(fpc_payload_raw, dict):
            fpc_payload = {str(k): v for k, v in fpc_payload_raw.items()}
            fpc_time = _signal_time_from_payload(
                payload=fpc_payload,
                preferred_keys=("precharge_end_time", "precharge_start_time"),
                fallback=latest_time or fallback_time,
            )
            fpc_signals.append(
                TraySignal(
                    source=SignalSource.FPC,
                    tray_id=tray_id,
                    collected_time=fpc_time,
                    payload=fpc_payload,
                )
            )
    return ccu_signals, fpc_signals


def _signal_time_from_payload(
    *,
    payload: dict[str, Any],
    preferred_keys: tuple[str, ...],
    fallback: datetime,
) -> datetime:
    for key in preferred_keys:
        parsed = _parse_datetime_safe(payload.get(key))
        if parsed is not None:
            return parsed
    return fallback


def _parse_datetime_safe(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    text = str(value).strip()
    if not text:
        return None
    normalized = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
        if parsed.tzinfo is not None:
            return parsed.astimezone().replace(tzinfo=None)
        return parsed
    except ValueError:
        return None


async def _maybe_start(obj: object) -> None:
    method = getattr(obj, "start", None)
    if callable(method):
        await method()


async def _maybe_close(obj: object) -> None:
    method = getattr(obj, "close", None)
    if callable(method):
        await method()
