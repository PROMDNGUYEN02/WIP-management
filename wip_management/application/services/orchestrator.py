# wip_management/application/services/orchestrator.py
from __future__ import annotations

import asyncio
import atexit
import contextlib
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, time as dt_time, timedelta
from typing import Any

from wip_management.application.ports import (
    CcuRepoPort,
    ClockPort,
    DashboardRepoPort,
    DeltaTrackerPort,
    EventBusPort,
    FpcRepoPort,
    GroupingStateRepoPort,
)
from wip_management.application.state.state_store import (
    SingleWriterStateStore,
    StoreApplyResult,
)
from wip_management.application.use_cases.ingest_signals import (
    IngestResult,
    IngestSignalsUseCase,
)
from wip_management.application.use_cases.manual_group import ManualGroupUseCase
from wip_management.application.use_cases.recompute_projection import (
    Projection,
    RecomputeProjectionUseCase,
)
from wip_management.config import settings
from wip_management.domain.events import SnapshotReady, TrolleyUpdated, TrayUpdated
from wip_management.domain.models.tray import (
    SignalSource,
    Tray,
    TrayId,
    TraySignal,
    Watermark,
)
from wip_management.domain.models.trolley import Column, TrolleyMode

log = logging.getLogger(__name__)
_MAX_UI_DATA_WINDOW_DAYS = 365

# ═══════════════════════════════════════════════════════════════════════════
# Thread Pool Management
# ═══════════════════════════════════════════════════════════════════════════
_NORMALIZE_EXECUTOR: ThreadPoolExecutor | None = None


def _get_normalize_executor(max_workers: int = 2) -> ThreadPoolExecutor:
    global _NORMALIZE_EXECUTOR
    if _NORMALIZE_EXECUTOR is None:
        _NORMALIZE_EXECUTOR = ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix="orch-normalize",
        )
        atexit.register(_shutdown_normalize_executor)
    return _NORMALIZE_EXECUTOR


def _shutdown_normalize_executor() -> None:
    global _NORMALIZE_EXECUTOR
    if _NORMALIZE_EXECUTOR is not None:
        _NORMALIZE_EXECUTOR.shutdown(wait=False, cancel_futures=True)
        _NORMALIZE_EXECUTOR = None


# ═══════════════════════════════════════════════════════════════════════════
# Data Classes
# ═══════════════════════════════════════════════════════════════════════════
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


@dataclass(slots=True)
class _CacheStats:
    hits: int = 0
    misses: int = 0
    evictions: int = 0

    def hit_rate(self) -> float:
        total = self.hits + self.misses
        return (self.hits / total * 100) if total > 0 else 0.0


# ═══════════════════════════════════════════════════════════════════════════
# Backfill Entry — Fix #5: give-up tracking per tray
# ═══════════════════════════════════════════════════════════════════════════
@dataclass(slots=True)
class _BackfillEntry:
    """Track per-tray CCU backfill state for give-up and exponential backoff."""
    tray_id: str
    first_seen: datetime
    attempt_count: int = 0
    next_allowed_at: datetime = field(default_factory=datetime.utcnow)

    def is_exhausted(self) -> bool:
        """True if we should permanently give up on this tray."""
        age_minutes = (
            datetime.utcnow() - self.first_seen
        ).total_seconds() / 60.0
        return (
            self.attempt_count >= settings.ccu_backfill_max_attempts
            or age_minutes >= settings.ccu_backfill_give_up_minutes
        )

    def record_attempt(self) -> None:
        """Record an attempt and schedule next allowed time with exponential backoff."""
        self.attempt_count += 1
        delay = settings.get_ccu_backfill_next_delay(self.attempt_count - 1)
        self.next_allowed_at = datetime.utcnow() + timedelta(seconds=delay)

    def is_ready(self) -> bool:
        """True if enough time has passed since last attempt."""
        return datetime.utcnow() >= self.next_allowed_at


# ═══════════════════════════════════════════════════════════════════════════
# Orchestrator Service
# ═══════════════════════════════════════════════════════════════════════════
class OrchestratorService:
    """
    Orchestrator with all priority fixes applied:
    - Fix #1: FPC IS_HIS/DEL_FLAG filter (in fpc_repo.py)
    - Fix #3: FPC initial load hard timeout — publishes CCU-only if FPC is slow
    - Fix #4: Stuck watermark guard — advances watermark when no new data
    - Fix #5: CCU backfill give-up after max_attempts / give_up_minutes
    - Fix #6: Exponential backoff between backfill retries
    - Today-only mode: split CCU/FPC windows, UI filter Option B
    """

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
            seconds=max(5.0, float(settings.ccu_backfill_retry_seconds))
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
        self._initial_fpc_publish_requires_ccu = bool(
            settings.initial_fpc_publish_requires_ccu
        )
        self._grouping_state_repo = grouping_state_repo

        # ── Today-only mode ────────────────────────────────────────────────
        self._ui_show_today_only = bool(settings.ui_show_today_only)
        self._ccu_today_lookback_buffer_hours = float(
            settings.ccu_today_lookback_buffer_hours
        )

        self._ingest = IngestSignalsUseCase(
            store=state_store, dashboard_repo=dashboard_repo
        )
        self._recompute_projection = RecomputeProjectionUseCase()
        self._manual_group = ManualGroupUseCase()
        self._executor = _get_normalize_executor(
            max_workers=max(1, max_parallel_workers)
        )

        # ── State ──────────────────────────────────────────────────────────
        self._running = False
        self._refresh_task: asyncio.Task | None = None
        self._stop_event = asyncio.Event()
        self._last_backfill_attempt: dict[str, datetime] = {}
        self._backfill_retry_not_before: dict[str, datetime] = {}
        self._backfill_no_result_streak: dict[str, int] = {}
        self._refresh_iteration = 0
        self._last_grouping_sync_at: datetime | None = None
        self._bootstrap_event = asyncio.Event()
        self._bootstrap_in_progress = False
        self._bootstrap_error: Exception | None = None

        # ── Fix #5/#6: per-tray backfill state with give-up ───────────────
        self._backfill_entries: dict[str, _BackfillEntry] = {}

        # ── Caches ────────────────────────────────────────────────────────
        self._tray_cells_cache_ttl = timedelta(
            seconds=max(0.0, settings.tray_detail_cache_ttl_seconds)
        )
        self._tray_cells_cache_max_entries = max(
            0, settings.tray_detail_cache_max_entries
        )
        self._tray_cells_cache: dict[
            str, tuple[datetime, list[dict[str, str | None]]]
        ] = {}
        self._tray_cells_cache_stats = _CacheStats()
        self._tray_cells_inflight: dict[
            str, asyncio.Task[list[dict[str, str | None]]]
        ] = {}
        self._active_detail_queries = 0
        self._initial_publish_lock = asyncio.Lock()

        self._peek_latest_cache_base_ttl = timedelta(
            seconds=max(0.0, settings.peek_latest_cache_ttl_seconds)
        )
        self._peek_latest_cache_ttl = self._peek_latest_cache_base_ttl
        self._peek_latest_cache_ttl_max = timedelta(
            seconds=max(
                self._peek_latest_cache_base_ttl.total_seconds(),
                self._delta_poll_idle_interval_seconds * 4.0,
            )
        )
        self._peek_no_change_streak = 0
        self._peek_latest_cache: dict[
            str, tuple[datetime, datetime, datetime | None]
        ] = {}

        # ── Backfill state ─────────────────────────────────────────────────
        self._pending_backfill_tray_ids: set[str] = set()
        self._last_backfill_ccu_latest_seen: datetime | None = None
        self._backfill_task: asyncio.Task[None] | None = None
        self._scheduled_backfill_latest_hint: datetime | None = None
        self._scheduled_backfill_allow_historical_scan = False
        self._scheduled_backfill_allow_targeted_lookup = False

        # ── Window backfill ────────────────────────────────────────────────
        self._window_backfill_task: asyncio.Task[None] | None = None
        self._scheduled_window_backfill_start: datetime | None = None
        self._window_backfill_generation = 0

        self._forced_delta_next_at: datetime | None = None
        self._forced_delta_interval = timedelta(
            seconds=max(
                self._delta_poll_idle_interval_seconds,
                self._delta_poll_interval_seconds * 8.0,
            )
        )

        self._projection_cache_key: tuple | None = None
        self._projection_cache_value: Projection | None = None
        self._loaded_window_start: datetime | None = None

        # ── Track split windows for delta reuse ────────────────────────────
        self._loaded_ccu_start: datetime | None = None
        self._loaded_fpc_start: datetime | None = None

        log.info(
            "Orchestrator initialized with settings: %s",
            settings.to_performance_summary(),
        )

    # ═══════════════════════════════════════════════════════════════════════
    # Today-Only Helpers
    # ═══════════════════════════════════════════════════════════════════════
    def _get_today_start(self) -> datetime:
        """Returns today 00:00:00 for UI display filtering."""
        return datetime.combine(self._clock.now().date(), dt_time.min)

    def _calculate_load_windows(self) -> tuple[datetime, datetime, datetime]:
        """
        Calculate (ccu_start, fpc_start, end_time) for initial load.

        Today mode (ui_show_today_only=True):
          FPC: [today 00:00, now]
          CCU: [today 00:00 - buffer, now]

        Rolling mode:
          Both: [now - lookback_hours, now]
        """
        now = self._clock.now()

        if self._ui_show_today_only:
            today_start = datetime.combine(now.date(), dt_time.min)
            fpc_start = today_start
            ccu_start = today_start - timedelta(
                hours=self._ccu_today_lookback_buffer_hours
            )
            return ccu_start, fpc_start, now

        # Rolling mode
        if self._initial_load_lookback_hours > 0:
            lookback = self._initial_load_lookback_hours
        else:
            lookback = float(self._ui_data_window_days * 24 + 2)

        start = now - timedelta(hours=lookback)
        return start, start, now

    def _is_tray_visible_today(self, tray: Tray) -> bool:
        """
        Option B: Show tray if CCU_time >= today OR FPC_time >= today.
        """
        today_start = self._get_today_start()

        # Check CCU time
        ccu_time = self._extract_signal_time(tray, SignalSource.CCU)
        if ccu_time is not None and ccu_time >= today_start:
            return True

        # Check FPC time
        fpc_time = self._extract_signal_time(tray, SignalSource.FPC)
        if fpc_time is not None and fpc_time >= today_start:
            return True

        return False

    def _extract_signal_time(
        self, tray: Tray, source: SignalSource
    ) -> datetime | None:
        """Extract collected_time for a specific signal source from a Tray."""
        # Try direct attributes first (common Tray model patterns)
        if source == SignalSource.CCU:
            for attr in ("ccu_collected_time", "ccu_time"):
                val = getattr(tray, attr, None)
                if isinstance(val, datetime):
                    return val
        elif source == SignalSource.FPC:
            for attr in ("fpc_collected_time", "fpc_time"):
                val = getattr(tray, attr, None)
                if isinstance(val, datetime):
                    return val

        # Try signal-based access
        if hasattr(tray, "ccu_signal") and source == SignalSource.CCU:
            sig = tray.ccu_signal
            if sig is not None and hasattr(sig, "collected_time"):
                return sig.collected_time
        if hasattr(tray, "fpc_signal") and source == SignalSource.FPC:
            sig = tray.fpc_signal
            if sig is not None and hasattr(sig, "collected_time"):
                return sig.collected_time

        # Try signals list
        if hasattr(tray, "signals"):
            for sig in tray.signals:
                if hasattr(sig, "source") and sig.source == source:
                    return getattr(sig, "collected_time", None)

        # Fallback: use latest_collected_time for either source
        # (only if the tray has that specific source flag)
        if source == SignalSource.CCU and getattr(tray, "has_ccu", False):
            return getattr(tray, "latest_collected_time", None)
        if source == SignalSource.FPC and getattr(tray, "has_fpc", False):
            return getattr(tray, "latest_collected_time", None)

        return None

    # ═══════════════════════════════════════════════════════════════════════
    # Lifecycle
    # ═══════════════════════════════════════════════════════════════════════
    async def start(self) -> None:
        if self._running:
            log.warning("Orchestrator already running")
            return
        log.info("Orchestrator start begin")
        try:
            await _maybe_start(self._ccu_repo)
            await _maybe_start(self._fpc_repo)
            await _maybe_start(self._dashboard_repo)
        except Exception:
            log.exception("Orchestrator start failed while starting repositories")
            await _maybe_close(self._dashboard_repo)
            await _maybe_close(self._fpc_repo)
            await _maybe_close(self._ccu_repo)
            raise

        await self._store.start()
        await self._sync_manual_assignments_from_repo(force=True)
        self._clear_all_caches()

        self._running = True
        self._stop_event.clear()
        self._bootstrap_in_progress = True

        try:
            await self._initial_load()
            self._bootstrap_error = None
        except Exception as exc:
            self._bootstrap_error = exc
            raise
        finally:
            self._bootstrap_in_progress = False
            self._bootstrap_event.set()

        self._refresh_task = asyncio.create_task(
            self._refresh_loop(), name="wip-delta-refresh-loop"
        )
        log.info("Orchestrator start completed")

    async def stop(self) -> None:
        if not self._running:
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
            await asyncio.gather(
                *self._tray_cells_inflight.values(), return_exceptions=True
            )

        self._log_cache_stats()
        self._clear_all_caches()

        await self._store.stop()
        await _maybe_close(self._dashboard_repo)
        await _maybe_close(self._ccu_repo)
        await _maybe_close(self._fpc_repo)
        log.info("Orchestrator stop completed")

    def _clear_all_caches(self) -> None:
        self._tray_cells_inflight.clear()
        self._tray_cells_cache.clear()
        self._peek_latest_cache.clear()
        self._peek_no_change_streak = 0
        self._peek_latest_cache_ttl = self._peek_latest_cache_base_ttl
        self._pending_backfill_tray_ids.clear()
        self._backfill_entries.clear()
        self._backfill_retry_not_before.clear()
        self._backfill_no_result_streak.clear()
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
        self._loaded_ccu_start = None
        self._loaded_fpc_start = None

    def _log_cache_stats(self) -> None:
        stats = self._tray_cells_cache_stats
        log.info(
            "Tray detail cache stats: hits=%s misses=%s evictions=%s hit_rate=%.1f%%",
            stats.hits,
            stats.misses,
            stats.evictions,
            stats.hit_rate(),
        )

    # ═══════════════════════════════════════════════════════════════════════
    # Public API
    # ═══════════════════════════════════════════════════════════════════════
    async def tray_snapshot(self, limit: int | None = None) -> list[dict]:
        rows = await self._store.snapshot_desc(limit=None)
        rows = self._filter_trays_for_display(rows, end_time=self._clock.now())
        if limit is not None:
            rows = rows[:limit]
        return [tray.to_dict() for tray in rows]

    async def tray_count(self) -> int:
        return await self._store.size()

    async def current_watermark(self) -> Watermark | None:
        return await self._delta_tracker.get()

    async def projection_snapshot(self) -> dict[str, object]:
        projection = await self._compute_projection()
        return _projection_to_payload(projection)

    async def data_window_loading_status(
        self, *, data_window_days: int | None = None
    ) -> dict[str, object]:
        now = self._clock.now()
        days = (
            self._ui_data_window_days
            if data_window_days is None
            else _normalize_data_window_days(data_window_days)
        )
        requested_start = self._window_start_for_days(end_time=now, days=days)
        loaded_start = self._loaded_window_start
        scheduled_start = self._scheduled_window_backfill_start
        task = self._window_backfill_task
        backfill_running = bool(task is not None and not task.done())
        ready = bool(
            loaded_start is not None and loaded_start <= requested_start
        )
        return {
            "data_window_days": days,
            "requested_start": requested_start.isoformat(),
            "loaded_start": loaded_start.isoformat() if loaded_start else None,
            "scheduled_start": (
                scheduled_start.isoformat() if scheduled_start else None
            ),
            "backfill_running": backfill_running,
            "ready": ready,
            "ui_show_today_only": self._ui_show_today_only,
        }

    async def cache_stats(self) -> dict[str, object]:
        stats = self._tray_cells_cache_stats
        exhausted = sum(
            1 for e in self._backfill_entries.values() if e.is_exhausted()
        )
        return {
            "tray_detail": {
                "size": len(self._tray_cells_cache),
                "max_size": self._tray_cells_cache_max_entries,
                "hits": stats.hits,
                "misses": stats.misses,
                "evictions": stats.evictions,
                "hit_rate_percent": round(stats.hit_rate(), 1),
                "inflight": len(self._tray_cells_inflight),
            },
            "peek_latest": {
                "size": len(self._peek_latest_cache),
                "no_change_streak": self._peek_no_change_streak,
                "current_ttl_seconds": (
                    self._peek_latest_cache_ttl.total_seconds()
                ),
            },
            "backfill": {
                "pending_count": len(self._pending_backfill_tray_ids),
                "task_running": (
                    self._backfill_task is not None
                    and not self._backfill_task.done()
                ),
                "tracking_count": len(self._backfill_entries),
                "exhausted_count": exhausted,
            },
        }

    # ═══════════════════════════════════════════════════════════════════════
    # Window Management — Today-Only Aware
    # ═══════════════════════════════════════════════════════════════════════
    def _window_start(self, end_time: datetime) -> datetime:
        """
        Get the earliest start time for data fetching.
        Today mode: uses the CCU start (includes buffer before midnight).
        Rolling mode: uses lookback hours.
        """
        if self._ui_show_today_only:
            today_start = datetime.combine(end_time.date(), dt_time.min)
            return today_start - timedelta(
                hours=self._ccu_today_lookback_buffer_hours
            )
        return self._window_start_for_days(
            end_time=end_time, days=self._ui_data_window_days
        )

    def _window_start_for_days(self, *, end_time: datetime, days: int) -> datetime:
        lookback_hours = _lookback_hours_for_window_days(days)
        return end_time - timedelta(hours=lookback_hours)

    def _ui_window_start(self, end_time: datetime) -> datetime:
        """
        Get the UI display window start.
        Today mode: today 00:00.
        Rolling mode: based on start_hour and data_window_days.
        """
        if self._ui_show_today_only:
            return datetime.combine(end_time.date(), dt_time.min)
        return self._ui_window_start_for_days(
            end_time=end_time, days=self._ui_data_window_days
        )

    def _ui_window_start_for_days(self, *, end_time: datetime, days: int) -> datetime:
        normalized_days = _normalize_data_window_days(days)
        start_hour = max(0, min(23, int(self._initial_load_start_hour)))
        day_anchor = end_time.replace(
            hour=start_hour, minute=0, second=0, microsecond=0
        )
        if end_time < day_anchor:
            day_anchor -= timedelta(days=1)
        return day_anchor - timedelta(days=normalized_days - 1)

    def _filter_trays_for_display(
        self, trays: list[Tray], *, end_time: datetime
    ) -> list[Tray]:
        """
        Filter trays for UI display.
        Today mode (Option B): show if CCU >= today OR FPC >= today.
        Rolling mode: show if latest_collected_time >= window_start.
        """
        if self._ui_show_today_only:
            return self._filter_trays_today_only(trays)
        return self._filter_trays_rolling_window(trays, end_time=end_time)

    def _filter_trays_today_only(self, trays: list[Tray]) -> list[Tray]:
        """
        Option B: Show tray if CCU_time >= today_00:00 OR FPC_time >= today_00:00.
        """
        out: list[Tray] = []
        hidden = 0
        for tray in trays:
            if self._is_tray_visible_today(tray):
                out.append(tray)
            else:
                hidden += 1

        if hidden > 0:
            log.debug(
                "Today filter applied total=%d shown=%d hidden=%d",
                len(trays), len(out), hidden,
            )
        return out

    def _filter_trays_rolling_window(
        self, trays: list[Tray], *, end_time: datetime
    ) -> list[Tray]:
        """Rolling window filter: show if latest_collected_time >= window_start."""
        start = self._ui_window_start(end_time)
        out: list[Tray] = []
        for tray in trays:
            latest = tray.latest_collected_time
            if latest is None:
                continue
            if latest >= start:
                out.append(tray)
        return out

    # Keep old name for backward compatibility within this file
    def _filter_trays_for_data_window(
        self, trays: list[Tray], *, end_time: datetime
    ) -> list[Tray]:
        return self._filter_trays_for_display(trays, end_time=end_time)

    # ═══════════════════════════════════════════════════════════════════════
    # Dashboard (unchanged)
    # ═══════════════════════════════════════════════════════════════════════
    async def dashboard_sessions(
        self, *, include_closed: bool = False, only_wip: bool = False
    ) -> dict[str, object]:
        if self._dashboard_repo is None:
            return {"count": 0, "sessions": []}
        sessions = await self._dashboard_repo.dashboard_sessions(
            include_closed=include_closed, only_wip=only_wip
        )
        return {"count": len(sessions), "sessions": sessions}

    # ═══════════════════════════════════════════════════════════════════════
    # Manual Grouping (unchanged — full pass-through)
    # ═══════════════════════════════════════════════════════════════════════
    async def group_tray_manual(
        self, tray_id: str, trolley_id: str, column: Column
    ) -> None:
        await self.group_trays_manual(
            [tray_id], trolley_id=trolley_id, column=column
        )

    async def group_trays_manual(
        self, tray_ids: list[str], trolley_id: str, column: Column
    ) -> dict[str, object]:
        target_column = Column.QUEUE
        normalized_tray_ids = [
            t.strip() for t in tray_ids if t and t.strip()
        ]
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
            [TrayId(t) for t in normalized_tray_ids],
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

    async def ungroup_trays_manual(
        self, tray_ids: list[str]
    ) -> dict[str, object]:
        normalized = list(dict.fromkeys(
            t.strip() for t in tray_ids if t and t.strip()
        ))
        if not normalized:
            raise ValueError("tray_ids must not be empty")
        log.info("Manual ungroup apply tray_count=%s", len(normalized))
        await self._sync_manual_assignments_from_repo(force=True)
        current = self._manual_group.projection_assignments()
        removed = 0
        for tray_key in normalized:
            if tray_key in current:
                removed += 1
            self._manual_group.ungroup(TrayId(tray_key))
        removed_mappings = 0
        if self._dashboard_repo is not None:
            removed_mappings = await self._dashboard_repo.remove_tray_mappings(
                tray_ids=normalized
            )
        await self._persist_manual_assignments_to_repo()
        projection = await self._compute_projection()
        await self._publish_trolley_updated(projection)
        return {
            "requested": len(normalized),
            "removed": removed,
            "mapping_removed": removed_mappings,
        }

    async def ungroup_tray_manual(self, tray_id: str) -> None:
        await self.ungroup_trays_manual([tray_id.strip()])

    async def clear_trolley_manual(
        self, trolley_id: str
    ) -> dict[str, object]:
        normalized = trolley_id.strip()
        if not normalized:
            raise ValueError("trolley_id must not be empty")
        log.info("Manual clear trolley apply trolley_id=%s", normalized)
        await self._sync_manual_assignments_from_repo(force=True)
        removed = self._manual_group.clear_trolley(normalized)
        closed_session_id: str | None = None
        if self._dashboard_repo is not None:
            closed_session_id = await self._dashboard_repo.close_open_session(
                trolley_id=normalized
            )
        if removed == 0 and closed_session_id is None:
            raise ValueError(
                f"No manual tray assignments found for trolley_id={normalized}"
            )
        await self._persist_manual_assignments_to_repo()
        projection = await self._compute_projection()
        await self._publish_trolley_updated(projection)
        return {
            "trolley_id": normalized,
            "removed": removed,
            "closed_session_id": closed_session_id,
        }

    async def delete_trolley_manual(
        self, trolley_id: str
    ) -> dict[str, object]:
        normalized = trolley_id.strip()
        if not normalized:
            raise ValueError("trolley_id must not be empty")
        result = await self.delete_trolleys_manual([normalized])
        not_found = set(result.get("not_found_trolley_ids") or [])
        return {
            "trolley_id": normalized,
            "removed": int(result.get("removed_tray_assignments") or 0),
            "closed_session_count": int(
                result.get("closed_session_count") or 0
            ),
            "deleted": normalized not in not_found,
            "not_found": normalized in not_found,
        }

    async def delete_trolleys_manual(
        self, trolley_ids: list[str]
    ) -> dict[str, object]:
        requested_ids = list(dict.fromkeys(
            t.strip() for t in trolley_ids if t and t.strip()
        ))
        if not requested_ids:
            raise ValueError("trolley_ids must not be empty")
        log.info(
            "Manual delete trolleys apply trolley_count=%s", len(requested_ids)
        )
        await self._sync_manual_assignments_from_repo(force=True)
        projection = await self._compute_projection()
        selected_set = set(requested_ids)
        trolley_to_trays: dict[str, list[str]] = {}
        for trolley in projection.trolleys:
            trolley_key = str(trolley.trolley_id).strip()
            if trolley_key not in selected_set:
                continue
            tray_ids = [
                str(tid).strip()
                for tid in trolley.tray_ids
                if str(tid).strip()
            ]
            grouped = trolley_to_trays.setdefault(trolley_key, [])
            grouped.extend(tray_ids)
        for k in trolley_to_trays:
            trolley_to_trays[k] = list(dict.fromkeys(trolley_to_trays[k]))
        current_assignments = self._manual_group.projection_assignments()
        tray_ids_to_remove: list[str] = []
        removed_assignment_count = 0
        for tray_list in trolley_to_trays.values():
            for tid in tray_list:
                tray_ids_to_remove.append(tid)
                if tid in current_assignments:
                    removed_assignment_count += 1
                self._manual_group.ungroup(TrayId(tid))
        tray_ids_to_remove = list(dict.fromkeys(tray_ids_to_remove))
        closed_session_count = 0
        removed_mapping_count = 0
        if self._dashboard_repo is not None:
            for tid in requested_ids:
                closed = await self._dashboard_repo.close_open_session(
                    trolley_id=tid
                )
                if closed is not None:
                    closed_session_count += 1
            if tray_ids_to_remove:
                removed_mapping_count = (
                    await self._dashboard_repo.remove_tray_mappings(
                        tray_ids=tray_ids_to_remove
                    )
                )
        await self._persist_manual_assignments_to_repo()
        projection = await self._compute_projection()
        await self._publish_trolley_updated(projection)
        not_found_ids = [
            tid for tid in requested_ids if tid not in trolley_to_trays
        ]
        return {
            "requested": len(requested_ids),
            "matched_trolley_count": len(trolley_to_trays),
            "removed_tray_assignments": removed_assignment_count,
            "removed_tray_mapping_count": removed_mapping_count,
            "closed_session_count": closed_session_count,
            "not_found_trolley_ids": not_found_ids,
            "deleted": len(requested_ids) - len(not_found_ids),
        }

    async def rename_trolley_manual(
        self, old_trolley_id: str, new_trolley_id: str
    ) -> dict[str, object]:
        old_id = old_trolley_id.strip()
        new_id = new_trolley_id.strip()
        if not old_id or not new_id:
            raise ValueError(
                "old_trolley_id and new_trolley_id must not be empty"
            )
        log.info(
            "Manual rename trolley apply old=%s new=%s", old_id, new_id
        )
        await self._sync_manual_assignments_from_repo(force=True)
        changed = self._manual_group.rename_trolley(old_id, new_id)
        promoted_from_auto = 0
        if changed == 0:
            projection = await self._compute_projection()
            promoted_from_auto = self._promote_auto_trolleys_to_manual(
                projection,
                source_trolley_id=old_id,
                target_trolley_id=new_id,
                preserve_auto_mode=True,
            )
            changed += promoted_from_auto
        session_updated = 0
        if self._dashboard_repo is not None:
            session_updated = (
                await self._dashboard_repo.rename_open_session_trolley(
                    old_trolley_id=old_id, new_trolley_id=new_id
                )
            )
        if changed == 0 and session_updated == 0:
            raise ValueError(
                f"No tray assignments found for trolley_id={old_id}"
            )
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

    async def set_auto_group_enabled(
        self, enabled: bool
    ) -> dict[str, object]:
        enabled = bool(enabled)
        if self._auto_group_enabled == enabled:
            return {"changed": False, "auto_group_enabled": enabled}
        promoted_auto = 0
        released_auto = 0
        if not enabled:
            await self._sync_manual_assignments_from_repo(force=True)
            projection_before = await self._compute_projection()
            promoted_auto = self._promote_auto_trolleys_to_manual(
                projection_before, preserve_auto_mode=True
            )
            if promoted_auto > 0:
                await self._persist_manual_assignments_to_repo()
        else:
            await self._sync_manual_assignments_from_repo(force=True)
            current_assignments = self._manual_group.projection_assignments()
            kept_manual: dict[str, tuple[Column, str, TrolleyMode]] = {}
            for tray_id, (column, trolley_id, mode) in current_assignments.items():
                if _normalize_trolley_mode(mode) is not TrolleyMode.MANUAL:
                    continue
                kept_manual[tray_id] = (column, trolley_id, TrolleyMode.MANUAL)
            released_auto = len(current_assignments) - len(kept_manual)
            if released_auto > 0:
                self._manual_group.restore(kept_manual)
                await self._persist_manual_assignments_to_repo()
        self._auto_group_enabled = enabled
        self._projection_cache_key = None
        self._projection_cache_value = None
        projection = await self._compute_projection()
        await self._publish_snapshot_ready(projection)
        log.info(
            "Auto group mode changed enabled=%s promoted_auto=%s "
            "released_auto=%s ungrouped=%s",
            enabled,
            promoted_auto,
            released_auto,
            len(projection.assembly_ungrouped),
        )
        return {
            "changed": True,
            "auto_group_enabled": enabled,
            "promoted_auto": promoted_auto,
            "released_auto": released_auto,
        }

    def _promote_auto_trolleys_to_manual(
        self,
        projection: Projection,
        *,
        source_trolley_id: str | None = None,
        target_trolley_id: str | None = None,
        preserve_auto_mode: bool = False,
    ) -> int:
        source_id = str(source_trolley_id or "").strip()
        target_id = str(target_trolley_id or "").strip()
        promoted = 0
        current_assignments = self._manual_group.projection_assignments()
        for trolley in projection.trolleys:
            if trolley.mode is not TrolleyMode.AUTO:
                continue
            trolley_id = str(trolley.trolley_id).strip()
            if source_id and trolley_id != source_id:
                continue
            tray_ids = [
                str(tid).strip()
                for tid in trolley.tray_ids
                if str(tid).strip()
            ]
            if not tray_ids:
                continue
            assign_trolley_id = target_id or trolley_id
            assign_mode = (
                TrolleyMode.AUTO if preserve_auto_mode else TrolleyMode.MANUAL
            )
            pending_tray_ids: list[str] = []
            for tray_id in tray_ids:
                current = current_assignments.get(tray_id)
                if current is None:
                    pending_tray_ids.append(tray_id)
                    continue
                current_column, current_trolley_id, current_mode = current
                if (
                    current_column == trolley.column
                    and str(current_trolley_id).strip() == assign_trolley_id
                    and _normalize_trolley_mode(current_mode) == assign_mode
                ):
                    continue
                pending_tray_ids.append(tray_id)
            if not pending_tray_ids:
                continue
            promoted += self._manual_group.group_many_to_trolley(
                [TrayId(t) for t in pending_tray_ids],
                trolley_id=assign_trolley_id,
                column=trolley.column,
                mode=assign_mode,
            )
            for t in pending_tray_ids:
                current_assignments[t] = (
                    trolley.column, assign_trolley_id, assign_mode
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
                raise ValueError(
                    "refresh_interval_seconds must be between 0.5 and 60.0"
                )
            if self._delta_poll_interval_seconds != refresh_interval:
                self._delta_poll_interval_seconds = refresh_interval
                settings.delta_poll_interval_seconds = refresh_interval
                self._forced_delta_interval = timedelta(
                    seconds=max(
                        self._delta_poll_idle_interval_seconds,
                        self._delta_poll_interval_seconds * 8.0,
                    )
                )
                changed = True
            payload["refresh_interval_seconds"] = self._delta_poll_interval_seconds

        if data_window_days is not None:
            normalized_days = _normalize_data_window_days(data_window_days)
            if self._ui_data_window_days != normalized_days:
                previous_window_start = self._window_start(now)
                self._ui_data_window_days = normalized_days
                self._initial_load_lookback_hours = _lookback_hours_for_window_days(
                    normalized_days
                )
                settings.ui_data_window_days = normalized_days
                settings.initial_load_lookback_hours = (
                    self._initial_load_lookback_hours
                )
                settings.ccu_backfill_lookback_hours = (
                    self._initial_load_lookback_hours
                )
                changed = True
                desired_window_start = self._window_start(now)
                if self._loaded_window_start is None:
                    payload["window_reuse"] = True
                    payload["window_loaded_start"] = None
                    payload["window_scan_skipped"] = True
                elif desired_window_start < self._loaded_window_start:
                    payload["window_reuse"] = True
                    payload["window_backfill_scheduled"] = (
                        self._schedule_window_backfill(
                            requested_start=desired_window_start,
                            reason="settings-window-expand",
                        )
                    )
                    payload["window_requested_start"] = (
                        desired_window_start.isoformat()
                    )
                    payload["window_loaded_start"] = (
                        self._loaded_window_start.isoformat()
                    )
                else:
                    self._cancel_window_backfill(
                        reason="settings-window-shrink-or-reuse"
                    )
                    payload["window_reuse"] = True
                    payload["window_previous_start"] = (
                        previous_window_start.isoformat()
                    )
                    payload["window_loaded_start"] = (
                        self._loaded_window_start.isoformat()
                    )
            payload["data_window_days"] = self._ui_data_window_days
            payload["initial_load_lookback_hours"] = (
                self._initial_load_lookback_hours
            )

        if changed:
            self._projection_cache_key = None
            self._projection_cache_value = None
            projection = await self._compute_projection()
            await self._publish_trolley_updated(projection)

        payload["changed"] = changed
        return payload

    # ═══════════════════════════════════════════════════════════════════════
    # Tray Detail (unchanged)
    # ═══════════════════════════════════════════════════════════════════════
    async def fetch_tray_cells(
        self, tray_id: str
    ) -> list[dict[str, str | None]]:
        tray_key = tray_id.strip()
        if not tray_key:
            return []
        await self._ensure_bootstrap_ready_for_detail()
        now = self._clock.now()

        cached = self._tray_cells_cache.get(tray_key)
        if cached is not None:
            cached_at, cached_rows = cached
            if (
                self._tray_cells_cache_ttl.total_seconds() <= 0
                or (now - cached_at) <= self._tray_cells_cache_ttl
            ):
                self._tray_cells_cache_stats.hits += 1
                return [dict(item) for item in cached_rows]

        self._tray_cells_cache_stats.misses += 1

        inflight = self._tray_cells_inflight.get(tray_key)
        if inflight is not None:
            rows = await inflight
            return [dict(item) for item in rows]

        task = asyncio.create_task(
            self._fetch_tray_cells_uncached(tray_key),
            name=f"tray-detail-{tray_key}",
        )
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

    async def fetch_cell_owner(
        self, cell_id: str
    ) -> dict[str, str | None] | None:
        cell_key = cell_id.strip()
        if not cell_key:
            return None
        await self._ensure_bootstrap_ready_for_detail()
        now = self._clock.now()
        start = self._window_start(now)
        started_at = time.perf_counter()
        owner = await self._ccu_repo.fetch_cell_owner(
            cell_id=cell_key, start_time=start, end_time=now
        )
        log.info(
            "Cell owner fetched cell_id=%s found=%s elapsed_ms=%s",
            cell_key,
            bool(owner),
            int((time.perf_counter() - started_at) * 1000),
        )
        return owner

    async def _fetch_tray_cells_uncached(
        self, tray_key: str
    ) -> list[dict[str, str | None]]:
        now = self._clock.now()
        start = self._window_start(now)
        started_at = time.perf_counter()
        rows = await self._ccu_repo.fetch_tray_cells(
            tray_id=tray_key, start_time=start, end_time=now
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
            raise RuntimeError(
                "Initial load is in progress. Please retry tray detail in a few seconds."
            )
        if not self._bootstrap_event.is_set():
            await self._bootstrap_event.wait()
        if self._bootstrap_error is not None:
            raise RuntimeError(
                "Initial load failed; tray detail is unavailable."
            ) from self._bootstrap_error

    def _trim_tray_detail_cache(self) -> None:
        if self._tray_cells_cache_max_entries <= 0:
            self._tray_cells_cache.clear()
            return
        overflow = len(self._tray_cells_cache) - self._tray_cells_cache_max_entries
        if overflow <= 0:
            return
        oldest = sorted(
            self._tray_cells_cache.items(), key=lambda item: item[1][0]
        )[:overflow]
        for tray_id, _ in oldest:
            self._tray_cells_cache.pop(tray_id, None)
            self._tray_cells_cache_stats.evictions += 1

    def _invalidate_tray_detail_cache_for_trays(
        self, trays: list[Tray]
    ) -> None:
        if not trays:
            return
        invalidated = 0
        for tray in trays:
            tray_id = str(tray.tray_id)
            if tray.has_ccu:
                self._pending_backfill_tray_ids.discard(tray_id)
                self._backfill_entries.pop(tray_id, None)
                self._last_backfill_attempt.pop(tray_id, None)
                self._backfill_retry_not_before.pop(tray_id, None)
                self._backfill_no_result_streak.pop(tray_id, None)
            if tray_id in self._tray_cells_cache:
                self._tray_cells_cache.pop(tray_id, None)
                invalidated += 1
        if invalidated:
            log.debug("Tray detail cache invalidated trays=%s", invalidated)

    # ═══════════════════════════════════════════════════════════════════════
    # Manual Refresh (unchanged)
    # ═══════════════════════════════════════════════════════════════════════
    async def manual_refresh(self, *, full_scan: bool) -> dict[str, object]:
        log.info("Manual refresh begin full_scan=%s", full_scan)
        if self._bootstrap_in_progress:
            return {"mode": "bootstrap_in_progress", "changed": False}
        if not self._bootstrap_event.is_set():
            await self._bootstrap_event.wait()
        if self._bootstrap_error is not None:
            raise RuntimeError(
                "Bootstrap failed; cannot run manual refresh"
            ) from self._bootstrap_error

        watermark = await self._delta_tracker.get()
        now = self._clock.now()

        if watermark is None:
            await self._initial_load()
            return {"mode": "bootstrap", "changed": True}

        if full_scan:
            result = await self._run_full_window_scan(now)
            backfill_changed = await self._maybe_backfill_ccu(
                result.store_result.missing_ccu_tray_ids,
                allow_historical_scan=True,
                allow_targeted_lookup=self._ccu_backfill_allow_targeted_lookup,
            )
            changed = bool(result.store_result.changed or backfill_changed)
            self._invalidate_tray_detail_cache_for_trays(
                result.store_result.changed
            )
            projection = await self._compute_projection()
            await self._publish_snapshot_ready(projection)
            await self._set_data_watermark(result.watermark)
            log.info(
                "Manual refresh full completed changed=%s merged=%s "
                "backfill_changed=%s",
                changed,
                len(result.merged),
                backfill_changed,
            )
            return {
                "mode": "manual_full",
                "changed": True,
                "merged": len(result.merged),
            }

        has_new_data = await self._has_new_data_since(
            watermark.collected_time, now
        )
        synced_changed = await self._sync_manual_assignments_from_repo()

        if not has_new_data.has_new_data:
            backfill_scheduled = self._schedule_ccu_backfill(
                set(),
                ccu_latest_hint=has_new_data.ccu_latest,
                allow_historical_scan=True,
                allow_targeted_lookup=self._ccu_backfill_allow_targeted_lookup,
                reason="manual-refresh-no-new-data",
            )
            projection = await self._compute_projection()
            await self._publish_trolley_updated(projection)
            log.info(
                "Manual refresh quick no-new-data synced=%s", synced_changed
            )
            return {
                "mode": "manual_quick",
                "changed": synced_changed,
                "merged": 0,
                "backfill_scheduled": backfill_scheduled,
            }

        result = await self._run_delta_scan(watermark=watermark, end_time=now)
        backfill_scheduled = self._schedule_ccu_backfill(
            result.store_result.missing_ccu_tray_ids,
            ccu_latest_hint=has_new_data.ccu_latest,
            reason="manual-refresh-delta",
        )
        self._invalidate_tray_detail_cache_for_trays(result.store_result.changed)

        if result.store_result.changed:
            for tray in result.store_result.changed:
                await self._event_bus.publish(TrayUpdated(tray=tray))

        projection = await self._compute_projection()
        await self._publish_trolley_updated(projection)
        await self._set_data_watermark(result.watermark)

        log.info(
            "Manual refresh quick completed changed=%s merged=%s "
            "backfill_scheduled=%s",
            bool(result.store_result.changed or synced_changed),
            len(result.merged),
            backfill_scheduled,
        )
        return {
            "mode": "manual_quick",
            "changed": bool(result.store_result.changed or synced_changed),
            "merged": len(result.merged),
            "backfill_scheduled": backfill_scheduled,
        }

    # ═══════════════════════════════════════════════════════════════════════
    # Initial Load — Split CCU/FPC windows for Today mode
    # ═══════════════════════════════════════════════════════════════════════
    async def _initial_load(self) -> None:
        started_at = time.perf_counter()
        now = self._clock.now()

        # ── Calculate split windows ────────────────────────────────────────
        ccu_start, fpc_start, end_time = self._calculate_load_windows()
        # Use the earliest start as the overall loaded window
        overall_start = min(ccu_start, fpc_start)

        if self._ui_show_today_only:
            today_start = self._get_today_start()
            ccu_hours = (end_time - ccu_start).total_seconds() / 3600
            fpc_hours = (end_time - fpc_start).total_seconds() / 3600
            log.info(
                "Initial load window TODAY mode "
                "ccu_start=%s (%.1fh) fpc_start=%s (%.1fh) end=%s "
                "buffer=%.1fh today_start=%s "
                "filter=show_if_ccu_today_OR_fpc_today "
                "fpc_timeout=%.0fs",
                ccu_start.isoformat(), ccu_hours,
                fpc_start.isoformat(), fpc_hours,
                end_time.isoformat(),
                self._ccu_today_lookback_buffer_hours,
                today_start.isoformat(),
                settings.fpc_initial_load_timeout_seconds,
            )
        else:
            total_hours = (end_time - overall_start).total_seconds() / 3600
            log.info(
                "Initial load window ROLLING mode "
                "start=%s end=%s lookback_hours=%.2f ui_days=%s "
                "fpc_timeout=%.0fs",
                overall_start.isoformat(),
                end_time.isoformat(),
                total_hours,
                self._ui_data_window_days,
                settings.fpc_initial_load_timeout_seconds,
            )

        previous = await self._delta_tracker.get()

        # ── Parallel fetch with SPLIT windows ──────────────────────────────
        ccu_task = asyncio.create_task(
            self._ccu_repo.fetch_initial(
                start_time=ccu_start, end_time=end_time
            ),
            name="initial-ccu-fetch",
        )
        fpc_task = asyncio.create_task(
            self._fpc_repo.fetch_initial(
                start_time=fpc_start, end_time=end_time
            ),
            name="initial-fpc-fetch",
        )

        norm_ccu: list[TraySignal] = []
        norm_fpc: list[TraySignal] = []
        partial_source = "none"
        fpc_timed_out = False

        try:
            done, _ = await asyncio.wait(
                {ccu_task, fpc_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
            first_task = next(iter(done))

            if first_task is ccu_task:
                partial_source = "ccu"
                ccu_rows = ccu_task.result()
                norm_ccu = await self._normalize_signals(ccu_rows)
                partial_result = await self._ingest.execute(
                    norm_ccu, [], previous_watermark=previous
                )
            else:
                partial_source = "fpc"
                norm_fpc = await self._normalize_signals(fpc_task.result())
                partial_result = await self._ingest.execute(
                    [], norm_fpc, previous_watermark=previous
                )
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
                "Initial load partial published source=%s merged=%s "
                "changed=%s trays=%s",
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

        # Wait for second task — FPC gets a hard timeout
        if not ccu_task.done():
            norm_ccu = await self._normalize_signals(await ccu_task)
        elif not norm_ccu and not ccu_task.cancelled():
            try:
                norm_ccu = await self._normalize_signals(ccu_task.result())
            except Exception:
                pass

        if not fpc_task.done():
            fpc_timeout = settings.fpc_initial_load_timeout_seconds
            remaining = fpc_timeout - (time.perf_counter() - started_at)
            remaining = max(10.0, remaining)
            try:
                fpc_rows = await asyncio.wait_for(fpc_task, timeout=remaining)
                norm_fpc = await self._normalize_signals(fpc_rows)
            except asyncio.TimeoutError:
                fpc_timed_out = True
                fpc_task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await fpc_task
                log.warning(
                    "FPC initial load timed out after %.0fs — "
                    "proceeding with CCU-only data. "
                    "FPC data will catch up via delta polling.",
                    fpc_timeout,
                )
                norm_fpc = []
        elif not norm_fpc and not fpc_task.cancelled():
            try:
                norm_fpc = await self._normalize_signals(fpc_task.result())
            except Exception:
                pass

        ccu_tray_ids = {str(item.tray_id) for item in norm_ccu}
        fpc_tray_ids = {str(item.tray_id) for item in norm_fpc}
        raw_overlap = ccu_tray_ids & fpc_tray_ids

        result = await self._ingest.execute(
            norm_ccu, norm_fpc, previous_watermark=previous
        )
        backfill_scheduled = self._schedule_ccu_backfill(
            result.store_result.missing_ccu_tray_ids,
            allow_historical_scan=True,
            allow_targeted_lookup=self._ccu_backfill_allow_targeted_lookup,
            reason="initial-load-final",
        )

        if self._loaded_window_start is None or overall_start < self._loaded_window_start:
            self._loaded_window_start = overall_start
        self._loaded_ccu_start = ccu_start
        self._loaded_fpc_start = fpc_start

        self._invalidate_tray_detail_cache_for_trays(result.store_result.changed)
        await self._set_data_watermark(result.watermark, fallback_time=end_time)
        projection = await self._compute_projection()
        await self._publish_snapshot_ready(projection)
        await self._save_projection_to_repo(projection)

        elapsed_ms = int((time.perf_counter() - started_at) * 1000)
        log.info(
            "Initial load completed elapsed_ms=%s merged=%s trays=%s "
            "ccu=%s fpc=%s overlap=%s backfill_scheduled=%s "
            "watermark=%s partial_source=%s fpc_timed_out=%s "
            "mode=%s",
            elapsed_ms,
            len(result.merged),
            len(projection.trays),
            len(norm_ccu),
            len(norm_fpc),
            len(raw_overlap),
            backfill_scheduled,
            end_time.isoformat(),
            partial_source,
            fpc_timed_out,
            "today" if self._ui_show_today_only else "rolling",
        )

        if fpc_timed_out:
            log.info(
                "FPC timeout recovery: scheduling window backfill for "
                "FPC catch-up via next delta polls"
            )
            self._schedule_window_backfill(
                requested_start=fpc_start, reason="fpc-timeout-recovery"
            )

    # ═══════════════════════════════════════════════════════════════════════
    # Refresh Loop (unchanged)
    # ═══════════════════════════════════════════════════════════════════════
    async def _refresh_loop(self) -> None:
        log.info(
            "Refresh loop started interval_seconds=%s",
            self._delta_poll_interval_seconds,
        )
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
            except Exception:
                log.exception("Refresh loop failed")

            sleep_seconds = (
                self._delta_poll_interval_seconds
                if had_change
                else max(
                    self._delta_poll_interval_seconds,
                    self._delta_poll_idle_interval_seconds,
                )
            )
            try:
                await asyncio.wait_for(
                    self._stop_event.wait(), timeout=sleep_seconds
                )
            except asyncio.TimeoutError:
                pass

        log.info("Refresh loop stopped")

    async def _refresh_once(self) -> bool:
        iteration = self._refresh_iteration
        started_at = time.perf_counter()

        if self._stop_event.is_set() or not self._running:
            return False
        if self._active_detail_queries > 0:
            return False
        if (
            self._refresh_skip_while_backfill
            and self._backfill_task is not None
            and not self._backfill_task.done()
        ):
            return False

        watermark = await self._delta_tracker.get()
        if watermark is None:
            return False

        now = self._clock.now()
        has_new_data = await self._has_new_data_since(
            watermark.collected_time, now
        )

        if not has_new_data.has_new_data:
            synced_changed = await self._sync_manual_assignments_from_repo()
            await self._advance_watermark_if_stuck(
                watermark=watermark, now=now
            )
            self._schedule_ccu_backfill(
                set(),
                ccu_latest_hint=has_new_data.ccu_latest,
                allow_historical_scan=True,
                allow_targeted_lookup=self._ccu_backfill_allow_targeted_lookup,
                reason=f"refresh#{iteration}-no-new-data",
            )
            if synced_changed:
                projection = await self._compute_projection()
                await self._publish_trolley_updated(projection)
            return bool(synced_changed)

        ingest_result = await self._run_delta_scan(
            watermark=watermark, end_time=now
        )
        self._schedule_ccu_backfill(
            ingest_result.store_result.missing_ccu_tray_ids,
            ccu_latest_hint=has_new_data.ccu_latest,
            reason=f"refresh#{iteration}-delta",
        )
        synced_changed = await self._sync_manual_assignments_from_repo()
        self._invalidate_tray_detail_cache_for_trays(
            ingest_result.store_result.changed
        )

        if ingest_result.store_result.changed:
            for tray in ingest_result.store_result.changed:
                await self._event_bus.publish(TrayUpdated(tray=tray))

        if ingest_result.store_result.changed or synced_changed:
            projection = await self._compute_projection()
            await self._publish_trolley_updated(projection)

        await self._set_data_watermark(ingest_result.watermark)
        changed = bool(ingest_result.store_result.changed or synced_changed)

        log.info(
            "Refresh #%s completed elapsed_ms=%s merged=%s changed=%s",
            iteration,
            int((time.perf_counter() - started_at) * 1000),
            len(ingest_result.merged),
            len(ingest_result.store_result.changed),
        )
        return changed

    async def _advance_watermark_if_stuck(
        self, watermark: Watermark, now: datetime
    ) -> None:
        safety_buffer = timedelta(seconds=max(30.0, settings.delta_overlap_seconds * 2))
        new_ct = now - safety_buffer
        if new_ct <= watermark.collected_time:
            return
        new_wm = Watermark(
            collected_time=new_ct,
            tray_id=watermark.tray_id,
        )
        await self._delta_tracker.set(new_wm)
        log.debug(
            "Watermark advanced (stuck guard) from=%s to=%s",
            watermark.collected_time.isoformat(),
            new_ct.isoformat(),
        )

    # ═══════════════════════════════════════════════════════════════════════
    # Scan Operations (unchanged)
    # ═══════════════════════════════════════════════════════════════════════
    async def _run_full_window_scan(self, end_time: datetime) -> IngestResult:
        started_at = time.perf_counter()

        # Use split windows for today mode
        ccu_start, fpc_start, _ = self._calculate_load_windows()

        ccu_rows, fpc_rows = await asyncio.gather(
            self._ccu_repo.fetch_initial(start_time=ccu_start, end_time=end_time),
            self._fpc_repo.fetch_initial(start_time=fpc_start, end_time=end_time),
        )
        norm_ccu, norm_fpc = await asyncio.gather(
            self._normalize_signals(ccu_rows),
            self._normalize_signals(fpc_rows),
        )
        previous = await self._delta_tracker.get()
        result = await self._ingest.execute(
            norm_ccu, norm_fpc, previous_watermark=previous
        )
        overall_start = min(ccu_start, fpc_start)
        if self._loaded_window_start is None or overall_start < self._loaded_window_start:
            self._loaded_window_start = overall_start
        self._loaded_ccu_start = ccu_start
        self._loaded_fpc_start = fpc_start
        log.info(
            "Full window scan done elapsed_ms=%s ccu=%s fpc=%s merged=%s "
            "mode=%s",
            int((time.perf_counter() - started_at) * 1000),
            len(norm_ccu),
            len(norm_fpc),
            len(result.merged),
            "today" if self._ui_show_today_only else "rolling",
        )
        return result

    async def _run_delta_scan(
        self, *, watermark: Watermark, end_time: datetime
    ) -> IngestResult:
        started_at = time.perf_counter()
        ccu_rows, fpc_rows = await asyncio.gather(
            self._ccu_repo.fetch_delta(watermark=watermark, end_time=end_time),
            self._fpc_repo.fetch_delta(watermark=watermark, end_time=end_time),
        )
        norm_ccu, norm_fpc = await asyncio.gather(
            self._normalize_signals(ccu_rows),
            self._normalize_signals(fpc_rows),
        )
        result = await self._ingest.execute(
            norm_ccu, norm_fpc, previous_watermark=watermark
        )
        log.debug(
            "Delta scan done elapsed_ms=%s ccu=%s fpc=%s merged=%s",
            int((time.perf_counter() - started_at) * 1000),
            len(norm_ccu),
            len(norm_fpc),
            len(result.merged),
        )
        return result

    # ═══════════════════════════════════════════════════════════════════════
    # Peek Latest (unchanged)
    # ═══════════════════════════════════════════════════════════════════════
    async def _has_new_data_since(
        self, since: datetime, end_time: datetime
    ) -> _HasNewDataResult:
        if not self._refresh_peek_enabled:
            now = self._clock.now()
            next_scan_at = self._forced_delta_next_at
            if next_scan_at is not None and now < next_scan_at:
                return _HasNewDataResult(has_new_data=False, was_fresh=False)
            self._forced_delta_next_at = now + self._forced_delta_interval
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
        has_new = (
            (ccu_latest is not None and ccu_latest > since)
            or (fpc_latest is not None and fpc_latest > since)
        )

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
                seconds=min(
                    self._peek_latest_cache_ttl_max.total_seconds(),
                    grown_seconds,
                )
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
                    return cached_latest, False
        latest = await fetcher(end_time=end_time)
        fetched_at = self._clock.now()
        self._peek_latest_cache[cache_key] = (fetched_at, end_time, latest)
        return latest, True

    # ═══════════════════════════════════════════════════════════════════════
    # CCU Backfill — Fix #5: give-up, Fix #6: exponential backoff
    # ═══════════════════════════════════════════════════════════════════════
    def _schedule_ccu_backfill(
        self,
        tray_ids: set[str] | None = None,
        *,
        ccu_latest_hint: datetime | None = None,
        allow_historical_scan: bool = False,
        allow_targeted_lookup: bool | None = None,
        reason: str,
    ) -> bool:
        incoming = {
            tid.strip()
            for tid in (tray_ids or set())
            if tid and tid.strip()
        }

        if incoming:
            now = datetime.utcnow()
            for tray_id in incoming:
                if tray_id not in self._backfill_entries:
                    self._backfill_entries[tray_id] = _BackfillEntry(
                        tray_id=tray_id, first_seen=now
                    )
                self._pending_backfill_tray_ids.add(tray_id)
                self._backfill_retry_not_before.pop(tray_id, None)
                self._backfill_no_result_streak.pop(tray_id, None)

        self._prune_exhausted_backfill_entries()

        if not self._pending_backfill_tray_ids:
            return False

        if ccu_latest_hint is not None:
            if (
                self._scheduled_backfill_latest_hint is None
                or ccu_latest_hint > self._scheduled_backfill_latest_hint
            ):
                self._scheduled_backfill_latest_hint = ccu_latest_hint

        self._scheduled_backfill_allow_historical_scan = (
            self._scheduled_backfill_allow_historical_scan
            or bool(allow_historical_scan)
        )
        effective_targeted = (
            self._ccu_backfill_allow_targeted_lookup
            if allow_targeted_lookup is None
            else bool(allow_targeted_lookup)
        )
        self._scheduled_backfill_allow_targeted_lookup = (
            self._scheduled_backfill_allow_targeted_lookup or effective_targeted
        )

        if self._backfill_task is not None and not self._backfill_task.done():
            return False

        self._backfill_task = asyncio.create_task(
            self._run_scheduled_backfill(reason=reason),
            name="wip-ccu-backfill",
        )
        log.info(
            "CCU backfill scheduled reason=%s pending=%s",
            reason,
            len(self._pending_backfill_tray_ids),
        )
        return True

    def _prune_exhausted_backfill_entries(self) -> None:
        exhausted: list[str] = []
        for tray_id, entry in list(self._backfill_entries.items()):
            if tray_id not in self._pending_backfill_tray_ids:
                continue
            if entry.is_exhausted():
                exhausted.append(tray_id)

        for tray_id in exhausted:
            entry = self._backfill_entries.pop(tray_id, None)
            self._pending_backfill_tray_ids.discard(tray_id)
            self._backfill_retry_not_before.pop(tray_id, None)
            self._backfill_no_result_streak.pop(tray_id, None)
            log.warning(
                "CCU backfill permanently giving up tray_id=%s "
                "attempts=%s age_min=%.1f — tray accepted as FPC-only",
                tray_id,
                entry.attempt_count if entry else "?",
                (
                    (datetime.utcnow() - entry.first_seen).total_seconds() / 60.0
                    if entry
                    else 0
                ),
            )

    async def _run_scheduled_backfill(self, *, reason: str) -> None:
        try:
            while self._running and self._pending_backfill_tray_ids:
                window_task = self._window_backfill_task
                if window_task is not None and not window_task.done():
                    try:
                        await asyncio.wait_for(self._stop_event.wait(), timeout=1.0)
                    except asyncio.TimeoutError:
                        pass
                    continue

                allow_historical_scan = self._scheduled_backfill_allow_historical_scan
                allow_targeted_lookup = self._scheduled_backfill_allow_targeted_lookup
                self._scheduled_backfill_latest_hint = None
                self._scheduled_backfill_allow_historical_scan = False
                self._scheduled_backfill_allow_targeted_lookup = False

                changed = await self._maybe_backfill_ccu(
                    set(),
                    allow_historical_scan=allow_historical_scan,
                    allow_targeted_lookup=allow_targeted_lookup,
                )
                synced_changed = await self._sync_manual_assignments_from_repo()
                if changed or synced_changed:
                    projection = await self._compute_projection()
                    await self._publish_trolley_updated(projection)

                self._prune_exhausted_backfill_entries()

                if self._pending_backfill_tray_ids:
                    max_attempts = max(
                        (
                            self._backfill_entries[tid].attempt_count
                            for tid in self._pending_backfill_tray_ids
                            if tid in self._backfill_entries
                        ),
                        default=0,
                    )
                    delay = settings.get_ccu_backfill_next_delay(max_attempts)
                    log.debug(
                        "CCU backfill sleeping %.1fs before retry "
                        "(attempt=%s pending=%s)",
                        delay,
                        max_attempts,
                        len(self._pending_backfill_tray_ids),
                    )
                    try:
                        await asyncio.wait_for(
                            self._stop_event.wait(), timeout=delay
                        )
                        break
                    except asyncio.TimeoutError:
                        self._scheduled_backfill_allow_historical_scan = True
                        self._scheduled_backfill_allow_targeted_lookup = (
                            self._ccu_backfill_allow_targeted_lookup
                        )
                        continue
                else:
                    break

            log.info("CCU backfill worker completed reason=%s", reason)
        except asyncio.CancelledError:
            log.info("CCU backfill worker cancelled reason=%s", reason)
            raise
        except Exception:
            log.exception("CCU backfill worker failed reason=%s", reason)
        finally:
            self._backfill_task = None

    async def _maybe_backfill_ccu(
        self,
        tray_ids: set[str] | None = None,
        *,
        allow_historical_scan: bool = False,
        allow_targeted_lookup: bool | None = None,
    ) -> bool:
        incoming = {
            tid.strip()
            for tid in (tray_ids or set())
            if tid and tid.strip()
        }
        if incoming:
            self._pending_backfill_tray_ids.update(incoming)
        if not self._pending_backfill_tray_ids:
            return False
        if self._stop_event.is_set() or not self._running:
            return False

        effective_targeted = (
            self._ccu_backfill_allow_targeted_lookup
            if allow_targeted_lookup is None
            else bool(allow_targeted_lookup)
        )

        now = datetime.utcnow()

        eligible: list[str] = []
        for tray_id in sorted(self._pending_backfill_tray_ids):
            entry = self._backfill_entries.get(tray_id)
            if entry is None:
                entry = _BackfillEntry(tray_id=tray_id, first_seen=now)
                self._backfill_entries[tray_id] = entry

            if entry.is_exhausted():
                continue
            if not entry.is_ready():
                continue

            eligible.append(tray_id)

        if not eligible:
            return False

        log.info(
            "CCU backfill fetch tray_ids=%s pending=%s",
            len(eligible),
            len(self._pending_backfill_tray_ids),
        )

        for tray_id in eligible:
            entry = self._backfill_entries.get(tray_id)
            if entry is not None:
                entry.record_attempt()
            self._last_backfill_attempt[tray_id] = datetime.utcnow()

        _BACKFILL_FETCH_TIMEOUT = max(
            30.0, settings.sql_query_timeout_seconds + 5.0
        )

        fetch_coro = self._ccu_repo.fetch_by_tray_ids(
            tray_ids=eligible,
            end_time=datetime.utcnow(),
            allow_historical_scan=allow_historical_scan,
            allow_targeted_lookup=effective_targeted,
        )
        fetch_task: asyncio.Task = asyncio.ensure_future(fetch_coro)
        stop_task: asyncio.Task = asyncio.ensure_future(self._stop_event.wait())

        try:
            done, pending_set = await asyncio.wait(
                {fetch_task, stop_task},
                timeout=_BACKFILL_FETCH_TIMEOUT,
                return_when=asyncio.FIRST_COMPLETED,
            )
        except asyncio.CancelledError:
            fetch_task.cancel()
            stop_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await fetch_task
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await stop_task
            log.info("CCU backfill cancelled during fetch")
            raise

        for t in pending_set:
            t.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await t

        if stop_task in done or self._stop_event.is_set():
            if fetch_task not in done:
                fetch_task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await fetch_task
            log.info(
                "CCU backfill interrupted by stop event eligible=%s",
                len(eligible),
            )
            raise asyncio.CancelledError()

        if fetch_task not in done:
            log.warning(
                "CCU backfill timeout after %.0fs eligible=%s pending=%s "
                "— will retry with backoff",
                _BACKFILL_FETCH_TIMEOUT,
                len(eligible),
                len(self._pending_backfill_tray_ids),
            )
            return False

        if fetch_task.exception() is not None:
            log.error(
                "CCU backfill fetch error: %r", fetch_task.exception()
            )
            return False

        ccu_rows = fetch_task.result()

        if not ccu_rows:
            log.info(
                "CCU backfill returned no rows pending=%s",
                len(self._pending_backfill_tray_ids),
            )
            return False

        norm_ccu = await self._normalize_signals(ccu_rows)
        resolved = {str(item.tray_id) for item in norm_ccu}

        if resolved:
            self._pending_backfill_tray_ids.difference_update(resolved)
            for tray_id in resolved:
                self._backfill_entries.pop(tray_id, None)
                self._last_backfill_attempt.pop(tray_id, None)
                self._backfill_retry_not_before.pop(tray_id, None)
                self._backfill_no_result_streak.pop(tray_id, None)

        current_wm = await self._delta_tracker.get()
        ingest_result = await self._ingest.execute(
            norm_ccu, [], previous_watermark=current_wm
        )
        self._invalidate_tray_detail_cache_for_trays(
            ingest_result.store_result.changed
        )
        await self._set_data_watermark(ingest_result.watermark)

        if not ingest_result.store_result.changed:
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

    # ═══════════════════════════════════════════════════════════════════════
    # Window Backfill (unchanged)
    # ═══════════════════════════════════════════════════════════════════════
    def _schedule_window_backfill(
        self, *, requested_start: datetime, reason: str
    ) -> bool:
        if not self._running:
            return False
        if self._loaded_window_start is None:
            return False
        if requested_start >= self._loaded_window_start:
            return False
        if (
            self._scheduled_window_backfill_start is None
            or requested_start < self._scheduled_window_backfill_start
        ):
            self._scheduled_window_backfill_start = requested_start
        if (
            self._window_backfill_task is not None
            and not self._window_backfill_task.done()
        ):
            return True
        self._window_backfill_generation += 1
        generation = self._window_backfill_generation
        self._window_backfill_task = asyncio.create_task(
            self._run_scheduled_window_backfill(
                reason=reason, generation=generation
            ),
            name="wip-window-backfill-worker",
        )
        return True

    def _cancel_window_backfill(self, *, reason: str) -> None:
        self._scheduled_window_backfill_start = None
        self._window_backfill_generation += 1
        if (
            self._window_backfill_task is None
            or self._window_backfill_task.done()
        ):
            return
        self._window_backfill_task.cancel()

    async def _run_scheduled_window_backfill(
        self, *, reason: str, generation: int
    ) -> None:
        try:
            while self._running:
                if generation != self._window_backfill_generation:
                    break
                target_start = self._scheduled_window_backfill_start
                loaded_start = self._loaded_window_start
                self._scheduled_window_backfill_start = None
                if (
                    target_start is None
                    or loaded_start is None
                    or target_start >= loaded_start
                ):
                    break
                result = await self._run_full_window_scan(self._clock.now())
                self._invalidate_tray_detail_cache_for_trays(
                    result.store_result.changed
                )
                await self._set_data_watermark(
                    result.watermark, fallback_time=self._clock.now()
                )
                if result.store_result.changed:
                    projection = await self._compute_projection()
                    await self._publish_trolley_updated(projection)
                break
            log.info(
                "Window backfill worker completed reason=%s generation=%s",
                reason,
                generation,
            )
        except asyncio.CancelledError:
            log.info(
                "Window backfill worker cancelled reason=%s generation=%s",
                reason,
                generation,
            )
            raise
        except Exception:
            log.exception(
                "Window backfill worker failed reason=%s generation=%s",
                reason,
                generation,
            )
        finally:
            if (
                self._window_backfill_task is not None
                and self._window_backfill_task.done()
            ):
                self._window_backfill_task = None

    # ═══════════════════════════════════════════════════════════════════════
    # Projection — Today-Only Aware
    # ═══════════════════════════════════════════════════════════════════════
    async def _compute_projection(self) -> Projection:
        started_at = time.perf_counter()
        now = self._clock.now()
        window_start = self._ui_window_start(now)
        manual_assignments = self._manual_group.projection_assignments()

        manual_signature = tuple(
            sorted(
                (tray_id, column.value, trolley_id, mode.value)
                for tray_id, (column, trolley_id, mode) in manual_assignments.items()
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
            self._ui_show_today_only,  # Include in cache key
        )

        if (
            self._projection_cache_key == cache_key
            and self._projection_cache_value is not None
        ):
            return self._projection_cache_value

        snapshot = await self._store.snapshot_desc(limit=None)
        # Apply today-only or rolling window filter
        snapshot = self._filter_trays_for_display(snapshot, end_time=now)

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
            "Projection computed elapsed_ms=%s trays=%s mode=%s",
            int((time.perf_counter() - started_at) * 1000),
            len(projection.trays),
            "today" if self._ui_show_today_only else "rolling",
        )
        return projection

    # ═══════════════════════════════════════════════════════════════════════
    # Event Publishing (unchanged)
    # ═══════════════════════════════════════════════════════════════════════
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

    # ═══════════════════════════════════════════════════════════════════════
    # State Sync (unchanged)
    # ═══════════════════════════════════════════════════════════════════════
    async def _sync_manual_assignments_from_repo(
        self, *, force: bool = False
    ) -> bool:
        if self._grouping_state_repo is None:
            return False
        now = self._clock.now()
        if not force and self._last_grouping_sync_at is not None:
            if (now - self._last_grouping_sync_at) < self._grouping_sync_interval:
                return False
        try:
            loaded = await self._grouping_state_repo.load_manual_assignments()
        except Exception:
            log.exception(
                "Failed to load manual assignments from shared repo"
            )
            self._last_grouping_sync_at = now
            return False

        parsed: dict[str, tuple[Column, str, TrolleyMode]] = {}
        for tray_id, assignment in loaded.items():
            if len(assignment) == 2:
                column_name, trolley_id = assignment
                mode_name = TrolleyMode.MANUAL.value
            else:
                column_name, trolley_id, mode_name = assignment
            try:
                mode = TrolleyMode(str(mode_name).strip().lower())
            except ValueError:
                mode = TrolleyMode.MANUAL
            try:
                parsed[tray_id] = (Column(column_name), trolley_id, mode)
            except ValueError:
                log.warning(
                    "Skip invalid manual assignment tray_id=%s column=%s "
                    "trolley_id=%s mode=%s",
                    tray_id,
                    column_name,
                    trolley_id,
                    mode_name,
                )

        current = self._manual_group.projection_assignments()
        self._last_grouping_sync_at = now

        if parsed == current:
            return False

        self._manual_group.restore(parsed)
        log.info(
            "Manual assignments synced from shared repo count=%s", len(parsed)
        )
        return True

    async def _persist_manual_assignments_to_repo(self) -> None:
        if self._grouping_state_repo is None:
            return
        projection_assignments = self._manual_group.projection_assignments()
        payload: dict[str, tuple[str, str, str]] = {}
        for tray_id, (column, trolley_id, mode) in projection_assignments.items():
            payload[tray_id] = (column.value, trolley_id, mode.value)
        await self._grouping_state_repo.replace_manual_assignments(payload)
        self._last_grouping_sync_at = self._clock.now()
        log.debug(
            "Manual assignments persisted to shared repo count=%s",
            len(payload),
        )

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
        except Exception:
            log.exception("Failed to save projection to shared repo")

    # ═══════════════════════════════════════════════════════════════════════
    # Watermark (unchanged)
    # ═══════════════════════════════════════════════════════════════════════
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

    # ═══════════════════════════════════════════════════════════════════════
    # Signal Normalization (unchanged)
    # ═══════════════════════════════════════════════════════════════════════
    async def _normalize_signals(
        self, rows: list[TraySignal]
    ) -> list[TraySignal]:
        if not rows:
            return []
        started_at = time.perf_counter()
        loop = asyncio.get_running_loop()
        normalized = await loop.run_in_executor(
            self._executor, _normalize_signals_sync, rows
        )
        log.debug(
            "Normalize signals done elapsed_ms=%s input=%s output=%s",
            int((time.perf_counter() - started_at) * 1000),
            len(rows),
            len(normalized),
        )
        return normalized


# ═══════════════════════════════════════════════════════════════════════════
# Helper Functions
# ═══════════════════════════════════════════════════════════════════════════

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
    out.sort(
        key=lambda s: (s.collected_time, str(s.tray_id), s.source.value)
    )
    return out


def _normalize_data_window_days(value: int) -> int:
    days = int(value)
    if days < 1:
        return 1
    if days > _MAX_UI_DATA_WINDOW_DAYS:
        return _MAX_UI_DATA_WINDOW_DAYS
    return days


def _lookback_hours_for_window_days(days: int) -> float:
    normalized_days = _normalize_data_window_days(days)
    return float(normalized_days * 24 + 2)


def _normalize_trolley_mode(mode: object) -> TrolleyMode:
    if isinstance(mode, TrolleyMode):
        return mode
    try:
        return TrolleyMode(str(mode).strip().lower())
    except Exception:
        return TrolleyMode.MANUAL


def _projection_to_payload(projection: Projection) -> dict[str, object]:
    return {
        "trays": [tray.to_dict() for tray in projection.trays],
        "assembly_trolleys": [
            trolley.to_dict() for trolley in projection.assembly_trolleys
        ],
        "queue_trolleys": [
            trolley.to_dict() for trolley in projection.queue_trolleys
        ],
        "precharge_trolleys": [
            trolley.to_dict() for trolley in projection.precharge_trolleys
        ],
        "assembly_ungrouped": [
            tray.to_dict() for tray in projection.assembly_ungrouped
        ],
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
        "assembly_trolleys": [
            trolley.to_dict() for trolley in projection.assembly_trolleys
        ],
        "queue_trolleys": [
            trolley.to_dict() for trolley in projection.queue_trolleys
        ],
        "precharge_trolleys": [
            trolley.to_dict() for trolley in projection.precharge_trolleys
        ],
        "assembly_ungrouped": [
            tray.to_dict() for tray in projection.assembly_ungrouped
        ],
        "assembly_ungrouped_tray_ids": [
            str(tray.tray_id) for tray in projection.assembly_ungrouped
        ],
        "data_window_days": int(data_window_days),
        "loaded_window_start": (
            loaded_window_start.isoformat()
            if loaded_window_start is not None
            else None
        ),
        "data_watermark": (
            {
                "collected_time": watermark.collected_time.isoformat(),
                "tray_id": watermark.tray_id,
            }
            if watermark is not None
            else None
        ),
    }


async def _maybe_start(obj: object) -> None:
    method = getattr(obj, "start", None)
    if callable(method):
        await method()


async def _maybe_close(obj: object) -> None:
    method = getattr(obj, "close", None)
    if callable(method):
        await method()