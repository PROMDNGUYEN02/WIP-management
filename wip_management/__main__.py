from __future__ import annotations

import asyncio
import contextlib
import logging
import logging.handlers
import os
from pathlib import Path
import sys
import threading
from datetime import datetime
from typing import Any, Callable

from PySide6.QtCore import QObject, Qt, Signal
from PySide6.QtWidgets import (
    QApplication,
    QCheckBox,
    QComboBox,
    QDialog,
    QDialogButtonBox,
    QDoubleSpinBox,
    QFormLayout,
    QHeaderView,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QListView,
    QMainWindow,
    QMessageBox,
    QPushButton,
    QSpinBox,
    QTableView,
    QVBoxLayout,
    QWidget,
)

from wip_management.application.services.orchestrator import OrchestratorService
from wip_management.application.state.state_store import SingleWriterStateStore
from wip_management.config import settings
from wip_management.domain.events import SnapshotReady, TrolleyUpdated, TrayUpdated
from wip_management.domain.models.trolley import Column
from wip_management.infrastructure.messaging.event_bus import AsyncEventBus
from wip_management.infrastructure.persistence.state_repo import SharedGroupingStateRepository
from wip_management.infrastructure.sqlserver.ccu_repo import CcuRepo
from wip_management.infrastructure.sqlserver.connection import SQLServerConnection
from wip_management.infrastructure.sqlserver.delta_tracker import InMemoryDeltaTracker
from wip_management.infrastructure.sqlserver.fpc_repo import FpcRepo
from wip_management.presentation.ui.viewmodels import BoardViewModel

log = logging.getLogger(__name__)


def _configure_logging() -> None:
    level_name = settings.log_level.upper().strip() or "INFO"
    level = getattr(logging, level_name, logging.INFO)
    formatter = logging.Formatter(
        fmt="%(asctime)s %(levelname)s %(name)s [%(threadName)s] %(message)s",
    )

    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(level)

    if settings.log_to_console:
        console_handler = logging.StreamHandler(stream=sys.stdout)
        console_handler.setFormatter(formatter)
        root.addHandler(console_handler)

    log_path = settings.log_file.strip()
    if log_path:
        path = Path(log_path)
        if not path.is_absolute():
            path = Path.cwd() / path
        path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.handlers.RotatingFileHandler(
            filename=path,
            maxBytes=20 * 1024 * 1024,
            backupCount=5,
            encoding="utf-8",
        )
        file_handler.setFormatter(formatter)
        root.addHandler(file_handler)

    logging.captureWarnings(True)
    log.info(
        "Logging configured level=%s console=%s file=%s",
        level_name,
        settings.log_to_console,
        log_path or "<disabled>",
    )


class _SystemClock:
    def now(self) -> datetime:
        return datetime.now()


class _UiBridge(QObject):
    event_ready = Signal(object)


class _UiActionBridge(QObject):
    action_done = Signal(str, object, object)


class _Runtime:
    def __init__(self, bridge: _UiBridge) -> None:
        self._bridge = bridge
        self._thread: threading.Thread | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._stop_event: asyncio.Event | None = None
        self._orchestrator: OrchestratorService | None = None
        self._started = threading.Event()
        self._start_error: BaseException | None = None

    def start(self) -> None:
        log.info("Runtime start requested")
        if self._thread is not None and self._thread.is_alive():
            log.warning("Runtime already started, skipping duplicate start")
            return
        self._started.clear()
        self._start_error = None
        self._thread = threading.Thread(target=self._thread_main, name="wip-runtime", daemon=True)
        self._thread.start()
        if not self._started.wait(timeout=20):
            log.error("Runtime bootstrap wait timed out after 20s")
            raise TimeoutError("Timed out while bootstrapping runtime thread")
        if self._start_error is not None:
            log.error("Runtime start failed error=%r", self._start_error)
            raise RuntimeError("Failed to start runtime") from self._start_error
        log.info("Runtime thread bootstrapped successfully")

    def stop(self, timeout: float = 60.0) -> None:
        log.info("Runtime stop requested")
        if self._thread is None:
            log.debug("Runtime stop ignored because thread is None")
            return
        if self._loop is not None and self._stop_event is not None:
            self._loop.call_soon_threadsafe(self._stop_event.set)
        self._thread.join(timeout=timeout)
        if self._thread.is_alive():
            log.warning("Runtime thread did not stop within timeout=%ss", timeout)
        else:
            log.info("Runtime thread stopped")
        self._thread = None

    def manual_refresh(self, *, full_scan: bool) -> dict[str, object]:
        log.info("Manual refresh requested full_scan=%s", full_scan)
        return self._submit(self._manual_refresh(full_scan=full_scan))

    def manual_group(self, tray_id: str, trolley_id: str, column: Column) -> None:
        log.info("Manual group requested tray_id=%s trolley_id=%s column=%s", tray_id, trolley_id, column.value)
        self._submit(self._manual_group(tray_id=tray_id, trolley_id=trolley_id, column=column))

    def manual_group_many(self, tray_ids: list[str], trolley_id: str, column: Column) -> dict[str, object]:
        log.info(
            "Manual group many requested tray_count=%s trolley_id=%s column=%s",
            len(tray_ids),
            trolley_id,
            column.value,
        )
        return self._submit(
            self._manual_group_many(tray_ids=tray_ids, trolley_id=trolley_id, column=column),
        )

    def manual_ungroup(self, tray_id: str) -> None:
        log.info("Manual ungroup requested tray_id=%s", tray_id)
        self._submit(self._manual_ungroup(tray_id=tray_id))

    def rename_trolley(self, old_trolley_id: str, new_trolley_id: str) -> dict[str, object]:
        log.info("Rename trolley requested old=%s new=%s", old_trolley_id, new_trolley_id)
        return self._submit(self._rename_trolley(old_trolley_id=old_trolley_id, new_trolley_id=new_trolley_id))

    def clear_trolley(self, trolley_id: str) -> dict[str, object]:
        log.info("Clear trolley requested trolley_id=%s", trolley_id)
        return self._submit(self._clear_trolley(trolley_id=trolley_id))

    def delete_trolley(self, trolley_id: str) -> dict[str, object]:
        log.info("Delete trolley requested trolley_id=%s", trolley_id)
        return self._submit(self._delete_trolley(trolley_id=trolley_id))

    def set_auto_group_enabled(self, enabled: bool) -> dict[str, object]:
        log.info("Set auto group requested enabled=%s", enabled)
        return self._submit(self._set_auto_group_enabled(enabled=enabled))

    def update_grouping_settings(self, *, max_trays_per_trolley: int) -> dict[str, object]:
        log.info("Update grouping settings requested max_trays_per_trolley=%s", max_trays_per_trolley)
        return self._submit(self._update_grouping_settings(max_trays_per_trolley=max_trays_per_trolley))

    def _submit(self, coroutine: Any) -> Any:
        if self._loop is None:
            raise RuntimeError("Runtime loop is not available")
        log.debug("Submitting coroutine to runtime loop coroutine=%s", getattr(coroutine, "__qualname__", type(coroutine)))
        future = asyncio.run_coroutine_threadsafe(coroutine, self._loop)
        return future.result(timeout=300)

    def _thread_main(self) -> None:
        log.info("Runtime thread main started")
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        try:
            self._loop.run_until_complete(self._run())
        except BaseException as exc:  # noqa: BLE001
            self._start_error = exc
            log.exception("Runtime crashed")
            self._started.set()
        finally:
            if self._loop is not None:
                self._loop.close()
            self._loop = None
            log.info("Runtime thread main exited")

    async def _run(self) -> None:
        log.info("Runtime async bootstrap started")
        connection = SQLServerConnection()
        ccu_repo = CcuRepo(connection)
        fpc_repo = FpcRepo(connection)
        delta_tracker = InMemoryDeltaTracker()
        event_bus = AsyncEventBus(default_queue_size=settings.event_queue_size)
        state_store = SingleWriterStateStore(queue_size=settings.event_queue_size)
        grouping_state_repo = None
        if settings.shared_state_enabled:
            grouping_state_repo = SharedGroupingStateRepository(
                directory=settings.shared_state_dir,
                file_name=settings.shared_state_file,
            )
            log.info(
                "Shared grouping state enabled path=%s",
                (Path(settings.shared_state_dir) / settings.shared_state_file),
            )
        else:
            log.info("Shared grouping state disabled")

        orchestrator = OrchestratorService(
            ccu_repo=ccu_repo,
            fpc_repo=fpc_repo,
            delta_tracker=delta_tracker,
            event_bus=event_bus,
            state_store=state_store,
            clock=_SystemClock(),
            initial_load_start_hour=settings.initial_load_start_hour,
            delta_poll_interval_seconds=settings.delta_poll_interval_seconds,
            delta_poll_idle_interval_seconds=settings.delta_poll_idle_interval_seconds,
            backfill_cooldown_seconds=settings.ccu_backfill_cooldown_seconds,
            max_parallel_workers=settings.max_parallel_workers,
            snapshot_limit=settings.ui_snapshot_limit,
            max_trays_per_trolley=settings.trolley_max_trays,
            assembly_auto_trolley_count=settings.assembly_auto_trolley_count,
            auto_group_enabled=settings.auto_group_default_enabled,
            grouping_sync_interval_seconds=settings.grouping_sync_interval_seconds,
            grouping_state_repo=grouping_state_repo,
        )
        self._orchestrator = orchestrator

        queue = await event_bus.subscribe(maxsize=settings.event_queue_size)
        log.info("Runtime subscribed to event bus queue_size=%s", settings.event_queue_size)
        self._stop_event = asyncio.Event()
        ui_task = asyncio.create_task(self._ui_event_loop(queue), name="ui-event-coalescer")
        self._started.set()
        log.info("Runtime bootstrap signal emitted to UI thread")
        try:
            log.info("Starting orchestrator service")
            await orchestrator.start()
            log.info("Orchestrator started successfully, entering wait state")
            await self._stop_event.wait()
            log.info("Stop event received")
        finally:
            log.info("Runtime shutdown sequence started")
            with contextlib.suppress(Exception):
                await orchestrator.stop()
            self._orchestrator = None
            ui_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await ui_task
            with contextlib.suppress(Exception):
                await event_bus.unsubscribe(queue)
            log.info("Runtime shutdown sequence completed")

    async def _ui_event_loop(self, queue: asyncio.Queue[Any]) -> None:
        log.info("UI event coalescer loop started")
        pending: dict[str, dict[str, Any]] = {}
        window_seconds = settings.ui_coalesce_window_ms / 1000.0
        max_batch = settings.ui_coalesce_max_batch
        last_flush = datetime.now()

        while True:
            timeout = window_seconds if pending else None
            try:
                if timeout is None:
                    event = await queue.get()
                else:
                    event = await asyncio.wait_for(queue.get(), timeout=timeout)
            except asyncio.TimeoutError:
                self._flush_pending(pending)
                last_flush = datetime.now()
                continue

            if isinstance(event, SnapshotReady):
                self._flush_pending(pending)
                log.info(
                    "UI snapshot event trays=%s assembly=%s queue=%s precharge=%s",
                    len(event.trays),
                    len(event.assembly_trolleys),
                    len(event.queue_trolleys),
                    len(event.precharge_trolleys),
                )
                self._bridge.event_ready.emit(
                    {
                        "type": "snapshot",
                        "trays": [tray.to_dict() for tray in event.trays],
                        "assembly_trolleys": [t.to_dict() for t in event.assembly_trolleys],
                        "queue_trolleys": [t.to_dict() for t in event.queue_trolleys],
                        "precharge_trolleys": [t.to_dict() for t in event.precharge_trolleys],
                        "assembly_ungrouped": [tray.to_dict() for tray in event.assembly_ungrouped],
                    }
                )
                last_flush = datetime.now()
                continue

            if isinstance(event, TrayUpdated):
                row = event.tray.to_dict()
                pending[row["tray_id"]] = row
                if len(pending) >= max_batch:
                    log.debug("UI pending tray batch reached max_batch=%s", max_batch)
                    self._flush_pending(pending)
                    last_flush = datetime.now()
                continue

            if isinstance(event, TrolleyUpdated):
                self._flush_pending(pending)
                log.info(
                    "UI projection event assembly=%s queue=%s precharge=%s ungrouped=%s",
                    len(event.assembly_trolleys),
                    len(event.queue_trolleys),
                    len(event.precharge_trolleys),
                    len(event.assembly_ungrouped),
                )
                self._bridge.event_ready.emit(
                    {
                        "type": "projection",
                        "assembly_trolleys": [t.to_dict() for t in event.assembly_trolleys],
                        "queue_trolleys": [t.to_dict() for t in event.queue_trolleys],
                        "precharge_trolleys": [t.to_dict() for t in event.precharge_trolleys],
                        "assembly_ungrouped": [tray.to_dict() for tray in event.assembly_ungrouped],
                    }
                )
                last_flush = datetime.now()
                continue

            if pending and (datetime.now() - last_flush).total_seconds() >= window_seconds:
                self._flush_pending(pending)
                last_flush = datetime.now()

    def _flush_pending(self, pending: dict[str, dict[str, Any]]) -> None:
        if not pending:
            return
        rows = list(pending.values())
        rows.sort(
            key=lambda item: (
                item.get("latest_collected_time") is not None,
                item.get("latest_collected_time"),
                item.get("tray_id"),
            ),
            reverse=True,
        )
        pending.clear()
        log.debug("UI flush pending tray updates count=%s", len(rows))
        self._bridge.event_ready.emit({"type": "trays_delta", "rows": rows})

    async def _manual_refresh(self, *, full_scan: bool) -> dict[str, object]:
        if self._orchestrator is None:
            raise RuntimeError("Runtime is not started")
        result = await self._orchestrator.manual_refresh(full_scan=full_scan)
        log.info("Manual refresh completed full_scan=%s result=%s", full_scan, result)
        return result

    async def _manual_group(self, *, tray_id: str, trolley_id: str, column: Column) -> None:
        if self._orchestrator is None:
            raise RuntimeError("Runtime is not started")
        await self._orchestrator.group_tray_manual(tray_id=tray_id, trolley_id=trolley_id, column=column)
        log.info("Manual group completed tray_id=%s trolley_id=%s column=%s", tray_id, trolley_id, column.value)

    async def _manual_group_many(self, *, tray_ids: list[str], trolley_id: str, column: Column) -> dict[str, object]:
        if self._orchestrator is None:
            raise RuntimeError("Runtime is not started")
        result = await self._orchestrator.group_trays_manual(
            tray_ids=tray_ids,
            trolley_id=trolley_id,
            column=column,
        )
        log.info(
            "Manual group many completed tray_count=%s trolley_id=%s column=%s result=%s",
            len(tray_ids),
            trolley_id,
            column.value,
            result,
        )
        return result

    async def _manual_ungroup(self, *, tray_id: str) -> None:
        if self._orchestrator is None:
            raise RuntimeError("Runtime is not started")
        await self._orchestrator.ungroup_tray_manual(tray_id=tray_id)
        log.info("Manual ungroup completed tray_id=%s", tray_id)

    async def _rename_trolley(self, *, old_trolley_id: str, new_trolley_id: str) -> dict[str, object]:
        if self._orchestrator is None:
            raise RuntimeError("Runtime is not started")
        result = await self._orchestrator.rename_trolley_manual(old_trolley_id, new_trolley_id)
        log.info("Rename trolley completed old=%s new=%s result=%s", old_trolley_id, new_trolley_id, result)
        return result

    async def _clear_trolley(self, *, trolley_id: str) -> dict[str, object]:
        if self._orchestrator is None:
            raise RuntimeError("Runtime is not started")
        result = await self._orchestrator.clear_trolley_manual(trolley_id)
        log.info("Clear trolley completed trolley_id=%s result=%s", trolley_id, result)
        return result

    async def _delete_trolley(self, *, trolley_id: str) -> dict[str, object]:
        if self._orchestrator is None:
            raise RuntimeError("Runtime is not started")
        result = await self._orchestrator.delete_trolley_manual(trolley_id)
        log.info("Delete trolley completed trolley_id=%s result=%s", trolley_id, result)
        return result

    async def _set_auto_group_enabled(self, *, enabled: bool) -> dict[str, object]:
        if self._orchestrator is None:
            raise RuntimeError("Runtime is not started")
        result = await self._orchestrator.set_auto_group_enabled(enabled=enabled)
        log.info("Set auto group completed enabled=%s result=%s", enabled, result)
        return result

    async def _update_grouping_settings(self, *, max_trays_per_trolley: int) -> dict[str, object]:
        if self._orchestrator is None:
            raise RuntimeError("Runtime is not started")
        result = await self._orchestrator.update_grouping_settings(
            max_trays_per_trolley=max_trays_per_trolley,
        )
        log.info("Update grouping settings completed result=%s", result)
        return result


class _ColumnCard(QWidget):
    def __init__(
        self,
        title: str,
        trolley_model,
        *,
        tray_model=None,
        tray_title: str | None = None,
        tray_actions_widget: QWidget | None = None,
    ) -> None:
        super().__init__()
        root = QVBoxLayout(self)
        root.setContentsMargins(0, 0, 0, 0)
        root.setSpacing(6)

        heading = QLabel(title, self)
        heading.setAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter)
        heading.setStyleSheet("font-weight: 600;")
        root.addWidget(heading)

        self._stats = QLabel("", self)
        self._stats.setStyleSheet("color: #666; font-size: 12px;")
        root.addWidget(self._stats)

        trolley_list = QListView(self)
        trolley_list.setModel(trolley_model)
        trolley_list.setAlternatingRowColors(True)
        root.addWidget(trolley_list, 3)

        if tray_model is not None:
            tray_header = QWidget(self)
            tray_header_layout = QHBoxLayout(tray_header)
            tray_header_layout.setContentsMargins(0, 0, 0, 0)
            tray_header_layout.setSpacing(6)
            tray_heading = QLabel(tray_title or "Ungrouped", tray_header)
            tray_heading.setStyleSheet("font-weight: 500;")
            tray_header_layout.addWidget(tray_heading)
            tray_header_layout.addStretch(1)
            if tray_actions_widget is not None:
                tray_actions_widget.setParent(tray_header)
                tray_header_layout.addWidget(tray_actions_widget)
            root.addWidget(tray_header)

            tray_table = QTableView(self)
            tray_table.setModel(tray_model)
            tray_table.setAlternatingRowColors(True)
            tray_table.setSelectionBehavior(QTableView.SelectionBehavior.SelectRows)
            tray_table.verticalHeader().setVisible(False)
            tray_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
            tray_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)
            tray_table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)
            tray_table.horizontalHeader().setSectionResizeMode(3, QHeaderView.ResizeMode.ResizeToContents)
            tray_table.horizontalHeader().setSectionResizeMode(7, QHeaderView.ResizeMode.ResizeToContents)
            root.addWidget(tray_table, 2)

    def set_stats_text(self, text: str) -> None:
        self._stats.setText(text)


class _SettingsDialog(QDialog):
    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self.setWindowTitle("Settings")
        self.setModal(True)
        self.resize(480, 320)

        root = QVBoxLayout(self)
        form = QFormLayout()
        form.setFieldGrowthPolicy(QFormLayout.FieldGrowthPolicy.ExpandingFieldsGrow)

        self._host_input = QLineEdit(self)
        self._host_input.setText(settings.sql_server)
        form.addRow("DB Host", self._host_input)

        self._user_input = QLineEdit(self)
        self._user_input.setText(settings.sql_user)
        form.addRow("DB User", self._user_input)

        self._pass_input = QLineEdit(self)
        self._pass_input.setText(settings.sql_password)
        self._pass_input.setEchoMode(QLineEdit.EchoMode.Password)
        form.addRow("DB Password", self._pass_input)

        self._max_trays_input = QSpinBox(self)
        self._max_trays_input.setRange(1, 500)
        self._max_trays_input.setValue(settings.trolley_max_trays)
        form.addRow("Max Tray / Trolley", self._max_trays_input)

        self._target_aging_input = QDoubleSpinBox(self)
        self._target_aging_input.setRange(0.1, 72.0)
        self._target_aging_input.setDecimals(1)
        self._target_aging_input.setSingleStep(0.5)
        self._target_aging_input.setValue(settings.target_aging_hours)
        self._target_aging_input.setSuffix(" h")
        form.addRow("Target Aging", self._target_aging_input)

        self._aging_tolerance_input = QDoubleSpinBox(self)
        self._aging_tolerance_input.setRange(0.0, 24.0)
        self._aging_tolerance_input.setDecimals(1)
        self._aging_tolerance_input.setSingleStep(0.5)
        self._aging_tolerance_input.setValue(settings.target_aging_tolerance_hours)
        self._aging_tolerance_input.setSuffix(" h")
        form.addRow("Aging Tolerance (+/-)", self._aging_tolerance_input)
        root.addLayout(form)

        hint = QLabel(
            "Database settings are saved immediately and applied after app restart.",
            self,
        )
        hint.setStyleSheet("color: #666; font-size: 12px;")
        root.addWidget(hint)

        button_box = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Save | QDialogButtonBox.StandardButton.Cancel,
            parent=self,
        )
        button_box.accepted.connect(self.accept)
        button_box.rejected.connect(self.reject)
        root.addWidget(button_box)

    def payload(self) -> dict[str, object]:
        return {
            "sql_server": self._host_input.text().strip(),
            "sql_user": self._user_input.text().strip(),
            "sql_password": self._pass_input.text(),
            "trolley_max_trays": int(self._max_trays_input.value()),
            "target_aging_hours": float(self._target_aging_input.value()),
            "target_aging_tolerance_hours": float(self._aging_tolerance_input.value()),
        }


class _MainWindow(QMainWindow):
    def __init__(self, vm: BoardViewModel, runtime: _Runtime) -> None:
        super().__init__()
        self._vm = vm
        self._runtime = runtime
        self._action_bridge = _UiActionBridge()
        self._action_inflight = False
        self._pending_db_restart_notice = False
        self.setWindowTitle("WIP Management - Clean Architecture")
        self.resize(1400, 820)

        root = QWidget(self)
        layout = QVBoxLayout(root)
        layout.setContentsMargins(12, 12, 12, 12)
        layout.setSpacing(10)

        self._summary = QLabel("Loading...", self)
        self._summary.setAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter)
        layout.addWidget(self._summary)

        controls = QWidget(self)
        controls_layout = QHBoxLayout(controls)
        controls_layout.setContentsMargins(0, 0, 0, 0)
        controls_layout.setSpacing(8)

        self._quick_refresh_btn = QPushButton("Quick Refresh", self)
        self._full_refresh_btn = QPushButton("Full Refresh", self)
        self._auto_group_checkbox = QCheckBox("Auto Group", self)
        self._auto_group_checkbox.setChecked(settings.auto_group_default_enabled)
        self._settings_btn = QPushButton("Settings", self)
        self._status = QLabel("", self)

        controls_layout.addWidget(self._quick_refresh_btn)
        controls_layout.addWidget(self._full_refresh_btn)
        controls_layout.addWidget(self._auto_group_checkbox)
        controls_layout.addWidget(self._settings_btn)
        controls_layout.addWidget(self._status, 1)
        layout.addWidget(controls)

        trolley_controls = QWidget(self)
        trolley_controls_layout = QHBoxLayout(trolley_controls)
        trolley_controls_layout.setContentsMargins(0, 0, 0, 0)
        trolley_controls_layout.setSpacing(8)
        trolley_controls_layout.addWidget(QLabel("Trolley", self))
        self._trolley_selector = QComboBox(self)
        self._trolley_selector.setEditable(True)
        self._trolley_selector.setMinimumWidth(220)
        self._new_trolley_input = QLineEdit(self)
        self._new_trolley_input.setPlaceholderText("New Trolley ID")
        self._edit_trolley_btn = QPushButton("Edit Trolley", self)
        self._clear_trolley_btn = QPushButton("Clear Trolley", self)
        self._delete_trolley_btn = QPushButton("Delete Trolley", self)
        trolley_controls_layout.addWidget(self._trolley_selector)
        trolley_controls_layout.addWidget(self._new_trolley_input)
        trolley_controls_layout.addWidget(self._edit_trolley_btn)
        trolley_controls_layout.addWidget(self._clear_trolley_btn)
        trolley_controls_layout.addWidget(self._delete_trolley_btn)
        trolley_controls_layout.addStretch(1)
        layout.addWidget(trolley_controls)

        self._select_all_btn = QPushButton("Select All", self)
        self._unselect_all_btn = QPushButton("Unselect All", self)
        self._add_trolley_input = QLineEdit(self)
        self._add_trolley_input.setPlaceholderText("Trolley ID")
        self._add_trolley_btn = QPushButton("Add Trolley", self)
        ungroup_actions = QWidget(self)
        ungroup_actions_layout = QHBoxLayout(ungroup_actions)
        ungroup_actions_layout.setContentsMargins(0, 0, 0, 0)
        ungroup_actions_layout.setSpacing(6)
        ungroup_actions_layout.addWidget(self._select_all_btn)
        ungroup_actions_layout.addWidget(self._unselect_all_btn)
        ungroup_actions_layout.addStretch(1)
        ungroup_actions_layout.addWidget(self._add_trolley_input)
        ungroup_actions_layout.addWidget(self._add_trolley_btn)

        board = QWidget(self)
        board_layout = QHBoxLayout(board)
        board_layout.setContentsMargins(0, 0, 0, 0)
        board_layout.setSpacing(10)

        self._assembly_card = _ColumnCard(
            "Assembly",
            vm.assembly_trolley_model,
            tray_model=vm.assembly_ungrouped_model,
            tray_title="Ungroup Trays",
            tray_actions_widget=ungroup_actions,
        )
        self._queue_card = _ColumnCard("Queue", vm.queue_trolley_model)
        self._precharge_card = _ColumnCard("Precharge", vm.precharge_trolley_model)
        board_layout.addWidget(self._assembly_card, 1)
        board_layout.addWidget(self._queue_card, 1)
        board_layout.addWidget(self._precharge_card, 1)
        layout.addWidget(board, 1)
        self.setCentralWidget(root)

        vm.updated.connect(self._on_updated)
        self._quick_refresh_btn.clicked.connect(self._on_quick_refresh)
        self._full_refresh_btn.clicked.connect(self._on_full_refresh)
        self._select_all_btn.clicked.connect(self._on_select_all)
        self._unselect_all_btn.clicked.connect(self._on_unselect_all)
        self._add_trolley_btn.clicked.connect(self._on_add_trolley)
        self._edit_trolley_btn.clicked.connect(self._on_edit_trolley)
        self._clear_trolley_btn.clicked.connect(self._on_clear_trolley)
        self._delete_trolley_btn.clicked.connect(self._on_delete_trolley)
        self._settings_btn.clicked.connect(self._on_settings)
        self._auto_group_checkbox.toggled.connect(self._on_auto_group_toggled)
        self._action_bridge.action_done.connect(self._on_action_done)

    def _on_updated(self, payload: dict[str, int]) -> None:
        self._summary.setText(
            "Tray={tray_count} | Group={group_count} | Assembly trolley={assembly_trolley_count} "
            "| Queue trolley={queue_trolley_count} | Precharge trolley={precharge_trolley_count} "
            "| Assembly ungroup={assembly_ungroup_count}".format(
                **payload
            )
        )
        self._assembly_card.set_stats_text(
            "Trolley={assembly_trolley_count} | Tray={assembly_tray_count} | Cell={assembly_cell_count} | Ungroup={assembly_ungroup_count}".format(
                **payload
            )
        )
        self._queue_card.set_stats_text(
            "Trolley={queue_trolley_count} | Tray={queue_tray_count} | Cell={queue_cell_count}".format(
                **payload
            )
        )
        self._precharge_card.set_stats_text(
            "Trolley={precharge_trolley_count} | Tray={precharge_tray_count} | Cell={precharge_cell_count}".format(
                **payload
            )
        )
        self._populate_trolley_selector()

    def _on_quick_refresh(self) -> None:
        self._run_action(
            action_name="Quick refresh",
            action=lambda: self._runtime.manual_refresh(full_scan=False),
        )

    def _on_full_refresh(self) -> None:
        self._run_action(
            action_name="Full refresh",
            action=lambda: self._runtime.manual_refresh(full_scan=True),
        )

    def _on_select_all(self) -> None:
        self._vm.assembly_ungrouped_model.set_all_checked(True)

    def _on_unselect_all(self) -> None:
        self._vm.assembly_ungrouped_model.set_all_checked(False)

    def _on_add_trolley(self) -> None:
        tray_ids = self._vm.assembly_ungrouped_model.checked_tray_ids()
        if not tray_ids:
            self._status.setText("Please check at least one tray in Ungroup list.")
            return
        trolley_id = self._add_trolley_input.text().strip()
        if not trolley_id:
            self._status.setText("Please input trolley ID for Add Trolley.")
            return
        self._run_action(
            action_name="Add trolley",
            action=lambda: self._runtime.manual_group_many(
                tray_ids=tray_ids,
                trolley_id=trolley_id,
                column=Column.QUEUE,
            ),
        )

    def _on_edit_trolley(self) -> None:
        old_trolley_id = self._current_trolley_id()
        if not old_trolley_id:
            self._status.setText("Please choose trolley ID to edit.")
            return
        new_trolley_id = self._new_trolley_input.text().strip()
        if not new_trolley_id:
            self._status.setText("Please input new trolley ID.")
            return
        self._run_action(
            action_name="Edit trolley",
            action=lambda: self._runtime.rename_trolley(
                old_trolley_id=old_trolley_id,
                new_trolley_id=new_trolley_id,
            ),
        )

    def _on_clear_trolley(self) -> None:
        trolley_id = self._current_trolley_id()
        if not trolley_id:
            self._status.setText("Please choose trolley ID to clear.")
            return
        self._run_action(
            action_name="Clear trolley",
            action=lambda: self._runtime.clear_trolley(trolley_id=trolley_id),
        )

    def _on_delete_trolley(self) -> None:
        trolley_id = self._current_trolley_id()
        if not trolley_id:
            self._status.setText("Please choose trolley ID to delete.")
            return
        self._run_action(
            action_name="Delete trolley",
            action=lambda: self._runtime.delete_trolley(trolley_id=trolley_id),
        )

    def _on_auto_group_toggled(self, checked: bool) -> None:
        self._run_action(
            action_name="Auto group",
            action=lambda: self._runtime.set_auto_group_enabled(checked),
        )

    def _on_settings(self) -> None:
        dialog = _SettingsDialog(self)
        if dialog.exec() != QDialog.DialogCode.Accepted:
            return
        payload = dialog.payload()
        sql_server = str(payload["sql_server"]).strip()
        sql_user = str(payload["sql_user"]).strip()
        sql_password = str(payload["sql_password"])
        if not sql_server or not sql_user:
            self._status.setText("DB host/user cannot be empty.")
            return

        db_changed = (
            sql_server != settings.sql_server
            or sql_user != settings.sql_user
            or sql_password != settings.sql_password
        )
        settings.sql_server = sql_server
        settings.sql_user = sql_user
        settings.sql_password = sql_password
        settings.trolley_max_trays = int(payload["trolley_max_trays"])
        settings.target_aging_hours = float(payload["target_aging_hours"])
        settings.target_aging_tolerance_hours = float(payload["target_aging_tolerance_hours"])

        try:
            _upsert_env_values(
                Path.cwd() / ".env",
                {
                    "SQL_SERVER": settings.sql_server,
                    "SQL_USER": settings.sql_user,
                    "SQL_PASSWORD": settings.sql_password,
                    "TROLLEY_MAX_TRAYS": str(settings.trolley_max_trays),
                    "TARGET_AGING_HOURS": str(settings.target_aging_hours),
                    "TARGET_AGING_TOLERANCE_HOURS": str(settings.target_aging_tolerance_hours),
                },
            )
        except Exception as exc:  # noqa: BLE001
            QMessageBox.critical(self, "Settings", f"Failed to save .env settings: {exc}")
            return

        self._pending_db_restart_notice = db_changed
        self._run_action(
            action_name="Apply settings",
            action=lambda: self._runtime.update_grouping_settings(
                max_trays_per_trolley=settings.trolley_max_trays,
            ),
        )

    def _populate_trolley_selector(self) -> None:
        current_text = self._trolley_selector.currentText().strip()
        trolley_ids = sorted(
            {
                *self._vm.assembly_trolley_model.trolley_ids(),
                *self._vm.queue_trolley_model.trolley_ids(),
                *self._vm.precharge_trolley_model.trolley_ids(),
            }
        )
        self._trolley_selector.blockSignals(True)
        self._trolley_selector.clear()
        self._trolley_selector.addItems(trolley_ids)
        if current_text:
            self._trolley_selector.setEditText(current_text)
        elif trolley_ids:
            self._trolley_selector.setCurrentIndex(0)
        self._trolley_selector.blockSignals(False)

    def _current_trolley_id(self) -> str:
        return self._trolley_selector.currentText().strip()

    def _run_action(self, *, action_name: str, action: Callable[[], Any]) -> None:
        if self._action_inflight:
            self._status.setText("Another action is running. Please wait.")
            return
        self._action_inflight = True
        self._set_controls_enabled(False)
        self._status.setText(f"{action_name} running...")
        log.info("UI action started action=%s", action_name)

        def _worker() -> None:
            try:
                result = action()
                self._action_bridge.action_done.emit(action_name, result, None)
            except Exception as exc:  # noqa: BLE001
                self._action_bridge.action_done.emit(action_name, None, str(exc))

        threading.Thread(
            target=_worker,
            name=f"ui-action-{action_name.replace(' ', '-').lower()}",
            daemon=True,
        ).start()

    def _on_action_done(self, action_name: str, result: object, error: object) -> None:
        self._action_inflight = False
        self._set_controls_enabled(True)
        if error:
            message = f"{action_name} failed: {error}"
            self._status.setText(message)
            log.error("UI action failed action=%s error=%s", action_name, error)
            if action_name == "Apply settings":
                self._pending_db_restart_notice = False
            return
        if result is None:
            message = f"{action_name} completed."
        else:
            message = f"{action_name} completed: {result}"
        if action_name == "Add trolley":
            self._vm.assembly_ungrouped_model.clear_checked()
        if action_name == "Apply settings" and self._pending_db_restart_notice:
            message = f"{message} Restart app to apply DB host/user/password."
            self._pending_db_restart_notice = False
        self._status.setText(message)
        log.info("UI action completed action=%s", action_name)

    def _set_controls_enabled(self, enabled: bool) -> None:
        self._quick_refresh_btn.setEnabled(enabled)
        self._full_refresh_btn.setEnabled(enabled)
        self._auto_group_checkbox.setEnabled(enabled)
        self._settings_btn.setEnabled(enabled)
        self._select_all_btn.setEnabled(enabled)
        self._unselect_all_btn.setEnabled(enabled)
        self._add_trolley_input.setEnabled(enabled)
        self._add_trolley_btn.setEnabled(enabled)
        self._trolley_selector.setEnabled(enabled)
        self._new_trolley_input.setEnabled(enabled)
        self._edit_trolley_btn.setEnabled(enabled)
        self._clear_trolley_btn.setEnabled(enabled)
        self._delete_trolley_btn.setEnabled(enabled)


def _upsert_env_values(path: Path, values: dict[str, str]) -> None:
    normalized = {key.strip(): value for key, value in values.items() if key.strip()}
    if not normalized:
        return
    existing_lines: list[str] = []
    if path.exists():
        existing_lines = path.read_text(encoding="utf-8").splitlines()
    updated_lines: list[str] = []
    seen_keys: set[str] = set()
    for line in existing_lines:
        raw = line.strip()
        if not raw or raw.startswith("#") or "=" not in line:
            updated_lines.append(line)
            continue
        key, _, _ = line.partition("=")
        env_key = key.strip()
        if env_key in normalized:
            updated_lines.append(f"{env_key}={normalized[env_key]}")
            seen_keys.add(env_key)
        else:
            updated_lines.append(line)
    for env_key, value in normalized.items():
        if env_key in seen_keys:
            continue
        updated_lines.append(f"{env_key}={value}")
    path.write_text("\n".join(updated_lines) + "\n", encoding="utf-8")


def main() -> int:
    _configure_logging()
    log.info("Application starting pid=%s python=%s", os.getpid(), sys.version.split()[0])
    app = QApplication(sys.argv)

    vm = BoardViewModel()
    bridge = _UiBridge()
    bridge.event_ready.connect(vm.on_event)

    runtime = _Runtime(bridge=bridge)
    runtime.start()

    window = _MainWindow(vm=vm, runtime=runtime)
    window.show()
    try:
        exit_code = app.exec()
        log.info("Qt event loop exited code=%s", exit_code)
        return exit_code
    finally:
        runtime.stop()
        log.info("Application stopped")


if __name__ == "__main__":
    raise SystemExit(main())
