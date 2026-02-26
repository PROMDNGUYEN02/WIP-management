from __future__ import annotations

import asyncio
import contextlib
import faulthandler
import logging
import logging.handlers
import os
from pathlib import Path
import sys
import threading
from datetime import datetime, timedelta
from typing import Any, Callable

from PySide6.QtCore import QEvent, QObject, QPoint, Qt, Signal
from PySide6.QtWidgets import (
    QAbstractItemView,
    QApplication,
    QCheckBox,
    QDialog,
    QDialogButtonBox,
    QDoubleSpinBox,
    QFrame,
    QFormLayout,
    QHeaderView,
    QHBoxLayout,
    QInputDialog,
    QLabel,
    QLineEdit,
    QListView,
    QMainWindow,
    QMenu,
    QMessageBox,
    QPushButton,
    QSpinBox,
    QTableView,
    QTableWidget,
    QTableWidgetItem,
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
from wip_management.presentation.ui.viewmodels import BoardViewModel, TrolleyListModel
from wip_management.presentation.ui.mapper import parse_datetime

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


def _install_crash_hooks() -> None:
    def _global_excepthook(exc_type, exc_value, exc_traceback) -> None:
        log.critical("Unhandled exception on main thread", exc_info=(exc_type, exc_value, exc_traceback))

    def _thread_excepthook(args) -> None:
        log.critical(
            "Unhandled exception on thread=%s",
            args.thread.name if args.thread else "<unknown>",
            exc_info=(args.exc_type, args.exc_value, args.exc_traceback),
        )

    sys.excepthook = _global_excepthook
    threading.excepthook = _thread_excepthook
    with contextlib.suppress(Exception):
        faulthandler.enable(all_threads=True)


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

    def tray_cells(self, tray_id: str) -> list[dict[str, str | None]]:
        log.info("Tray detail requested tray_id=%s", tray_id)
        return self._submit(self._tray_cells(tray_id=tray_id))

    def cell_owner(self, cell_id: str) -> dict[str, str | None] | None:
        log.info("Cell owner requested cell_id=%s", cell_id)
        return self._submit(self._cell_owner(cell_id=cell_id))

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

    async def _tray_cells(self, *, tray_id: str) -> list[dict[str, str | None]]:
        if self._orchestrator is None:
            raise RuntimeError("Runtime is not started")
        rows = await self._orchestrator.fetch_tray_cells(tray_id=tray_id)
        return rows

    async def _cell_owner(self, *, cell_id: str) -> dict[str, str | None] | None:
        if self._orchestrator is None:
            raise RuntimeError("Runtime is not started")
        row = await self._orchestrator.fetch_cell_owner(cell_id=cell_id)
        return row


class _MetricBox(QFrame):
    def __init__(self, title: str, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self.setObjectName("metricBox")
        layout = QVBoxLayout(self)
        layout.setContentsMargins(12, 10, 12, 10)
        layout.setSpacing(2)

        self._title = QLabel(title, self)
        self._title.setObjectName("metricTitle")
        self._value = QLabel("0", self)
        self._value.setObjectName("metricValue")

        layout.addWidget(self._title)
        layout.addWidget(self._value)

    def set_value(self, value: int) -> None:
        self._value.setText(str(value))


class _ColumnCard(QWidget):
    def __init__(
        self,
        title: str,
        trolley_model,
        *,
        tray_model=None,
        tray_title: str | None = None,
        tray_actions_widget: QWidget | None = None,
        on_trolley_context: Callable[[QListView, QModelIndex], None] | None = None,
        on_trolley_double_click: Callable[[QListView, QModelIndex], None] | None = None,
        on_tray_header_click: Callable[[int], None] | None = None,
    ) -> None:
        super().__init__()
        self.setObjectName("columnCard")
        self.tray_table: QTableView | None = None
        self.trolley_list = QListView(self)
        self._on_trolley_context_cb = on_trolley_context
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

        self.trolley_list.setModel(trolley_model)
        self.trolley_list.setAlternatingRowColors(True)
        self.trolley_list.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        root.addWidget(self.trolley_list, 3)
        if on_trolley_context is not None:
            self.trolley_list.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
            self.trolley_list.customContextMenuRequested.connect(self._handle_trolley_context)
        if on_trolley_double_click is not None:
            self.trolley_list.doubleClicked.connect(
                lambda idx: on_trolley_double_click(self.trolley_list, idx),
            )

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

            self.tray_table = QTableView(self)
            self.tray_table.setModel(tray_model)
            self.tray_table.setAlternatingRowColors(True)
            self.tray_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
            self.tray_table.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)
            self.tray_table.setEditTriggers(
                QAbstractItemView.EditTrigger.CurrentChanged
                | QAbstractItemView.EditTrigger.SelectedClicked
                | QAbstractItemView.EditTrigger.DoubleClicked
            )
            self.tray_table.verticalHeader().setVisible(False)
            self.tray_table.verticalHeader().setDefaultSectionSize(34)
            header = self.tray_table.horizontalHeader()
            header.setSectionsClickable(True)
            header.setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
            header.setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)
            header.setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)
            header.setSectionResizeMode(3, QHeaderView.ResizeMode.ResizeToContents)
            header.setSectionResizeMode(8, QHeaderView.ResizeMode.ResizeToContents)
            header.setDefaultAlignment(Qt.AlignmentFlag.AlignCenter | Qt.AlignmentFlag.AlignVCenter)
            table_font = self.tray_table.font()
            table_font.setPointSize(max(table_font.pointSize(), 11))
            self.tray_table.setFont(table_font)
            header.setFont(table_font)
            self.tray_table.setColumnWidth(0, 52)
            if on_tray_header_click is not None:
                header.sectionClicked.connect(on_tray_header_click)
            self.tray_table.clicked.connect(self._on_tray_table_clicked)
            self.tray_table.setShowGrid(False)
            root.addWidget(self.tray_table, 2)

    def set_stats_text(self, text: str) -> None:
        self._stats.setText(text)

    def _handle_trolley_context(self, pos: QPoint) -> None:
        if self._on_trolley_context_cb is None:
            return
        index = self.trolley_list.indexAt(pos)
        if not index.isValid():
            return
        self._on_trolley_context_cb(self.trolley_list, index)

    def _on_tray_table_clicked(self, index: QModelIndex) -> None:
        if self.tray_table is None or not index.isValid():
            return
        self.tray_table.selectRow(index.row())
        model = self.tray_table.model()
        if model is None:
            return
        # Click circle column toggles; click any other cell force-selects the row.
        if index.column() == 0:
            toggle_row = getattr(model, "toggle_row", None)
            if callable(toggle_row):
                toggle_row(index.row())
                return
        else:
            set_checked = getattr(model, "set_row_checked", None)
            if callable(set_checked):
                set_checked(index.row(), True)
                return
        checked_now = model.data(index, Qt.ItemDataRole.CheckStateRole) == Qt.CheckState.Checked
        target = Qt.CheckState.Unchecked if checked_now else Qt.CheckState.Checked
        model.setData(index, target, Qt.ItemDataRole.CheckStateRole)


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


class _TrayDetailBridge(QObject):
    loaded = Signal(str, int, object, object)


class _TrolleyDetailDialog(QDialog):
    def __init__(self, runtime: _Runtime, trolley_id: str, tray_rows: list[dict[str, str]], parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self._runtime = runtime
        self._bridge = _TrayDetailBridge()
        self._active_tray_id = ""
        self._active_request_id = 0
        self._request_seq = 0
        self._loading = False
        self._pending_tray_id: str | None = None
        self._tray_cells_cache: dict[str, list[dict[str, str | None]]] = {}
        self._bridge.loaded.connect(self._on_cells_loaded)

        self.setWindowTitle(f"Trolley Detail - {trolley_id}")
        self.resize(920, 620)

        root = QVBoxLayout(self)
        root.setContentsMargins(10, 10, 10, 10)
        root.setSpacing(8)

        self._summary_label = QLabel(f"Trolley: {trolley_id}", self)
        self._summary_label.setStyleSheet("font-weight: 600;")
        root.addWidget(self._summary_label)

        self._summary_table = QTableWidget(self)
        self._summary_table.setColumnCount(5)
        self._summary_table.setHorizontalHeaderLabels(
            ["Tray_ID", "Start_Time", "End_Time", "Aging_Time", "Status"],
        )
        self._summary_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self._summary_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self._summary_table.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        self._summary_table.verticalHeader().setVisible(False)
        self._summary_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        root.addWidget(self._summary_table, 3)

        self._cell_label = QLabel("Cell Detail", self)
        self._cell_label.setStyleSheet("font-weight: 600;")
        root.addWidget(self._cell_label)

        self._cell_table = QTableWidget(self)
        self._cell_table.setColumnCount(3)
        self._cell_table.setHorizontalHeaderLabels(["Cell_ID", "Start_Time", "End_Time"])
        self._cell_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self._cell_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self._cell_table.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        self._cell_table.verticalHeader().setVisible(False)
        self._cell_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        root.addWidget(self._cell_table, 2)

        self._status = QLabel("", self)
        root.addWidget(self._status)

        self._fill_summary(tray_rows)
        self._summary_table.itemSelectionChanged.connect(self._on_summary_selection_changed)
        if self._summary_table.rowCount() > 0:
            self._summary_table.selectRow(0)

    def _fill_summary(self, tray_rows: list[dict[str, str]]) -> None:
        self._summary_table.setRowCount(len(tray_rows))
        for row_idx, row in enumerate(tray_rows):
            tray_item = QTableWidgetItem(row.get("tray_id", ""))
            tray_item.setData(Qt.ItemDataRole.UserRole, row.get("tray_id", ""))
            self._summary_table.setItem(row_idx, 0, tray_item)
            self._summary_table.setItem(row_idx, 1, QTableWidgetItem(row.get("start_time", "-")))
            self._summary_table.setItem(row_idx, 2, QTableWidgetItem(row.get("end_time", "-")))
            self._summary_table.setItem(row_idx, 3, QTableWidgetItem(row.get("aging_time", "-")))
            self._summary_table.setItem(row_idx, 4, QTableWidgetItem(row.get("status", "-")))

    def _on_summary_selection_changed(self) -> None:
        selected = self._summary_table.selectedItems()
        if not selected:
            return
        row = selected[0].row()
        item = self._summary_table.item(row, 0)
        if item is None:
            return
        tray_id = str(item.data(Qt.ItemDataRole.UserRole) or "").strip()
        if not tray_id:
            return
        if self._loading and tray_id == self._active_tray_id:
            return
        if self._pending_tray_id == tray_id:
            return
        cached = self._tray_cells_cache.get(tray_id)
        if cached is not None:
            self._active_tray_id = tray_id
            self._render_cells(tray_id, cached)
            self._status.setText(f"Loaded {len(cached)} cells for tray {tray_id} (cache)")
            return
        if self._loading:
            self._pending_tray_id = tray_id
            self._status.setText(f"Queued cell detail for tray {tray_id} ...")
            return
        self._start_tray_cells_query(tray_id)

    def _start_tray_cells_query(self, tray_id: str) -> None:
        self._request_seq += 1
        request_id = self._request_seq
        self._active_request_id = request_id
        self._active_tray_id = tray_id
        self._loading = True
        self._pending_tray_id = None
        self._status.setText(f"Loading cell detail for tray {tray_id} ...")
        self._cell_table.setRowCount(0)

        def _worker() -> None:
            try:
                rows = self._runtime.tray_cells(tray_id)
                self._bridge.loaded.emit(tray_id, request_id, rows, None)
            except Exception as exc:  # noqa: BLE001
                self._bridge.loaded.emit(tray_id, request_id, None, str(exc))

        threading.Thread(
            target=_worker,
            name=f"ui-tray-detail-{tray_id}-{request_id}",
            daemon=True,
        ).start()

    def _render_cells(self, tray_id: str, rows: list[dict[str, str | None]]) -> None:
        self._cell_table.setRowCount(len(rows))
        for row_idx, row_data in enumerate(rows):
            self._cell_table.setItem(row_idx, 0, QTableWidgetItem(str(row_data.get("cell_id", ""))))
            self._cell_table.setItem(row_idx, 1, QTableWidgetItem(str(row_data.get("start_time") or "-")))
            self._cell_table.setItem(row_idx, 2, QTableWidgetItem(str(row_data.get("end_time") or "-")))
        self._status.setText(f"Loaded {len(rows)} cells for tray {tray_id}")

    def _on_cells_loaded(self, tray_id: str, request_id: int, rows: object, error: object) -> None:
        if request_id != self._active_request_id:
            return
        self._loading = False
        if error:
            self._status.setText(f"Detail load failed: {error}")
            pending = self._pending_tray_id
            self._pending_tray_id = None
            if pending and pending != tray_id:
                self._start_tray_cells_query(pending)
            return
        values = list(rows or [])
        self._tray_cells_cache[tray_id] = [dict(item) for item in values]
        self._render_cells(tray_id, self._tray_cells_cache[tray_id])
        pending = self._pending_tray_id
        self._pending_tray_id = None
        if pending and pending != tray_id:
            self._start_tray_cells_query(pending)


class _MainWindow(QMainWindow):
    def __init__(self, vm: BoardViewModel, runtime: _Runtime) -> None:
        super().__init__()
        self._vm = vm
        self._runtime = runtime
        self._action_bridge = _UiActionBridge()
        self._action_inflight = False
        self._pending_db_restart_notice = False
        self._event_filter_installed = False
        self.setWindowTitle("WIP Management - Clean Architecture")
        self.resize(1400, 820)
        self._apply_styles()
        app = QApplication.instance()
        if app is not None:
            app.installEventFilter(self)
            self._event_filter_installed = True

        root = QWidget(self)
        layout = QVBoxLayout(root)
        layout.setContentsMargins(12, 12, 12, 12)
        layout.setSpacing(12)

        summary_row = QWidget(self)
        summary_layout = QHBoxLayout(summary_row)
        summary_layout.setContentsMargins(0, 0, 0, 0)
        summary_layout.setSpacing(8)
        self._metric_boxes = {
            "tray_count": _MetricBox("Total Tray", self),
            "group_count": _MetricBox("Group", self),
            "assembly_ungroup_count": _MetricBox("Ungroup", self),
            "assembly_trolley_count": _MetricBox("Assembly Trolley", self),
            "queue_trolley_count": _MetricBox("Queue Trolley", self),
            "precharge_trolley_count": _MetricBox("Precharge Trolley", self),
        }
        for key in (
            "tray_count",
            "group_count",
            "assembly_ungroup_count",
            "assembly_trolley_count",
            "queue_trolley_count",
            "precharge_trolley_count",
        ):
            summary_layout.addWidget(self._metric_boxes[key], 1)
        layout.addWidget(summary_row)

        controls = QWidget(self)
        controls_layout = QHBoxLayout(controls)
        controls_layout.setContentsMargins(0, 0, 0, 0)
        controls_layout.setSpacing(8)

        self._quick_refresh_btn = QPushButton("Quick Refresh", self)
        self._full_refresh_btn = QPushButton("Full Refresh", self)
        self._auto_group_checkbox = QCheckBox("Auto Group", self)
        self._auto_group_checkbox.setChecked(settings.auto_group_default_enabled)
        self._settings_btn = QPushButton("Settings", self)
        self._search_input = QLineEdit(self)
        self._search_input.setPlaceholderText("Find Tray_ID / Cell_ID")
        self._search_input.setMinimumWidth(220)
        self._search_btn = QPushButton("Search", self)
        self._search_result = QLabel("-", self)
        self._search_result.setWordWrap(True)
        self._search_result.setStyleSheet("color: #334155;")
        self._pending_search_query = ""
        controls_layout.addWidget(self._search_input, 2)
        controls_layout.addWidget(self._search_btn)
        controls_layout.addWidget(self._search_result, 3)
        controls_layout.addStretch(1)
        controls_layout.addWidget(self._quick_refresh_btn)
        controls_layout.addWidget(self._full_refresh_btn)
        controls_layout.addWidget(self._auto_group_checkbox)
        controls_layout.addWidget(self._settings_btn)
        layout.addWidget(controls)

        self._status = QLabel("", self)
        self._status.setObjectName("statusBar")
        layout.addWidget(self._status)

        self._add_trolley_input = QLineEdit(self)
        self._add_trolley_input.setPlaceholderText("Trolley ID")
        self._add_trolley_btn = QPushButton("Add Trolley", self)
        ungroup_actions = QWidget(self)
        ungroup_actions_layout = QHBoxLayout(ungroup_actions)
        ungroup_actions_layout.setContentsMargins(0, 0, 0, 0)
        ungroup_actions_layout.setSpacing(6)
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
            on_trolley_context=self._on_trolley_item_context,
            on_trolley_double_click=self._on_trolley_item_double_click,
            on_tray_header_click=self._on_ungroup_header_clicked,
        )
        self._queue_card = _ColumnCard(
            "Queue",
            vm.queue_trolley_model,
            on_trolley_context=self._on_trolley_item_context,
            on_trolley_double_click=self._on_trolley_item_double_click,
        )
        self._precharge_card = _ColumnCard(
            "Precharge",
            vm.precharge_trolley_model,
            on_trolley_context=self._on_trolley_item_context,
            on_trolley_double_click=self._on_trolley_item_double_click,
        )
        board_layout.addWidget(self._assembly_card, 1)
        board_layout.addWidget(self._queue_card, 1)
        board_layout.addWidget(self._precharge_card, 1)
        layout.addWidget(board, 1)
        self.setCentralWidget(root)

        vm.updated.connect(self._on_updated)
        self._quick_refresh_btn.clicked.connect(self._on_quick_refresh)
        self._full_refresh_btn.clicked.connect(self._on_full_refresh)
        self._add_trolley_btn.clicked.connect(self._on_add_trolley)
        self._search_btn.clicked.connect(self._on_search)
        self._search_input.returnPressed.connect(self._on_search)
        self._settings_btn.clicked.connect(self._on_settings)
        self._auto_group_checkbox.toggled.connect(self._on_auto_group_toggled)
        self._action_bridge.action_done.connect(self._on_action_done)

    def _on_updated(self, payload: dict[str, int]) -> None:
        for key, box in self._metric_boxes.items():
            box.set_value(int(payload.get(key, 0)))
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

    def _on_search(self) -> None:
        raw = self._search_input.text().strip()
        if not raw:
            self._search_result.setText("-")
            self._status.setText("Search input is empty.")
            return
        lookup = raw.upper()
        tray_payload = self._vm.tray_payload_by_id(lookup)
        if tray_payload is not None:
            self._search_result.setText(self._format_tray_search_result(lookup, tray_payload))
            self._status.setText(f"Found tray {lookup}.")
            return
        self._pending_search_query = lookup
        self._search_result.setText(f"Searching {lookup} ...")
        self._run_action(
            action_name="Search",
            action=lambda: self._runtime.cell_owner(lookup),
        )

    def _format_tray_search_result(self, tray_id: str, tray_payload: dict[str, Any]) -> str:
        location, trolley_id = self._find_tray_location(tray_id)
        ccu_payload = tray_payload.get("ccu_payload") or {}
        fpc_payload = tray_payload.get("fpc_payload") or {}
        start_dt = parse_datetime(ccu_payload.get("start_time")) or parse_datetime(fpc_payload.get("precharge_start_time"))
        end_dt = parse_datetime(ccu_payload.get("end_time")) or parse_datetime(fpc_payload.get("precharge_end_time"))
        qty_raw = ccu_payload.get("quantity") if ccu_payload else 0
        try:
            qty = int(str(qty_raw).strip()) if qty_raw is not None else 0
        except Exception:  # noqa: BLE001
            qty = 0
        aging_text = "-"
        if end_dt is not None:
            ref_dt = datetime.now()
            precharge_start_dt = parse_datetime(fpc_payload.get("precharge_start_time"))
            if ccu_payload and fpc_payload and precharge_start_dt is not None:
                ref_dt = precharge_start_dt
            aging_text = _format_timedelta(ref_dt - end_dt)
        start_text = start_dt.isoformat(sep=" ", timespec="seconds") if start_dt else "-"
        end_text = end_dt.isoformat(sep=" ", timespec="seconds") if end_dt else "-"
        trolley_text = trolley_id if trolley_id else "-"
        return (
            f"Tray_ID {tray_id} | Trolley {trolley_text} | {location} | "
            f"Qty {qty} | Start {start_text} | End {end_text} | Aging {aging_text}"
        )

    def _format_cell_search_result(self, cell_id: str, payload: dict[str, str | None]) -> str:
        tray_id = str(payload.get("tray_id") or "").strip()
        if not tray_id:
            return f"Cell {cell_id} not found."
        location, trolley_id = self._find_tray_location(tray_id)
        trolley_text = trolley_id if trolley_id else "-"
        start_text = str(payload.get("start_time") or "-")
        end_text = str(payload.get("end_time") or "-")
        return (
            f"Cell_ID {cell_id} | Tray_ID {tray_id} | Trolley {trolley_text} | "
            f"{location} | Start {start_text} | End {end_text}"
        )

    def _find_tray_location(self, tray_id: str) -> tuple[str, str]:
        tray_key = tray_id.strip()
        if not tray_key:
            return "Unknown", ""
        precharge_row = self._vm.precharge_trolley_model.find_by_tray_id(tray_key)
        if precharge_row is not None:
            return "Precharge", precharge_row.trolley_id
        queue_row = self._vm.queue_trolley_model.find_by_tray_id(tray_key)
        if queue_row is not None:
            return "Queue", queue_row.trolley_id
        assembly_row = self._vm.assembly_trolley_model.find_by_tray_id(tray_key)
        if assembly_row is not None:
            return "Assembly", assembly_row.trolley_id
        if self._vm.assembly_ungrouped_model.has_tray(tray_key):
            return "Assembly", ""
        return "Unknown", ""

    def _on_add_trolley(self) -> None:
        tray_ids = self._selected_ungroup_tray_ids()
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

    def _on_ungroup_header_clicked(self, section: int) -> None:
        if section != 0:
            return
        self._vm.assembly_ungrouped_model.toggle_all_checked()

    def _selected_ungroup_tray_ids(self) -> list[str]:
        tray_ids = self._vm.assembly_ungrouped_model.checked_tray_ids()
        if tray_ids:
            return tray_ids
        tray_table = self._assembly_card.tray_table
        if tray_table is None or tray_table.selectionModel() is None:
            return []
        selected_rows = {index.row() for index in tray_table.selectionModel().selectedIndexes()}
        out: list[str] = []
        for row in sorted(selected_rows):
            tray_index = self._vm.assembly_ungrouped_model.index(row, 2)
            tray_id = str(self._vm.assembly_ungrouped_model.data(tray_index, Qt.ItemDataRole.DisplayRole) or "").strip()
            if tray_id:
                out.append(tray_id)
        return out

    def _on_trolley_item_context(self, list_view: QListView, index: QModelIndex) -> None:
        payload = self._trolley_payload_from_index(index)
        if payload is None:
            return
        global_pos = list_view.viewport().mapToGlobal(list_view.visualRect(index).bottomRight())
        self._show_trolley_menu(payload, global_pos)

    def _on_trolley_item_double_click(self, _: QListView, index: QModelIndex) -> None:
        payload = self._trolley_payload_from_index(index)
        if payload is None:
            return
        self._show_trolley_detail_dialog(payload)

    def _trolley_payload_from_index(self, index: QModelIndex) -> dict[str, Any] | None:
        if not index.isValid():
            return None
        model = index.model()
        if model is None:
            return None
        trolley_role = getattr(model, "TrolleyIdRole", TrolleyListModel.TrolleyIdRole)
        tray_ids_role = getattr(model, "TrayIdsRole", TrolleyListModel.TrayIdsRole)
        mode_role = getattr(model, "ModeRole", TrolleyListModel.ModeRole)
        column_role = getattr(model, "ColumnRole", TrolleyListModel.ColumnRole)
        state_role = getattr(model, "StateRole", TrolleyListModel.StateRole)
        trolley_id = str(index.data(trolley_role) or "").strip()
        if not trolley_id:
            return None
        return {
            "trolley_id": trolley_id,
            "tray_ids": list(index.data(tray_ids_role) or []),
            "mode": str(index.data(mode_role) or ""),
            "column": str(index.data(column_role) or ""),
            "state": str(index.data(state_role) or ""),
        }

    def _show_trolley_menu(self, trolley: dict[str, Any], global_pos: QPoint) -> None:
        trolley_id = str(trolley.get("trolley_id", "")).strip()
        if not trolley_id:
            return
        mode = str(trolley.get("mode", "")).strip().lower()
        is_manual = mode == "manual"
        tray_ids = [str(item).strip() for item in trolley.get("tray_ids", []) if str(item).strip()]

        menu = QMenu(self)
        add_action = menu.addAction("Edit Trolley: Add Selected Trays")
        remove_action = menu.addAction("Edit Trolley: Delete Tray...")
        menu.addSeparator()
        clear_action = menu.addAction("Clear Trolley")
        delete_action = menu.addAction("Delete Trolley")

        if not is_manual:
            add_action.setEnabled(False)
            remove_action.setEnabled(False)
            clear_action.setEnabled(False)
            delete_action.setEnabled(False)

        chosen = menu.exec(global_pos)
        if chosen is None:
            return
        if not is_manual:
            self._status.setText("Auto trolley cannot be edited. Please use manual group trolley.")
            return
        if chosen == add_action:
            tray_ids_to_add = self._selected_ungroup_tray_ids()
            if not tray_ids_to_add:
                self._status.setText("Please select trays in Ungroup list first.")
                return
            self._run_action(
                action_name="Edit trolley add trays",
                action=lambda: self._runtime.manual_group_many(
                    tray_ids=tray_ids_to_add,
                    trolley_id=trolley_id,
                    column=Column.QUEUE,
                ),
            )
            return
        if chosen == remove_action:
            if not tray_ids:
                self._status.setText("No tray available in trolley to remove.")
                return
            tray_id, ok = QInputDialog.getItem(
                self,
                "Edit Trolley",
                "Select tray to remove",
                tray_ids,
                0,
                False,
            )
            if not ok or not tray_id:
                return
            selected_tray = str(tray_id).strip()
            self._run_action(
                action_name="Edit trolley delete tray",
                action=lambda: self._runtime.manual_ungroup(tray_id=selected_tray),
            )
            return
        if chosen == clear_action:
            self._run_action(
                action_name="Clear trolley",
                action=lambda: self._runtime.clear_trolley(trolley_id=trolley_id),
            )
            return
        if chosen == delete_action:
            self._run_action(
                action_name="Delete trolley",
                action=lambda: self._runtime.delete_trolley(trolley_id=trolley_id),
            )
            return

    def _show_trolley_detail_dialog(self, trolley: dict[str, Any]) -> None:
        tray_ids = [str(item).strip() for item in trolley.get("tray_ids", []) if str(item).strip()]
        if not tray_ids:
            self._status.setText("Trolley has no tray data to show detail.")
            return
        tray_payloads = self._vm.tray_payloads(tray_ids)
        rows = self._build_tray_summary_rows(tray_payloads)
        if not rows:
            self._status.setText("No tray summary available for selected trolley.")
            return
        dialog = _TrolleyDetailDialog(
            runtime=self._runtime,
            trolley_id=str(trolley.get("trolley_id", "")),
            tray_rows=rows,
            parent=self,
        )
        dialog.exec()

    def _build_tray_summary_rows(self, tray_payloads: list[dict[str, Any]]) -> list[dict[str, str]]:
        now = datetime.now()
        out: list[dict[str, str]] = []
        for row in tray_payloads:
            tray_id = str(row.get("tray_id", "")).strip()
            if not tray_id:
                continue
            ccu_payload = row.get("ccu_payload") or {}
            fpc_payload = row.get("fpc_payload") or {}
            start_dt = parse_datetime(ccu_payload.get("start_time"))
            end_dt = parse_datetime(ccu_payload.get("end_time"))
            precharge_start_dt = parse_datetime(fpc_payload.get("precharge_start_time"))
            if fpc_payload:
                status_text = "Precharged"
                if end_dt is None:
                    aging_text = "-"
                else:
                    ref_time = precharge_start_dt or now
                    aging_text = _format_timedelta(ref_time - end_dt)
            else:
                if end_dt is None:
                    aging_text = "-"
                    status_text = "-"
                else:
                    aging_delta = now - end_dt
                    aging_text = _format_timedelta(aging_delta)
                    status_text = _aging_status_text(aging_delta)
            out.append(
                {
                    "tray_id": tray_id,
                    "start_time": start_dt.isoformat(sep=" ", timespec="seconds") if start_dt else "-",
                    "end_time": end_dt.isoformat(sep=" ", timespec="seconds") if end_dt else "-",
                    "aging_time": aging_text,
                    "status": status_text,
                }
            )
        out.sort(key=lambda item: (item.get("end_time", ""), item.get("tray_id", "")), reverse=True)
        return out

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
        if action_name == "Search":
            query = self._pending_search_query
            self._pending_search_query = ""
            if error:
                self._search_result.setText(f"Search failed: {error}")
                self._status.setText("Search failed.")
                log.error("UI action failed action=%s error=%s", action_name, error)
                return
            owner = dict(result) if isinstance(result, dict) else None
            if owner is None:
                self._search_result.setText(f"No data for {query}.")
                self._status.setText("Search completed.")
                log.info("UI action completed action=%s", action_name)
                return
            self._search_result.setText(self._format_cell_search_result(query, owner))
            self._status.setText("Search completed.")
            log.info("UI action completed action=%s", action_name)
            return
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
        if action_name in {"Add trolley", "Edit trolley add trays"}:
            self._vm.assembly_ungrouped_model.clear_checked()
            self._add_trolley_input.clear()
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
        self._search_input.setEnabled(enabled)
        self._search_btn.setEnabled(enabled)
        self._add_trolley_input.setEnabled(enabled)
        self._add_trolley_btn.setEnabled(enabled)
        self._assembly_card.trolley_list.setEnabled(enabled)
        self._queue_card.trolley_list.setEnabled(enabled)
        self._precharge_card.trolley_list.setEnabled(enabled)
        if self._assembly_card.tray_table is not None:
            self._assembly_card.tray_table.setEnabled(enabled)

    def _clear_ungroup_selection(self) -> None:
        tray_table = self._assembly_card.tray_table
        if tray_table is None:
            return
        if tray_table.selectionModel() is not None:
            tray_table.clearSelection()
        self._vm.assembly_ungrouped_model.clear_checked()

    def eventFilter(self, watched: QObject, event: QEvent) -> bool:  # noqa: N802
        if event.type() == QEvent.Type.MouseButtonPress:
            tray_table = self._assembly_card.tray_table
            if tray_table is not None and tray_table.isVisible():
                global_position_fn = getattr(event, "globalPosition", None)
                if callable(global_position_fn):
                    global_point = global_position_fn().toPoint()
                    target_widget = QApplication.widgetAt(global_point)
                    keep_widgets = {
                        self._add_trolley_btn,
                        self._add_trolley_input,
                        self._search_btn,
                        self._search_input,
                        self._quick_refresh_btn,
                        self._full_refresh_btn,
                        self._auto_group_checkbox,
                        self._settings_btn,
                    }
                    keep_selection = bool(
                        target_widget is not None
                        and any(
                            target_widget is keep_widget or keep_widget.isAncestorOf(target_widget)
                            for keep_widget in keep_widgets
                        )
                    )
                    inside_table = bool(
                        target_widget is not None
                        and (target_widget is tray_table or tray_table.isAncestorOf(target_widget))
                    )
                    if not inside_table and not keep_selection:
                        self._clear_ungroup_selection()
        return super().eventFilter(watched, event)

    def closeEvent(self, event) -> None:  # noqa: N802
        app = QApplication.instance()
        if self._event_filter_installed and app is not None:
            app.removeEventFilter(self)
            self._event_filter_installed = False
        super().closeEvent(event)

    def _apply_styles(self) -> None:
        self.setStyleSheet(
            """
            QMainWindow {
                background: #f3f5fb;
            }
            QFrame#metricBox {
                background: #ffffff;
                border: 1px solid #d8dfec;
                border-radius: 12px;
            }
            QLabel#metricTitle {
                color: #64748b;
                font-size: 12px;
            }
            QLabel#metricValue {
                color: #0f172a;
                font-size: 22px;
                font-weight: 700;
            }
            QWidget#columnCard {
                background: #ffffff;
                border: 1px solid #d8dfec;
                border-radius: 12px;
                padding: 8px;
            }
            QPushButton {
                background: #1d4ed8;
                color: #ffffff;
                border: none;
                border-radius: 8px;
                padding: 6px 12px;
                font-weight: 600;
            }
            QPushButton:hover {
                background: #1e40af;
            }
            QLineEdit {
                background: #ffffff;
                border: 1px solid #c9d3e6;
                border-radius: 8px;
                padding: 6px 8px;
            }
            QLabel#statusBar {
                color: #334155;
                font-size: 12px;
                padding-left: 2px;
            }
            """
        )


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


def _format_timedelta(delta: timedelta) -> str:
    total_seconds = int(delta.total_seconds())
    sign = "-" if total_seconds < 0 else ""
    total_seconds = abs(total_seconds)
    hours, rem = divmod(total_seconds, 3600)
    minutes, seconds = divmod(rem, 60)
    return f"{sign}{hours:02d}:{minutes:02d}:{seconds:02d}"


def _aging_status_text(delta: timedelta) -> str:
    age_hours = max(delta.total_seconds(), 0.0) / 3600.0
    target = max(float(settings.target_aging_hours), 0.0)
    tolerance = max(float(settings.target_aging_tolerance_hours), 0.0)
    min_target = max(target - tolerance, 0.0)
    max_target = target + tolerance
    if age_hours < min_target:
        return "Waiting to send to Precharge"
    if age_hours <= max_target:
        return "Ready to send to Precharge"
    return "Exceed Aging Time"


def main() -> int:
    _configure_logging()
    _install_crash_hooks()
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
