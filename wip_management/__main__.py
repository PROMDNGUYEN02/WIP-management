from __future__ import annotations

import asyncio
import contextlib
import logging
import sys
import threading
from datetime import datetime
from typing import Any

from PySide6.QtCore import QObject, Qt, Signal
from PySide6.QtWidgets import (
    QApplication,
    QComboBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QListView,
    QMainWindow,
    QPushButton,
    QVBoxLayout,
    QWidget,
)

from wip_management.application.services.orchestrator import OrchestratorService
from wip_management.application.state.state_store import SingleWriterStateStore
from wip_management.config import settings
from wip_management.domain.events import SnapshotReady, TrolleyUpdated, TrayUpdated
from wip_management.domain.models.trolley import Column
from wip_management.infrastructure.messaging.event_bus import AsyncEventBus
from wip_management.infrastructure.sqlserver.ccu_repo import CcuRepo
from wip_management.infrastructure.sqlserver.connection import SQLServerConnection
from wip_management.infrastructure.sqlserver.delta_tracker import InMemoryDeltaTracker
from wip_management.infrastructure.sqlserver.fpc_repo import FpcRepo
from wip_management.presentation.ui.viewmodels import BoardViewModel

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger(__name__)


class _SystemClock:
    def now(self) -> datetime:
        return datetime.now()


class _UiBridge(QObject):
    event_ready = Signal(object)


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
        if self._thread is not None and self._thread.is_alive():
            return
        self._started.clear()
        self._start_error = None
        self._thread = threading.Thread(target=self._thread_main, name="wip-runtime", daemon=True)
        self._thread.start()
        if not self._started.wait(timeout=20):
            raise TimeoutError("Timed out while bootstrapping runtime thread")
        if self._start_error is not None:
            raise RuntimeError("Failed to start runtime") from self._start_error

    def stop(self, timeout: float = 20.0) -> None:
        if self._thread is None:
            return
        if self._loop is not None and self._stop_event is not None:
            self._loop.call_soon_threadsafe(self._stop_event.set)
        self._thread.join(timeout=timeout)
        self._thread = None

    def manual_refresh(self, *, full_scan: bool) -> dict[str, object]:
        return self._submit(self._manual_refresh(full_scan=full_scan))

    def manual_group(self, tray_id: str, trolley_id: str, column: Column) -> None:
        self._submit(self._manual_group(tray_id=tray_id, trolley_id=trolley_id, column=column))

    def manual_ungroup(self, tray_id: str) -> None:
        self._submit(self._manual_ungroup(tray_id=tray_id))

    def _submit(self, coroutine: Any) -> Any:
        if self._loop is None:
            raise RuntimeError("Runtime loop is not available")
        future = asyncio.run_coroutine_threadsafe(coroutine, self._loop)
        return future.result(timeout=300)

    def _thread_main(self) -> None:
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        try:
            self._loop.run_until_complete(self._run())
        except BaseException as exc:  # noqa: BLE001
            self._start_error = exc
            log.exception("Runtime crashed")
            self._started.set()
        finally:
            self._loop.close()
            self._loop = None

    async def _run(self) -> None:
        connection = SQLServerConnection()
        ccu_repo = CcuRepo(connection)
        fpc_repo = FpcRepo(connection)
        delta_tracker = InMemoryDeltaTracker()
        event_bus = AsyncEventBus(default_queue_size=settings.event_queue_size)
        state_store = SingleWriterStateStore(queue_size=settings.event_queue_size)

        orchestrator = OrchestratorService(
            ccu_repo=ccu_repo,
            fpc_repo=fpc_repo,
            delta_tracker=delta_tracker,
            event_bus=event_bus,
            state_store=state_store,
            clock=_SystemClock(),
            initial_load_start_hour=settings.initial_load_start_hour,
            delta_poll_interval_seconds=settings.delta_poll_interval_seconds,
            backfill_cooldown_seconds=settings.ccu_backfill_cooldown_seconds,
            max_parallel_workers=settings.max_parallel_workers,
            snapshot_limit=settings.ui_snapshot_limit,
            max_trays_per_trolley=settings.trolley_max_trays,
            assembly_auto_trolley_count=settings.assembly_auto_trolley_count,
        )
        self._orchestrator = orchestrator

        queue = await event_bus.subscribe(maxsize=settings.event_queue_size)
        self._stop_event = asyncio.Event()
        ui_task = asyncio.create_task(self._ui_event_loop(queue), name="ui-event-coalescer")
        self._started.set()
        try:
            await orchestrator.start()
            await self._stop_event.wait()
        finally:
            with contextlib.suppress(Exception):
                await orchestrator.stop()
            self._orchestrator = None
            ui_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await ui_task
            with contextlib.suppress(Exception):
                await event_bus.unsubscribe(queue)

    async def _ui_event_loop(self, queue: asyncio.Queue[Any]) -> None:
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
                    self._flush_pending(pending)
                    last_flush = datetime.now()
                continue

            if isinstance(event, TrolleyUpdated):
                self._flush_pending(pending)
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
        self._bridge.event_ready.emit({"type": "trays_delta", "rows": rows})

    async def _manual_refresh(self, *, full_scan: bool) -> dict[str, object]:
        if self._orchestrator is None:
            raise RuntimeError("Runtime is not started")
        return await self._orchestrator.manual_refresh(full_scan=full_scan)

    async def _manual_group(self, *, tray_id: str, trolley_id: str, column: Column) -> None:
        if self._orchestrator is None:
            raise RuntimeError("Runtime is not started")
        await self._orchestrator.group_tray_manual(tray_id=tray_id, trolley_id=trolley_id, column=column)

    async def _manual_ungroup(self, *, tray_id: str) -> None:
        if self._orchestrator is None:
            raise RuntimeError("Runtime is not started")
        await self._orchestrator.ungroup_tray_manual(tray_id=tray_id)


class _ColumnCard(QWidget):
    def __init__(
        self,
        title: str,
        trolley_model,
        *,
        tray_model=None,
        tray_title: str | None = None,
    ) -> None:
        super().__init__()
        root = QVBoxLayout(self)
        root.setContentsMargins(0, 0, 0, 0)
        root.setSpacing(6)

        heading = QLabel(title, self)
        heading.setAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter)
        heading.setStyleSheet("font-weight: 600;")
        root.addWidget(heading)

        trolley_list = QListView(self)
        trolley_list.setModel(trolley_model)
        trolley_list.setAlternatingRowColors(True)
        root.addWidget(trolley_list, 3)

        if tray_model is not None:
            tray_heading = QLabel(tray_title or "Ungrouped", self)
            tray_heading.setStyleSheet("font-weight: 500;")
            root.addWidget(tray_heading)
            tray_list = QListView(self)
            tray_list.setModel(tray_model)
            tray_list.setAlternatingRowColors(True)
            root.addWidget(tray_list, 2)


class _MainWindow(QMainWindow):
    def __init__(self, vm: BoardViewModel, runtime: _Runtime) -> None:
        super().__init__()
        self._vm = vm
        self._runtime = runtime
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
        self._tray_input = QLineEdit(self)
        self._tray_input.setPlaceholderText("Tray ID")
        self._trolley_input = QLineEdit(self)
        self._trolley_input.setPlaceholderText("Trolley ID")
        self._column_combo = QComboBox(self)
        self._column_combo.addItems([Column.ASSEMBLY.value, Column.QUEUE.value, Column.PRECHARGE.value])
        self._group_btn = QPushButton("Manual Group", self)
        self._ungroup_btn = QPushButton("Ungroup Tray", self)
        self._status = QLabel("", self)

        controls_layout.addWidget(self._quick_refresh_btn)
        controls_layout.addWidget(self._full_refresh_btn)
        controls_layout.addWidget(self._tray_input)
        controls_layout.addWidget(self._trolley_input)
        controls_layout.addWidget(self._column_combo)
        controls_layout.addWidget(self._group_btn)
        controls_layout.addWidget(self._ungroup_btn)
        controls_layout.addWidget(self._status, 1)
        layout.addWidget(controls)

        board = QWidget(self)
        board_layout = QHBoxLayout(board)
        board_layout.setContentsMargins(0, 0, 0, 0)
        board_layout.setSpacing(10)

        board_layout.addWidget(
            _ColumnCard(
                "Assembly",
                vm.assembly_trolley_model,
                tray_model=vm.assembly_ungrouped_model,
                tray_title="Ungrouped Trays",
            ),
            1,
        )
        board_layout.addWidget(_ColumnCard("Queue", vm.queue_trolley_model), 1)
        board_layout.addWidget(_ColumnCard("Precharge", vm.precharge_trolley_model), 1)
        layout.addWidget(board, 1)
        self.setCentralWidget(root)

        vm.updated.connect(self._on_updated)
        self._quick_refresh_btn.clicked.connect(self._on_quick_refresh)
        self._full_refresh_btn.clicked.connect(self._on_full_refresh)
        self._group_btn.clicked.connect(self._on_group)
        self._ungroup_btn.clicked.connect(self._on_ungroup)

    def _on_updated(self, payload: dict[str, int]) -> None:
        self._summary.setText(
            "Trays={all_trays} | Grouped={grouped_trays} | Assembly trolley={assembly_trolleys} "
            "| Queue trolley={queue_trolleys} | Precharge trolley={precharge_trolleys} | Assembly ungroup={assembly_ungrouped}".format(
                **payload
            )
        )

    def _on_quick_refresh(self) -> None:
        try:
            result = self._runtime.manual_refresh(full_scan=False)
            self._status.setText(f"Quick refresh: {result}")
        except Exception as exc:  # noqa: BLE001
            self._status.setText(f"Quick refresh failed: {exc}")

    def _on_full_refresh(self) -> None:
        try:
            result = self._runtime.manual_refresh(full_scan=True)
            self._status.setText(f"Full refresh: {result}")
        except Exception as exc:  # noqa: BLE001
            self._status.setText(f"Full refresh failed: {exc}")

    def _on_group(self) -> None:
        tray_id = self._tray_input.text().strip()
        trolley_id = self._trolley_input.text().strip()
        if not tray_id or not trolley_id:
            self._status.setText("Manual group requires tray ID and trolley ID.")
            return
        column_text = self._column_combo.currentText()
        column = Column(column_text)
        try:
            self._runtime.manual_group(tray_id=tray_id, trolley_id=trolley_id, column=column)
            self._status.setText(f"Grouped {tray_id} -> {trolley_id} ({column.value})")
        except Exception as exc:  # noqa: BLE001
            self._status.setText(f"Manual group failed: {exc}")

    def _on_ungroup(self) -> None:
        tray_id = self._tray_input.text().strip()
        if not tray_id:
            self._status.setText("Ungroup requires tray ID.")
            return
        try:
            self._runtime.manual_ungroup(tray_id=tray_id)
            self._status.setText(f"Ungrouped {tray_id}")
        except Exception as exc:  # noqa: BLE001
            self._status.setText(f"Ungroup failed: {exc}")


def main() -> int:
    app = QApplication(sys.argv)

    vm = BoardViewModel()
    bridge = _UiBridge()
    bridge.event_ready.connect(vm.on_event)

    runtime = _Runtime(bridge=bridge)
    runtime.start()

    window = _MainWindow(vm=vm, runtime=runtime)
    window.show()
    try:
        return app.exec()
    finally:
        runtime.stop()


if __name__ == "__main__":
    raise SystemExit(main())
