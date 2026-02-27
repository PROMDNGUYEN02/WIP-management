from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
import logging
import time
from typing import Any

from PySide6.QtCore import QAbstractListModel, QAbstractTableModel, QModelIndex, QObject, Qt, QTimer, Signal, Slot
from PySide6.QtGui import QBrush, QColor

from wip_management.config import settings
from wip_management.presentation.ui.mapper import parse_datetime

log = logging.getLogger(__name__)

UNGROUP_CHECK_VISUAL_ROLE = int(Qt.ItemDataRole.UserRole) + 101

_AGE_STATE_UNKNOWN = "unknown"
_AGE_STATE_WAITING = "waiting"
_AGE_STATE_READY = "ready"
_AGE_STATE_EXCEED = "exceed"

_AGE_LABEL_WAITING = "Aging"
_AGE_LABEL_READY = "Aged"
_AGE_LABEL_EXCEED = "Aged Out"

_SELECT_ICON_COLOR = QBrush(QColor("#8a949e"))


@dataclass(slots=True, frozen=True)
class TrolleyRowVM:
    trolley_id: str
    column: str
    mode: str
    state: str
    tray_quantity: int
    cell_quantity: int
    tray_ids: list[str]
    aging_state: str
    aging_time: str

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> "TrolleyRowVM":
        tray_ids = [str(item) for item in payload.get("tray_ids", [])]
        return cls(
            trolley_id=str(payload.get("trolley_id", "")),
            column=str(payload.get("column", "")),
            mode=str(payload.get("mode", "auto")),
            state=str(payload.get("state", "Waiting")),
            tray_quantity=_to_int(payload.get("tray_quantity"), default=len(tray_ids)),
            cell_quantity=_to_int(payload.get("cell_quantity"), default=0),
            tray_ids=tray_ids,
            aging_state=str(payload.get("aging_state", _AGE_STATE_UNKNOWN)),
            aging_time=str(payload.get("aging_time", "-")),
        )


@dataclass(slots=True, frozen=True)
class UngroupTrayRowVM:
    no: int
    tray_id: str
    quantity: int
    assembly_out: str
    precharge_in: str
    aging_time: str
    status: str
    location: str


class TrolleyListModel(QAbstractListModel):
    TrolleyIdRole = Qt.ItemDataRole.UserRole + 1
    ColumnRole = Qt.ItemDataRole.UserRole + 2
    ModeRole = Qt.ItemDataRole.UserRole + 3
    TrayCountRole = Qt.ItemDataRole.UserRole + 4
    TrayIdsRole = Qt.ItemDataRole.UserRole + 5
    StateRole = Qt.ItemDataRole.UserRole + 6
    CellCountRole = Qt.ItemDataRole.UserRole + 7
    AgingStateRole = Qt.ItemDataRole.UserRole + 8
    AgingTimeRole = Qt.ItemDataRole.UserRole + 9

    def __init__(self) -> None:
        super().__init__()
        self._rows: list[TrolleyRowVM] = []

    def rowCount(self, parent: QModelIndex = QModelIndex()) -> int:  # noqa: N802
        if parent.isValid():
            return 0
        return len(self._rows)

    def data(self, index: QModelIndex, role: int = Qt.ItemDataRole.DisplayRole) -> Any:  # noqa: N802
        if not index.isValid():
            return None
        row = index.row()
        if row < 0 or row >= len(self._rows):
            return None
        item = self._rows[row]
        if role == Qt.ItemDataRole.DisplayRole:
            if item.column == "Queue" and item.aging_state != _AGE_STATE_UNKNOWN:
                return (
                    f"{item.trolley_id} | {item.state} | age={item.aging_time} | "
                    f"trays={item.tray_quantity} | cells={item.cell_quantity} | {item.mode.upper()}"
                )
            return f"{item.trolley_id} | {item.state} | trays={item.tray_quantity} | cells={item.cell_quantity} | {item.mode.upper()}"
        if role == Qt.ItemDataRole.ToolTipRole and item.column == "Queue":
            return f"Aging: {item.aging_time}"
        if role == Qt.ItemDataRole.BackgroundRole:
            if item.column == "Queue":
                if item.aging_state == _AGE_STATE_WAITING:
                    return QBrush(QColor("#ffedd5"))
                if item.aging_state == _AGE_STATE_READY:
                    return QBrush(QColor("#dcfce7"))
                if item.aging_state == _AGE_STATE_EXCEED:
                    return QBrush(QColor("#fee2e2"))
            return None
        if role == self.TrolleyIdRole:
            return item.trolley_id
        if role == self.ColumnRole:
            return item.column
        if role == self.ModeRole:
            return item.mode
        if role == self.TrayCountRole:
            return item.tray_quantity
        if role == self.TrayIdsRole:
            return item.tray_ids
        if role == self.StateRole:
            return item.state
        if role == self.CellCountRole:
            return item.cell_quantity
        if role == self.AgingStateRole:
            return item.aging_state
        if role == self.AgingTimeRole:
            return item.aging_time
        return None

    def roleNames(self) -> dict[int, bytes]:  # noqa: N802
        return {
            self.TrolleyIdRole: b"trolleyId",
            self.ColumnRole: b"column",
            self.ModeRole: b"mode",
            self.TrayCountRole: b"trayCount",
            self.TrayIdsRole: b"trayIds",
            self.StateRole: b"state",
            self.CellCountRole: b"cellCount",
            self.AgingStateRole: b"agingState",
            self.AgingTimeRole: b"agingTime",
        }

    def replace_all(self, payload_rows: list[dict[str, Any]]) -> None:
        items = [TrolleyRowVM.from_payload(row) for row in payload_rows if row.get("trolley_id")]
        items.sort(key=lambda row: row.trolley_id)
        if items == self._rows:
            return
        self.beginResetModel()
        self._rows = items
        self.endResetModel()

    def all_tray_ids(self) -> set[str]:
        out: set[str] = set()
        for row in self._rows:
            out.update(row.tray_ids)
        return out

    def totals(self) -> tuple[int, int, int]:
        trolley_count = len(self._rows)
        tray_count = sum(row.tray_quantity for row in self._rows)
        cell_count = sum(row.cell_quantity for row in self._rows)
        return trolley_count, tray_count, cell_count

    def trolley_ids(self) -> list[str]:
        return [row.trolley_id for row in self._rows]

    def find_by_tray_id(self, tray_id: str) -> TrolleyRowVM | None:
        wanted = tray_id.strip()
        if not wanted:
            return None
        for row in self._rows:
            if wanted in row.tray_ids:
                return row
        return None


class UngroupTrayTableModel(QAbstractTableModel):
    _headers = ["Select", "#", "Tray_ID", "Q'ty", "Assembly_Out", "Precharge_In", "Aging", "Status", "Location"]

    def __init__(self) -> None:
        super().__init__()
        self._rows: list[UngroupTrayRowVM] = []
        self._checked_tray_ids: set[str] = set()
        self._interaction_hold = False
        self._pending_tray_payload_rows: list[dict[str, Any]] | None = None

    def rowCount(self, parent: QModelIndex = QModelIndex()) -> int:  # noqa: N802
        if parent.isValid():
            return 0
        return len(self._rows)

    def columnCount(self, parent: QModelIndex = QModelIndex()) -> int:  # noqa: N802
        if parent.isValid():
            return 0
        return len(self._headers)

    def data(self, index: QModelIndex, role: int = Qt.ItemDataRole.DisplayRole) -> Any:  # noqa: N802
        if not index.isValid():
            return None
        row = index.row()
        col = index.column()
        if row < 0 or row >= len(self._rows):
            return None
        item = self._rows[row]
        if col == 0:
            if role == Qt.ItemDataRole.DisplayRole:
                return ""
            if role == UNGROUP_CHECK_VISUAL_ROLE:
                return "on" if item.tray_id in self._checked_tray_ids else "off"
            if role == Qt.ItemDataRole.ForegroundRole:
                return _SELECT_ICON_COLOR
            if role == Qt.ItemDataRole.TextAlignmentRole:
                return Qt.AlignmentFlag.AlignCenter | Qt.AlignmentFlag.AlignVCenter
            return None
        if role == Qt.ItemDataRole.TextAlignmentRole:
            return Qt.AlignmentFlag.AlignCenter | Qt.AlignmentFlag.AlignVCenter
        if role != Qt.ItemDataRole.DisplayRole:
            return None
        if col == 1:
            return item.no
        if col == 2:
            return item.tray_id
        if col == 3:
            return item.quantity
        if col == 4:
            return item.assembly_out
        if col == 5:
            return item.precharge_in
        if col == 6:
            return item.aging_time
        if col == 7:
            return item.status
        if col == 8:
            return item.location
        return None

    def flags(self, index: QModelIndex) -> Qt.ItemFlag:  # noqa: N802
        if not index.isValid():
            return Qt.ItemFlag.NoItemFlags
        return Qt.ItemFlag.ItemIsEnabled | Qt.ItemFlag.ItemIsSelectable

    def setData(self, index: QModelIndex, value: Any, role: int = Qt.ItemDataRole.EditRole) -> bool:  # noqa: N802
        if (
            not index.isValid()
            or index.column() != 0
            or role not in {Qt.ItemDataRole.CheckStateRole, Qt.ItemDataRole.EditRole}
        ):
            return False
        row = index.row()
        if row < 0 or row >= len(self._rows):
            return False
        tray_id = self._rows[row].tray_id
        if not tray_id:
            return False
        checked_value = Qt.CheckState.Checked.value
        checked = value in {True, 1, Qt.CheckState.Checked, checked_value}
        if checked:
            self._checked_tray_ids.add(tray_id)
        else:
            self._checked_tray_ids.discard(tray_id)
        self.dataChanged.emit(
            index,
            index,
            [Qt.ItemDataRole.DisplayRole, Qt.ItemDataRole.TextAlignmentRole, UNGROUP_CHECK_VISUAL_ROLE],
        )
        self.headerDataChanged.emit(Qt.Orientation.Horizontal, 0, 0)
        return True

    def headerData(  # noqa: N802
        self,
        section: int,
        orientation: Qt.Orientation,
        role: int = Qt.ItemDataRole.DisplayRole,
    ) -> Any:
        if orientation == Qt.Orientation.Horizontal:
            if section < 0 or section >= len(self._headers):
                return None
            if section == 0:
                if role == Qt.ItemDataRole.DisplayRole:
                    return ""
                if role == UNGROUP_CHECK_VISUAL_ROLE:
                    if not self._rows:
                        return "off"
                    total = len(self._rows)
                    selected = len(self._checked_tray_ids)
                    if selected == 0:
                        return "off"
                    if selected >= total:
                        return "on"
                    return "partial"
                if role == Qt.ItemDataRole.ForegroundRole:
                    return _SELECT_ICON_COLOR
                if role == Qt.ItemDataRole.TextAlignmentRole:
                    return Qt.AlignmentFlag.AlignCenter | Qt.AlignmentFlag.AlignVCenter
                return None
            if role == Qt.ItemDataRole.DisplayRole:
                return self._headers[section]
            if role == Qt.ItemDataRole.TextAlignmentRole:
                return Qt.AlignmentFlag.AlignCenter | Qt.AlignmentFlag.AlignVCenter
            return None
        if role != Qt.ItemDataRole.DisplayRole:
            return None
        return section + 1

    def replace_from_tray_payloads(self, tray_payload_rows: list[dict[str, Any]]) -> None:
        if self._interaction_hold:
            self._pending_tray_payload_rows = list(tray_payload_rows)
            return
        self._apply_tray_payload_rows(tray_payload_rows)

    def set_interaction_hold(self, hold: bool) -> None:
        hold = bool(hold)
        if self._interaction_hold == hold:
            return
        self._interaction_hold = hold
        if hold:
            return
        if self._pending_tray_payload_rows is None:
            return
        pending = self._pending_tray_payload_rows
        self._pending_tray_payload_rows = None
        self._apply_tray_payload_rows(pending)

    def _apply_tray_payload_rows(self, tray_payload_rows: list[dict[str, Any]]) -> None:
        started_at = time.perf_counter()
        now = _coarse_now(step_seconds=30)
        items: list[tuple[datetime | None, UngroupTrayRowVM]] = []
        for row in tray_payload_rows:
            tray_id = str(row.get("tray_id", "")).strip()
            if not tray_id:
                continue
            ccu_payload = row.get("ccu_payload") or {}
            fpc_payload = row.get("fpc_payload") or {}
            has_ccu = bool(ccu_payload)
            has_fpc = bool(fpc_payload)
            if not has_ccu:
                # Ungroup view is CCU-first: hide FPC-only rows to avoid misleading "Precharge" interpretation.
                continue

            quantity = _to_int(
                ccu_payload.get("quantity"),
                default=_to_int(fpc_payload.get("cell_count"), default=0),
            )
            ccu_end_dt = parse_datetime(ccu_payload.get("end_time"))
            end_dt = ccu_end_dt
            raw_precharge_start_dt = parse_datetime(fpc_payload.get("precharge_start_time"))
            precharge_start_dt = raw_precharge_start_dt if (has_ccu and has_fpc) else None

            if has_fpc:
                location = "Precharge"
                status = "Precharged"
                if ccu_end_dt is None or precharge_start_dt is None:
                    aging_text = "-"
                else:
                    aging_text = _format_timedelta(precharge_start_dt - ccu_end_dt)
            else:
                location = "Assembly"
                if end_dt is None:
                    aging_text = "-"
                    status = "-"
                else:
                    aging = now - end_dt
                    aging_text = _format_timedelta(aging)
                    _, status = _aging_state_and_label(aging)
            vm = UngroupTrayRowVM(
                no=0,
                tray_id=tray_id,
                quantity=quantity,
                assembly_out=end_dt.isoformat(sep=" ", timespec="seconds") if end_dt else "-",
                precharge_in=precharge_start_dt.isoformat(sep=" ", timespec="seconds") if precharge_start_dt else "-",
                aging_time=aging_text,
                status=status,
                location=location,
            )
            items.append((end_dt, vm))

        items.sort(key=lambda item: (item[0] or datetime.min, item[1].tray_id), reverse=True)
        ordered: list[UngroupTrayRowVM] = []
        for idx, (_, vm) in enumerate(items, start=1):
            ordered.append(
                UngroupTrayRowVM(
                    no=idx,
                    tray_id=vm.tray_id,
                    quantity=vm.quantity,
                    assembly_out=vm.assembly_out,
                    precharge_in=vm.precharge_in,
                    aging_time=vm.aging_time,
                    status=vm.status,
                    location=vm.location,
                )
            )

        retained = {row.tray_id for row in ordered}
        if self._rows == ordered:
            new_checked = self._checked_tray_ids.intersection(retained)
            if new_checked != self._checked_tray_ids:
                self._checked_tray_ids = set(new_checked)
                if self._rows:
                    top_left = self.index(0, 0)
                    bottom_right = self.index(len(self._rows) - 1, 0)
                    self.dataChanged.emit(
                        top_left,
                        bottom_right,
                        [Qt.ItemDataRole.DisplayRole, Qt.ItemDataRole.TextAlignmentRole],
                    )
                self.headerDataChanged.emit(Qt.Orientation.Horizontal, 0, 0)
            return

        self.beginResetModel()
        self._rows = ordered
        self._checked_tray_ids.intersection_update(retained)
        self.endResetModel()
        elapsed_ms = int((time.perf_counter() - started_at) * 1000)
        if elapsed_ms >= 35:
            log.warning(
                "Ungroup table rebuild slow elapsed_ms=%s input_rows=%s output_rows=%s",
                elapsed_ms,
                len(tray_payload_rows),
                len(ordered),
            )

    def checked_tray_ids(self) -> list[str]:
        ordered: list[str] = []
        for row in self._rows:
            if row.tray_id in self._checked_tray_ids:
                ordered.append(row.tray_id)
        return ordered

    def has_tray(self, tray_id: str) -> bool:
        wanted = tray_id.strip()
        if not wanted:
            return False
        return any(row.tray_id == wanted for row in self._rows)

    def set_row_checked(self, row: int, checked: bool) -> None:
        if row < 0 or row >= len(self._rows):
            return
        tray_id = self._rows[row].tray_id
        if not tray_id:
            return
        index = self.index(row, 0)
        self.setData(
            index,
            Qt.CheckState.Checked if checked else Qt.CheckState.Unchecked,
            Qt.ItemDataRole.CheckStateRole,
        )

    def toggle_row(self, row: int) -> None:
        if row < 0 or row >= len(self._rows):
            return
        tray_id = self._rows[row].tray_id
        if not tray_id:
            return
        next_checked = tray_id not in self._checked_tray_ids
        self.set_row_checked(row, next_checked)

    def set_all_checked(self, checked: bool) -> None:
        if checked:
            self._checked_tray_ids = {row.tray_id for row in self._rows if row.tray_id}
        else:
            self._checked_tray_ids.clear()
        if self._rows:
            top_left = self.index(0, 0)
            bottom_right = self.index(len(self._rows) - 1, 0)
            self.dataChanged.emit(
                top_left,
                bottom_right,
                [Qt.ItemDataRole.DisplayRole, Qt.ItemDataRole.TextAlignmentRole, UNGROUP_CHECK_VISUAL_ROLE],
            )
        self.headerDataChanged.emit(Qt.Orientation.Horizontal, 0, 0)

    def clear_checked(self) -> None:
        self.set_all_checked(False)

    def is_all_checked(self) -> bool:
        if not self._rows:
            return False
        return len(self._checked_tray_ids) >= len(self._rows)

    def toggle_all_checked(self) -> None:
        self.set_all_checked(not self.is_all_checked())


class BoardViewModel(QObject):
    updated = Signal(object)

    def __init__(self) -> None:
        super().__init__()
        self.assembly_trolley_model = TrolleyListModel()
        self.queue_trolley_model = TrolleyListModel()
        self.precharge_trolley_model = TrolleyListModel()
        self.assembly_ungrouped_model = UngroupTrayTableModel()
        self._tray_cache: dict[str, dict[str, Any]] = {}
        self._last_event_seq = 0
        self._rebuild_ungroup_pending = False
        self._last_summary_payload: dict[str, int] | None = None
        self._queued_event_payload: dict[str, Any] | None = None
        self._event_apply_scheduled = False

    @Slot(object)
    def on_event(self, payload: dict[str, Any]) -> None:
        seq = _to_int(payload.get("seq"), default=0)
        if seq > 0 and seq <= self._last_event_seq:
            log.debug(
                "BoardViewModel ignored stale queued event seq=%s last_seq=%s type=%s",
                seq,
                self._last_event_seq,
                payload.get("type"),
            )
            return
        current_queued = self._queued_event_payload
        current_seq = _to_int(current_queued.get("seq"), default=0) if isinstance(current_queued, dict) else 0
        if current_queued is None or seq == 0 or seq >= current_seq:
            self._queued_event_payload = payload
        if self._event_apply_scheduled:
            return
        self._event_apply_scheduled = True
        QTimer.singleShot(16, self._apply_queued_event)

    def _apply_queued_event(self) -> None:
        self._event_apply_scheduled = False
        payload = self._queued_event_payload
        self._queued_event_payload = None
        if payload is None:
            return
        started_at = time.perf_counter()
        seq = _to_int(payload.get("seq"), default=0)
        if seq > 0:
            if seq <= self._last_event_seq:
                log.debug(
                    "BoardViewModel ignored stale event seq=%s last_seq=%s type=%s",
                    seq,
                    self._last_event_seq,
                    payload.get("type"),
                )
                if self._queued_event_payload is not None and not self._event_apply_scheduled:
                    self._event_apply_scheduled = True
                    QTimer.singleShot(16, self._apply_queued_event)
                return
            self._last_event_seq = seq
        event_type = str(payload.get("type", ""))
        log.debug("BoardViewModel event received type=%s seq=%s", event_type, seq)
        if event_type == "snapshot":
            self._apply_snapshot(payload)
        elif event_type == "projection":
            self._apply_projection(payload)
        elif event_type == "trays_delta":
            self._apply_trays_delta(payload.get("rows", []))
        else:
            log.debug("BoardViewModel ignored unknown event type=%s", event_type)
        summary_payload = self.summary()
        if summary_payload != self._last_summary_payload:
            self._last_summary_payload = dict(summary_payload)
            self.updated.emit(summary_payload)
        elapsed_ms = int((time.perf_counter() - started_at) * 1000)
        if elapsed_ms >= 45:
            log.warning(
                "BoardViewModel event slow type=%s seq=%s elapsed_ms=%s",
                event_type,
                seq,
                elapsed_ms,
            )
        if self._queued_event_payload is not None and not self._event_apply_scheduled:
            self._event_apply_scheduled = True
            QTimer.singleShot(16, self._apply_queued_event)

    def summary(self) -> dict[str, int]:
        grouped_ids = self._grouped_tray_ids()
        assembly_ungroup_count = self.assembly_ungrouped_model.rowCount()
        # "Total Tray" should reflect what UI is currently rendering (grouped + ungrouped),
        # not raw tray cache size which can include hidden FPC-only/stale rows.
        total_visible_trays = len(grouped_ids) + assembly_ungroup_count
        asm_trolley, asm_tray, asm_cell = self.assembly_trolley_model.totals()
        que_trolley, que_tray, que_cell = self.queue_trolley_model.totals()
        pre_trolley, pre_tray, pre_cell = self.precharge_trolley_model.totals()
        return {
            "tray_count": total_visible_trays,
            "group_count": len(grouped_ids),
            "assembly_trolley_count": asm_trolley,
            "queue_trolley_count": que_trolley,
            "precharge_trolley_count": pre_trolley,
            "assembly_ungroup_count": assembly_ungroup_count,
            "assembly_tray_count": asm_tray,
            "queue_tray_count": que_tray,
            "precharge_tray_count": pre_tray,
            "assembly_cell_count": asm_cell,
            "queue_cell_count": que_cell,
            "precharge_cell_count": pre_cell,
        }

    def _apply_snapshot(self, payload: dict[str, Any]) -> None:
        tray_rows = payload.get("trays", [])
        self._tray_cache = {str(row["tray_id"]): row for row in tray_rows if row.get("tray_id")}
        self._apply_projection(payload)
        log.info("BoardViewModel snapshot applied trays=%s", len(self._tray_cache))

    def _apply_projection(self, payload: dict[str, Any]) -> None:
        assembly_rows = payload.get("assembly_trolleys", [])
        queue_rows = self._annotate_queue_trolley_rows(payload.get("queue_trolleys", []))
        precharge_rows = payload.get("precharge_trolleys", [])
        self.assembly_trolley_model.replace_all(assembly_rows)
        self.queue_trolley_model.replace_all(queue_rows)
        self.precharge_trolley_model.replace_all(precharge_rows)
        log.debug(
            "BoardViewModel projection applied assembly=%s queue=%s precharge=%s",
            self.assembly_trolley_model.rowCount(),
            self.queue_trolley_model.rowCount(),
            self.precharge_trolley_model.rowCount(),
        )
        if "assembly_ungrouped" in payload:
            raw_ungrouped = payload.get("assembly_ungrouped", [])
            if not isinstance(raw_ungrouped, list):
                raw_ungrouped = []
            ungrouped_rows = [row for row in raw_ungrouped if isinstance(row, dict)]
            for row in ungrouped_rows:
                tray_id = str(row.get("tray_id", "")).strip()
                if tray_id:
                    self._tray_cache[tray_id] = row
            self._rebuild_ungroup_pending = False
            self.assembly_ungrouped_model.replace_from_tray_payloads(ungrouped_rows)
            return
        self._schedule_rebuild_assembly_ungrouped()

    def tray_payload_by_id(self, tray_id: str) -> dict[str, Any] | None:
        return self._tray_cache.get(tray_id)

    def tray_payloads(self, tray_ids: list[str]) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for tray_id in tray_ids:
            row = self._tray_cache.get(tray_id)
            if row is not None:
                out.append(row)
        return out

    def _apply_trays_delta(self, payload_rows: list[dict[str, Any]]) -> None:
        if not payload_rows:
            return
        changed = False
        for row in payload_rows:
            tray_id = str(row.get("tray_id", "")).strip()
            if not tray_id:
                continue
            previous = self._tray_cache.get(tray_id)
            if previous == row:
                continue
            self._tray_cache[tray_id] = row
            changed = True
        if not changed:
            return
        self._schedule_rebuild_assembly_ungrouped()
        log.debug(
            "BoardViewModel trays delta applied rows=%s cache=%s",
            len(payload_rows),
            len(self._tray_cache),
        )

    def _schedule_rebuild_assembly_ungrouped(self) -> None:
        if self._rebuild_ungroup_pending:
            return
        self._rebuild_ungroup_pending = True
        QTimer.singleShot(180, self._flush_rebuild_assembly_ungrouped)

    def _flush_rebuild_assembly_ungrouped(self) -> None:
        if not self._rebuild_ungroup_pending:
            return
        self._rebuild_ungroup_pending = False
        self._rebuild_assembly_ungrouped_from_cache()

    def _rebuild_assembly_ungrouped_from_cache(self) -> None:
        grouped_ids = self._grouped_tray_ids()
        rows = [row for tray_id, row in self._tray_cache.items() if tray_id not in grouped_ids]
        self.assembly_ungrouped_model.replace_from_tray_payloads(rows)

    def _annotate_queue_trolley_rows(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        now = _coarse_now(step_seconds=30)
        out: list[dict[str, Any]] = []
        for row in rows:
            row_copy = dict(row)
            latest_end: datetime | None = None
            tray_ids = [str(item) for item in row.get("tray_ids", [])]
            for tray_id in tray_ids:
                tray_row = self._tray_cache.get(tray_id)
                if tray_row is None:
                    continue
                ccu_payload = tray_row.get("ccu_payload") or {}
                fpc_payload = tray_row.get("fpc_payload") or {}
                end_dt = parse_datetime(ccu_payload.get("end_time")) or parse_datetime(
                    fpc_payload.get("precharge_end_time"),
                )
                if end_dt is None:
                    continue
                if latest_end is None or end_dt > latest_end:
                    latest_end = end_dt
            if latest_end is None:
                row_copy["aging_state"] = _AGE_STATE_UNKNOWN
                row_copy["aging_time"] = "-"
            else:
                aging = now - latest_end
                state, _ = _aging_state_and_label(aging)
                row_copy["aging_state"] = state
                row_copy["aging_time"] = _format_timedelta(aging)
            out.append(row_copy)
        return out

    def _grouped_tray_ids(self) -> set[str]:
        return {
            *self.assembly_trolley_model.all_tray_ids(),
            *self.queue_trolley_model.all_tray_ids(),
            *self.precharge_trolley_model.all_tray_ids(),
        }


def _to_int(value: Any, *, default: int) -> int:
    if value is None:
        return default
    if isinstance(value, int):
        return value
    try:
        return int(str(value).strip())
    except Exception:  # noqa: BLE001
        return default


def _format_timedelta(delta: timedelta) -> str:
    total_seconds = max(0, int(delta.total_seconds()))
    hours, rem = divmod(total_seconds, 3600)
    minutes, seconds = divmod(rem, 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


def _coarse_now(*, step_seconds: int) -> datetime:
    step = max(1, int(step_seconds))
    now = datetime.now()
    floored_second = (now.second // step) * step
    return now.replace(second=floored_second, microsecond=0)


def _aging_state_and_label(delta: timedelta) -> tuple[str, str]:
    age_hours = max(delta.total_seconds(), 0.0) / 3600.0
    target = max(float(settings.target_aging_hours), 0.0)
    tolerance = max(float(settings.target_aging_tolerance_hours), 0.0)
    min_target = max(target - tolerance, 0.0)
    max_target = target + tolerance
    if age_hours < min_target:
        return _AGE_STATE_WAITING, _AGE_LABEL_WAITING
    if age_hours <= max_target:
        return _AGE_STATE_READY, _AGE_LABEL_READY
    return _AGE_STATE_EXCEED, _AGE_LABEL_EXCEED
