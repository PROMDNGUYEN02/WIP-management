from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
import logging
from typing import Any

from PySide6.QtCore import QAbstractListModel, QAbstractTableModel, QModelIndex, QObject, Qt, Signal, Slot

from wip_management.presentation.ui.mapper import parse_datetime

log = logging.getLogger(__name__)


@dataclass(slots=True, frozen=True)
class TrolleyRowVM:
    trolley_id: str
    column: str
    mode: str
    state: str
    tray_quantity: int
    cell_quantity: int
    tray_ids: list[str]

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
        )


@dataclass(slots=True, frozen=True)
class UngroupTrayRowVM:
    no: int
    tray_id: str
    quantity: int
    start_time: str
    end_time: str
    aging_time: str
    location: str


class TrolleyListModel(QAbstractListModel):
    TrolleyIdRole = Qt.ItemDataRole.UserRole + 1
    ColumnRole = Qt.ItemDataRole.UserRole + 2
    ModeRole = Qt.ItemDataRole.UserRole + 3
    TrayCountRole = Qt.ItemDataRole.UserRole + 4
    TrayIdsRole = Qt.ItemDataRole.UserRole + 5
    StateRole = Qt.ItemDataRole.UserRole + 6
    CellCountRole = Qt.ItemDataRole.UserRole + 7

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
            return (
                f"{item.trolley_id} | {item.state} | trays={item.tray_quantity} | "
                f"cells={item.cell_quantity} | {item.mode.upper()}"
            )
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
        }

    def replace_all(self, payload_rows: list[dict[str, Any]]) -> None:
        items = [TrolleyRowVM.from_payload(row) for row in payload_rows if row.get("trolley_id")]
        items.sort(key=lambda row: row.trolley_id)
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


class UngroupTrayTableModel(QAbstractTableModel):
    _headers = ["Select", "No", "Tray_ID", "Quantity", "Start_Time", "End_Time", "Aging_Time", "Location"]

    def __init__(self) -> None:
        super().__init__()
        self._rows: list[UngroupTrayRowVM] = []
        self._checked_tray_ids: set[str] = set()

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
        if col == 0 and role == Qt.ItemDataRole.CheckStateRole:
            return (
                Qt.CheckState.Checked
                if item.tray_id in self._checked_tray_ids
                else Qt.CheckState.Unchecked
            )
        if role != Qt.ItemDataRole.DisplayRole:
            return None
        if col == 0:
            return ""
        if col == 1:
            return item.no
        if col == 2:
            return item.tray_id
        if col == 3:
            return item.quantity
        if col == 4:
            return item.start_time
        if col == 5:
            return item.end_time
        if col == 6:
            return item.aging_time
        if col == 7:
            return item.location
        return None

    def flags(self, index: QModelIndex) -> Qt.ItemFlag:  # noqa: N802
        if not index.isValid():
            return Qt.ItemFlag.NoItemFlags
        base = Qt.ItemFlag.ItemIsEnabled | Qt.ItemFlag.ItemIsSelectable
        if index.column() == 0:
            return base | Qt.ItemFlag.ItemIsUserCheckable
        return base

    def setData(self, index: QModelIndex, value: Any, role: int = Qt.ItemDataRole.EditRole) -> bool:  # noqa: N802
        if (
            not index.isValid()
            or index.column() != 0
            or role != Qt.ItemDataRole.CheckStateRole
        ):
            return False
        row = index.row()
        if row < 0 or row >= len(self._rows):
            return False
        tray_id = self._rows[row].tray_id
        if not tray_id:
            return False
        if value == Qt.CheckState.Checked:
            self._checked_tray_ids.add(tray_id)
        else:
            self._checked_tray_ids.discard(tray_id)
        self.dataChanged.emit(index, index, [Qt.ItemDataRole.CheckStateRole])
        return True

    def headerData(  # noqa: N802
        self,
        section: int,
        orientation: Qt.Orientation,
        role: int = Qt.ItemDataRole.DisplayRole,
    ) -> Any:
        if role != Qt.ItemDataRole.DisplayRole:
            return None
        if orientation == Qt.Orientation.Horizontal:
            if section < 0 or section >= len(self._headers):
                return None
            return self._headers[section]
        return section + 1

    def replace_from_tray_payloads(self, tray_payload_rows: list[dict[str, Any]]) -> None:
        now = datetime.now()
        items: list[tuple[datetime | None, UngroupTrayRowVM]] = []
        for row in tray_payload_rows:
            tray_id = str(row.get("tray_id", "")).strip()
            if not tray_id:
                continue
            ccu_payload = row.get("ccu_payload") or {}
            fpc_payload = row.get("fpc_payload") or {}

            quantity = _to_int(ccu_payload.get("quantity"), default=0)
            start_dt = parse_datetime(ccu_payload.get("start_time"))
            end_dt = parse_datetime(ccu_payload.get("end_time"))
            precharge_start_dt = parse_datetime(fpc_payload.get("precharge_start_time"))

            if end_dt is None:
                aging_text = "-"
            else:
                ref_time = now
                if ccu_payload and fpc_payload and precharge_start_dt is not None:
                    ref_time = precharge_start_dt
                aging = ref_time - end_dt
                aging_text = _format_timedelta(aging)

            location = "Assembly" if ccu_payload and (not fpc_payload) else "Precharge"
            vm = UngroupTrayRowVM(
                no=0,
                tray_id=tray_id,
                quantity=quantity,
                start_time=start_dt.isoformat(sep=" ", timespec="seconds") if start_dt else "-",
                end_time=end_dt.isoformat(sep=" ", timespec="seconds") if end_dt else "-",
                aging_time=aging_text,
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
                    start_time=vm.start_time,
                    end_time=vm.end_time,
                    aging_time=vm.aging_time,
                    location=vm.location,
                )
            )

        retained = {row.tray_id for row in ordered}
        self.beginResetModel()
        self._rows = ordered
        self._checked_tray_ids.intersection_update(retained)
        self.endResetModel()

    def checked_tray_ids(self) -> list[str]:
        ordered: list[str] = []
        for row in self._rows:
            if row.tray_id in self._checked_tray_ids:
                ordered.append(row.tray_id)
        return ordered

    def set_all_checked(self, checked: bool) -> None:
        if checked:
            self._checked_tray_ids = {row.tray_id for row in self._rows if row.tray_id}
        else:
            self._checked_tray_ids.clear()
        if self._rows:
            top_left = self.index(0, 0)
            bottom_right = self.index(len(self._rows) - 1, 0)
            self.dataChanged.emit(top_left, bottom_right, [Qt.ItemDataRole.CheckStateRole])

    def clear_checked(self) -> None:
        self.set_all_checked(False)


class BoardViewModel(QObject):
    updated = Signal(object)

    def __init__(self) -> None:
        super().__init__()
        self.assembly_trolley_model = TrolleyListModel()
        self.queue_trolley_model = TrolleyListModel()
        self.precharge_trolley_model = TrolleyListModel()
        self.assembly_ungrouped_model = UngroupTrayTableModel()
        self._tray_cache: dict[str, dict[str, Any]] = {}

    @Slot(object)
    def on_event(self, payload: dict[str, Any]) -> None:
        event_type = str(payload.get("type", ""))
        log.debug("BoardViewModel event received type=%s", event_type)
        if event_type == "snapshot":
            self._apply_snapshot(payload)
        elif event_type == "projection":
            self._apply_projection(payload)
        elif event_type == "trays_delta":
            self._apply_trays_delta(payload.get("rows", []))
        else:
            log.debug("BoardViewModel ignored unknown event type=%s", event_type)
        self.updated.emit(self.summary())

    def summary(self) -> dict[str, int]:
        grouped_ids = self._grouped_tray_ids()
        asm_trolley, asm_tray, asm_cell = self.assembly_trolley_model.totals()
        que_trolley, que_tray, que_cell = self.queue_trolley_model.totals()
        pre_trolley, pre_tray, pre_cell = self.precharge_trolley_model.totals()
        return {
            "tray_count": len(self._tray_cache),
            "group_count": len(grouped_ids),
            "assembly_trolley_count": asm_trolley,
            "queue_trolley_count": que_trolley,
            "precharge_trolley_count": pre_trolley,
            "assembly_ungroup_count": self.assembly_ungrouped_model.rowCount(),
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
        self._rebuild_assembly_ungrouped_from_cache()

    def _apply_projection(self, payload: dict[str, Any]) -> None:
        self.assembly_trolley_model.replace_all(payload.get("assembly_trolleys", []))
        self.queue_trolley_model.replace_all(payload.get("queue_trolleys", []))
        self.precharge_trolley_model.replace_all(payload.get("precharge_trolleys", []))
        log.debug(
            "BoardViewModel projection applied assembly=%s queue=%s precharge=%s",
            self.assembly_trolley_model.rowCount(),
            self.queue_trolley_model.rowCount(),
            self.precharge_trolley_model.rowCount(),
        )
        self._rebuild_assembly_ungrouped_from_cache()

    def _apply_trays_delta(self, payload_rows: list[dict[str, Any]]) -> None:
        if not payload_rows:
            return
        for row in payload_rows:
            tray_id = str(row.get("tray_id", "")).strip()
            if not tray_id:
                continue
            self._tray_cache[tray_id] = row
        self._rebuild_assembly_ungrouped_from_cache()
        log.debug(
            "BoardViewModel trays delta applied rows=%s cache=%s",
            len(payload_rows),
            len(self._tray_cache),
        )

    def _rebuild_assembly_ungrouped_from_cache(self) -> None:
        grouped_ids = self._grouped_tray_ids()
        rows = [row for tray_id, row in self._tray_cache.items() if tray_id not in grouped_ids]
        self.assembly_ungrouped_model.replace_from_tray_payloads(rows)

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
    total_seconds = int(delta.total_seconds())
    sign = "-" if total_seconds < 0 else ""
    total_seconds = abs(total_seconds)
    hours, rem = divmod(total_seconds, 3600)
    minutes, seconds = divmod(rem, 60)
    return f"{sign}{hours:02d}:{minutes:02d}:{seconds:02d}"
