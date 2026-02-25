from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from PySide6.QtCore import QAbstractListModel, QModelIndex, QObject, Qt, Signal, Slot

from wip_management.presentation.ui.mapper import parse_datetime


@dataclass(slots=True, frozen=True)
class TrayRowVM:
    tray_id: str
    status: str
    latest_collected_time: datetime | None
    updated_at: datetime | None
    ccu_payload: dict[str, Any] | None
    fpc_payload: dict[str, Any] | None

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> "TrayRowVM":
        return cls(
            tray_id=str(payload.get("tray_id", "")),
            status=str(payload.get("status", "Unknown")),
            latest_collected_time=parse_datetime(payload.get("latest_collected_time")),
            updated_at=parse_datetime(payload.get("updated_at")),
            ccu_payload=payload.get("ccu_payload"),
            fpc_payload=payload.get("fpc_payload"),
        )


@dataclass(slots=True, frozen=True)
class TrolleyRowVM:
    trolley_id: str
    column: str
    mode: str
    tray_ids: list[str]

    @property
    def tray_count(self) -> int:
        return len(self.tray_ids)

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> "TrolleyRowVM":
        tray_ids = [str(item) for item in payload.get("tray_ids", [])]
        return cls(
            trolley_id=str(payload.get("trolley_id", "")),
            column=str(payload.get("column", "")),
            mode=str(payload.get("mode", "auto")),
            tray_ids=tray_ids,
        )


class TrayListModel(QAbstractListModel):
    TrayIdRole = Qt.ItemDataRole.UserRole + 1
    StatusRole = Qt.ItemDataRole.UserRole + 2
    LatestTimeRole = Qt.ItemDataRole.UserRole + 3
    UpdatedAtRole = Qt.ItemDataRole.UserRole + 4

    def __init__(self) -> None:
        super().__init__()
        self._rows: list[TrayRowVM] = []
        self._index_by_id: dict[str, int] = {}

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
            latest = item.latest_collected_time.isoformat() if item.latest_collected_time else "-"
            return f"{item.tray_id} | {item.status} | {latest}"
        if role == self.TrayIdRole:
            return item.tray_id
        if role == self.StatusRole:
            return item.status
        if role == self.LatestTimeRole:
            return item.latest_collected_time.isoformat() if item.latest_collected_time else None
        if role == self.UpdatedAtRole:
            return item.updated_at.isoformat() if item.updated_at else None
        return None

    def roleNames(self) -> dict[int, bytes]:  # noqa: N802
        return {
            self.TrayIdRole: b"trayId",
            self.StatusRole: b"status",
            self.LatestTimeRole: b"latestCollectedTime",
            self.UpdatedAtRole: b"updatedAt",
        }

    def replace_all(self, payload_rows: list[dict[str, Any]]) -> None:
        items = [TrayRowVM.from_payload(row) for row in payload_rows if row.get("tray_id")]
        self.beginResetModel()
        self._rows = sorted(items, key=_tray_sort_key, reverse=True)
        self._index_by_id = {row.tray_id: idx for idx, row in enumerate(self._rows)}
        self.endResetModel()

    def upsert_many(self, payload_rows: list[dict[str, Any]]) -> None:
        for payload in payload_rows:
            if not payload.get("tray_id"):
                continue
            self._upsert_one(TrayRowVM.from_payload(payload))

    def snapshot_ids(self) -> set[str]:
        return {row.tray_id for row in self._rows}

    def _upsert_one(self, row: TrayRowVM) -> None:
        idx = self._index_by_id.get(row.tray_id)
        if idx is None:
            insert_at = self._find_insert_index(row)
            self.beginInsertRows(QModelIndex(), insert_at, insert_at)
            self._rows.insert(insert_at, row)
            self.endInsertRows()
            self._reindex_from(insert_at)
            return
        current = self._rows[idx]
        if current == row:
            return
        if _tray_sort_key(current) == _tray_sort_key(row):
            self._rows[idx] = row
            qidx = self.index(idx, 0)
            self.dataChanged.emit(qidx, qidx)
            return
        self._index_by_id.pop(current.tray_id, None)
        self.beginRemoveRows(QModelIndex(), idx, idx)
        self._rows.pop(idx)
        self.endRemoveRows()
        self._reindex_from(idx)
        insert_at = self._find_insert_index(row)
        self.beginInsertRows(QModelIndex(), insert_at, insert_at)
        self._rows.insert(insert_at, row)
        self.endInsertRows()
        self._reindex_from(min(insert_at, idx))

    def _find_insert_index(self, row: TrayRowVM) -> int:
        lo = 0
        hi = len(self._rows)
        while lo < hi:
            mid = (lo + hi) // 2
            if _tray_sort_key(row) > _tray_sort_key(self._rows[mid]):
                hi = mid
            else:
                lo = mid + 1
        return lo

    def _reindex_from(self, start: int) -> None:
        for idx in range(start, len(self._rows)):
            self._index_by_id[self._rows[idx].tray_id] = idx


class TrolleyListModel(QAbstractListModel):
    TrolleyIdRole = Qt.ItemDataRole.UserRole + 1
    ColumnRole = Qt.ItemDataRole.UserRole + 2
    ModeRole = Qt.ItemDataRole.UserRole + 3
    TrayCountRole = Qt.ItemDataRole.UserRole + 4
    TrayIdsRole = Qt.ItemDataRole.UserRole + 5

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
            return f"{item.trolley_id} | {item.mode.upper()} | trays={item.tray_count}"
        if role == self.TrolleyIdRole:
            return item.trolley_id
        if role == self.ColumnRole:
            return item.column
        if role == self.ModeRole:
            return item.mode
        if role == self.TrayCountRole:
            return item.tray_count
        if role == self.TrayIdsRole:
            return item.tray_ids
        return None

    def roleNames(self) -> dict[int, bytes]:  # noqa: N802
        return {
            self.TrolleyIdRole: b"trolleyId",
            self.ColumnRole: b"column",
            self.ModeRole: b"mode",
            self.TrayCountRole: b"trayCount",
            self.TrayIdsRole: b"trayIds",
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


class BoardViewModel(QObject):
    updated = Signal(object)

    def __init__(self) -> None:
        super().__init__()
        self.assembly_trolley_model = TrolleyListModel()
        self.queue_trolley_model = TrolleyListModel()
        self.precharge_trolley_model = TrolleyListModel()
        self.assembly_ungrouped_model = TrayListModel()
        self._tray_cache: dict[str, dict[str, Any]] = {}

    @Slot(object)
    def on_event(self, payload: dict[str, Any]) -> None:
        event_type = str(payload.get("type", ""))
        if event_type == "snapshot":
            self._apply_snapshot(payload)
        elif event_type == "projection":
            self._apply_projection(payload)
        elif event_type == "trays_delta":
            self._apply_trays_delta(payload.get("rows", []))
        self.updated.emit(self.summary())

    def summary(self) -> dict[str, int]:
        grouped_ids = self._grouped_tray_ids()
        return {
            "all_trays": len(self._tray_cache),
            "grouped_trays": len(grouped_ids),
            "assembly_trolleys": self.assembly_trolley_model.rowCount(),
            "queue_trolleys": self.queue_trolley_model.rowCount(),
            "precharge_trolleys": self.precharge_trolley_model.rowCount(),
            "assembly_ungrouped": self.assembly_ungrouped_model.rowCount(),
        }

    def _apply_snapshot(self, payload: dict[str, Any]) -> None:
        tray_rows = payload.get("trays", [])
        self._tray_cache = {str(row["tray_id"]): row for row in tray_rows if row.get("tray_id")}
        self._apply_projection(payload)
        if "assembly_ungrouped" in payload:
            self.assembly_ungrouped_model.replace_all(payload["assembly_ungrouped"])
            return
        self._rebuild_assembly_ungrouped_from_cache()

    def _apply_projection(self, payload: dict[str, Any]) -> None:
        self.assembly_trolley_model.replace_all(payload.get("assembly_trolleys", []))
        self.queue_trolley_model.replace_all(payload.get("queue_trolleys", []))
        self.precharge_trolley_model.replace_all(payload.get("precharge_trolleys", []))
        if "assembly_ungrouped" in payload:
            self.assembly_ungrouped_model.replace_all(payload["assembly_ungrouped"])
            return
        self._rebuild_assembly_ungrouped_from_cache()

    def _apply_trays_delta(self, payload_rows: list[dict[str, Any]]) -> None:
        if not payload_rows:
            return
        for row in payload_rows:
            tray_id = str(row.get("tray_id", "")).strip()
            if not tray_id:
                continue
            self._tray_cache[tray_id] = row
        grouped_ids = self._grouped_tray_ids()
        ungrouped_rows = [row for row in payload_rows if str(row.get("tray_id", "")) not in grouped_ids]
        if ungrouped_rows:
            self.assembly_ungrouped_model.upsert_many(ungrouped_rows)

    def _rebuild_assembly_ungrouped_from_cache(self) -> None:
        grouped_ids = self._grouped_tray_ids()
        rows = [row for tray_id, row in self._tray_cache.items() if tray_id not in grouped_ids]
        self.assembly_ungrouped_model.replace_all(rows)

    def _grouped_tray_ids(self) -> set[str]:
        return {
            *self.assembly_trolley_model.all_tray_ids(),
            *self.queue_trolley_model.all_tray_ids(),
            *self.precharge_trolley_model.all_tray_ids(),
        }


def _tray_sort_key(row: TrayRowVM) -> tuple[datetime, str]:
    return (row.latest_collected_time or datetime.min, row.tray_id)
