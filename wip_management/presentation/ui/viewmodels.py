from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from functools import lru_cache
import logging
import time
from typing import Any

from PySide6.QtCore import (
    QAbstractListModel, QAbstractTableModel, QModelIndex,
    QObject, Qt, QTimer, Signal, Slot
)
from PySide6.QtGui import QBrush, QColor

from wip_management.config import settings
from wip_management.presentation.ui.mapper import parse_datetime

log = logging.getLogger(__name__)

UNGROUP_CHECK_VISUAL_ROLE = int(Qt.ItemDataRole.UserRole) + 101

_AGE_STATE_UNKNOWN = "unknown"
_AGE_STATE_WAITING = "waiting"
_AGE_STATE_READY = "ready"
_AGE_STATE_EXCEED = "exceed"

_SELECT_ICON_COLOR = QBrush(QColor("#8a949e"))


@dataclass(slots=True, frozen=True)
class TrolleyRowVM:
    trolley_id: str
    column: str
    mode: str
    state: str
    tray_quantity: int
    cell_quantity: int
    tray_ids: tuple[str, ...]
    aging_state: str
    aging_time: str

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> "TrolleyRowVM":
        tray_ids = tuple(str(item) for item in payload.get("tray_ids", []))
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
    """Optimized trolley list model with diff-based updates."""

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
        self._row_hash: int = 0

    def rowCount(self, parent: QModelIndex = QModelIndex()) -> int:
        if parent.isValid():
            return 0
        return len(self._rows)

    def data(self, index: QModelIndex, role: int = Qt.ItemDataRole.DisplayRole) -> Any:
        if not index.isValid():
            return None
        row = index.row()
        if row < 0 or row >= len(self._rows):
            return None
        item = self._rows[row]

        if role == Qt.ItemDataRole.DisplayRole:
            if item.column == "Queue" and item.aging_state != _AGE_STATE_UNKNOWN:
                return f"{item.trolley_id} | {item.state} | age={item.aging_time} | trays={item.tray_quantity} | {item.mode.upper()}"
            return f"{item.trolley_id} | {item.state} | trays={item.tray_quantity} | {item.mode.upper()}"

        if role == Qt.ItemDataRole.BackgroundRole and item.column == "Queue":
            colors = {
                _AGE_STATE_WAITING: "#ffedd5",
                _AGE_STATE_READY: "#dcfce7",
                _AGE_STATE_EXCEED: "#fee2e2",
            }
            color = colors.get(item.aging_state)
            return QBrush(QColor(color)) if color else None

        role_map = {
            self.TrolleyIdRole: item.trolley_id,
            self.ColumnRole: item.column,
            self.ModeRole: item.mode,
            self.TrayCountRole: item.tray_quantity,
            self.TrayIdsRole: list(item.tray_ids),
            self.StateRole: item.state,
            self.CellCountRole: item.cell_quantity,
            self.AgingStateRole: item.aging_state,
            self.AgingTimeRole: item.aging_time,
        }
        return role_map.get(role)

    def roleNames(self) -> dict[int, bytes]:
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
        """Replace with diff detection to minimize UI updates."""
        items = [TrolleyRowVM.from_payload(row) for row in payload_rows if row.get("trolley_id")]

        new_hash = hash(tuple(items))
        if new_hash == self._row_hash:
            return

        self._row_hash = new_hash
        self.beginResetModel()
        self._rows = items
        self.endResetModel()

    def all_tray_ids(self) -> set[str]:
        out: set[str] = set()
        for row in self._rows:
            out.update(row.tray_ids)
        return out

    def totals(self) -> tuple[int, int, int]:
        return (
            len(self._rows),
            sum(row.tray_quantity for row in self._rows),
            sum(row.cell_quantity for row in self._rows),
        )

    def trolley_ids(self) -> list[str]:
        return [row.trolley_id for row in self._rows]

    def find_by_tray_id(self, tray_id: str) -> TrolleyRowVM | None:
        wanted = tray_id.strip()
        for row in self._rows:
            if wanted in row.tray_ids:
                return row
        return None


class UngroupTrayTableModel(QAbstractTableModel):
    """Optimized ungroup table with incremental updates."""

    _headers = ["Select", "#", "Tray_ID", "Q'ty", "Assembly_Out", "Precharge_In", "Aging", "Status", "Location"]

    def __init__(self) -> None:
        super().__init__()
        self._rows: list[UngroupTrayRowVM] = []
        self._checked_tray_ids: set[str] = set()
        self._interaction_hold = False
        self._pending_payload: list[dict[str, Any]] | None = None
        self._data_hash: int = 0
        self._update_scheduled = False

    def rowCount(self, parent: QModelIndex = QModelIndex()) -> int:
        return 0 if parent.isValid() else len(self._rows)

    def columnCount(self, parent: QModelIndex = QModelIndex()) -> int:
        return 0 if parent.isValid() else len(self._headers)

    def data(self, index: QModelIndex, role: int = Qt.ItemDataRole.DisplayRole) -> Any:
        if not index.isValid():
            return None

        row = index.row()
        col = index.column()

        if row < 0 or row >= len(self._rows):
            return None

        item = self._rows[row]

        if col == 0:
            if role == UNGROUP_CHECK_VISUAL_ROLE:
                return "on" if item.tray_id in self._checked_tray_ids else "off"
            if role == Qt.ItemDataRole.TextAlignmentRole:
                return Qt.AlignmentFlag.AlignCenter
            return None

        if role == Qt.ItemDataRole.TextAlignmentRole:
            return Qt.AlignmentFlag.AlignCenter

        if role != Qt.ItemDataRole.DisplayRole:
            return None

        col_map = {
            1: item.no,
            2: item.tray_id,
            3: item.quantity,
            4: item.assembly_out,
            5: item.precharge_in,
            6: item.aging_time,
            7: item.status,
            8: item.location,
        }
        return col_map.get(col)

    def flags(self, index: QModelIndex) -> Qt.ItemFlag:
        if not index.isValid():
            return Qt.ItemFlag.NoItemFlags
        return Qt.ItemFlag.ItemIsEnabled | Qt.ItemFlag.ItemIsSelectable

    def setData(self, index: QModelIndex, value: Any, role: int = Qt.ItemDataRole.EditRole) -> bool:
        if not index.isValid() or index.column() != 0:
            return False

        row = index.row()
        if row < 0 or row >= len(self._rows):
            return False

        tray_id = self._rows[row].tray_id
        if not tray_id:
            return False

        checked = value in {True, 1, Qt.CheckState.Checked, Qt.CheckState.Checked.value}

        if checked:
            self._checked_tray_ids.add(tray_id)
        else:
            self._checked_tray_ids.discard(tray_id)

        self.dataChanged.emit(index, index, [UNGROUP_CHECK_VISUAL_ROLE])
        self.headerDataChanged.emit(Qt.Orientation.Horizontal, 0, 0)
        return True

    def headerData(self, section: int, orientation: Qt.Orientation, role: int = Qt.ItemDataRole.DisplayRole) -> Any:
        if orientation != Qt.Orientation.Horizontal:
            return section + 1 if role == Qt.ItemDataRole.DisplayRole else None

        if section < 0 or section >= len(self._headers):
            return None

        if section == 0:
            if role == UNGROUP_CHECK_VISUAL_ROLE:
                if not self._rows:
                    return "off"
                selected = len(self._checked_tray_ids)
                if selected == 0:
                    return "off"
                return "on" if selected >= len(self._rows) else "partial"
            if role == Qt.ItemDataRole.TextAlignmentRole:
                return Qt.AlignmentFlag.AlignCenter
            if role == Qt.ItemDataRole.DisplayRole:
                return ""
            return None

        if role == Qt.ItemDataRole.DisplayRole:
            return self._headers[section]
        if role == Qt.ItemDataRole.TextAlignmentRole:
            return Qt.AlignmentFlag.AlignCenter
        return None

    def replace_from_tray_payloads(self, tray_payload_rows: list[dict[str, Any]], *, force: bool = False, immediate: bool = False) -> None:
        """Replace data with debounced updates."""
        if self._interaction_hold and not force:
            self._pending_payload = list(tray_payload_rows)
            return

        if force:
            self._pending_payload = None

        if immediate:
            self._apply_payload(tray_payload_rows)
        else:
            self._schedule_apply(tray_payload_rows)

    def _schedule_apply(self, rows: list[dict[str, Any]]) -> None:
        """Debounce rapid updates."""
        if self._update_scheduled:
            self._pending_payload = rows
            return

        self._update_scheduled = True
        QTimer.singleShot(50, lambda: self._apply_payload(rows))

    def _apply_payload(self, tray_payload_rows: list[dict[str, Any]]) -> None:
        """Apply payload with hash-based change detection."""
        self._update_scheduled = False

        if self._pending_payload is not None:
            tray_payload_rows = self._pending_payload
            self._pending_payload = None

        started_at = time.perf_counter()
        now = _coarse_now(60)

        items: list[tuple[tuple, UngroupTrayRowVM]] = []

        for row in tray_payload_rows:
            tray_id = str(row.get("tray_id", "")).strip()
            if not tray_id:
                continue

            ccu = row.get("ccu_payload") or {}
            fpc = row.get("fpc_payload") or {}

            if not ccu:
                continue

            qty = _to_int(ccu.get("quantity"), default=_to_int(fpc.get("cell_count"), default=0))
            end_dt = _parse_cached(ccu.get("end_time"))
            precharge_start = _parse_cached(fpc.get("precharge_start_time")) if ccu and fpc else None

            if fpc:
                location = "Precharge"
                status = "Precharged"
                aging = _format_td(precharge_start - end_dt) if end_dt and precharge_start else "-"
            else:
                location = "Assembly"
                if end_dt:
                    delta = now - end_dt
                    aging = _format_td(delta)
                    status = _aging_label(delta)
                else:
                    aging = "-"
                    status = "-"

            sort_key = (end_dt or datetime.min, precharge_start or datetime.min, tray_id)

            items.append((sort_key, UngroupTrayRowVM(
                no=0,
                tray_id=tray_id,
                quantity=qty,
                assembly_out=end_dt.isoformat(sep=" ", timespec="seconds") if end_dt else "-",
                precharge_in=precharge_start.isoformat(sep=" ", timespec="seconds") if precharge_start else "-",
                aging_time=aging,
                status=status,
                location=location,
            )))

        items.sort(key=lambda x: x[0], reverse=True)

        ordered = [
            UngroupTrayRowVM(
                no=idx + 1,
                tray_id=vm.tray_id,
                quantity=vm.quantity,
                assembly_out=vm.assembly_out,
                precharge_in=vm.precharge_in,
                aging_time=vm.aging_time,
                status=vm.status,
                location=vm.location,
            )
            for idx, (_, vm) in enumerate(items)
        ]

        new_hash = hash(tuple((r.tray_id, r.assembly_out, r.status) for r in ordered))
        if new_hash == self._data_hash and len(ordered) == len(self._rows):
            return

        self._data_hash = new_hash

        retained = {r.tray_id for r in ordered}
        self._checked_tray_ids &= retained

        self.beginResetModel()
        self._rows = ordered
        self.endResetModel()

        elapsed = int((time.perf_counter() - started_at) * 1000)
        if elapsed > 30:
            log.warning("Ungroup rebuild slow elapsed_ms=%s rows=%s", elapsed, len(ordered))

    def set_interaction_hold(self, hold: bool) -> None:
        if self._interaction_hold == hold:
            return
        self._interaction_hold = hold

        if not hold and self._pending_payload is not None:
            pending = self._pending_payload
            self._pending_payload = None
            self._apply_payload(pending)

    def checked_tray_ids(self) -> list[str]:
        return [r.tray_id for r in self._rows if r.tray_id in self._checked_tray_ids]

    def has_tray(self, tray_id: str) -> bool:
        wanted = tray_id.strip()
        return any(r.tray_id == wanted for r in self._rows)

    def toggle_row(self, row: int) -> None:
        if row < 0 or row >= len(self._rows):
            return
        tray_id = self._rows[row].tray_id
        if not tray_id:
            return

        if tray_id in self._checked_tray_ids:
            self._checked_tray_ids.discard(tray_id)
        else:
            self._checked_tray_ids.add(tray_id)

        index = self.index(row, 0)
        self.dataChanged.emit(index, index, [UNGROUP_CHECK_VISUAL_ROLE])
        self.headerDataChanged.emit(Qt.Orientation.Horizontal, 0, 0)

    def set_all_checked(self, checked: bool) -> None:
        if checked:
            self._checked_tray_ids = {r.tray_id for r in self._rows if r.tray_id}
        else:
            self._checked_tray_ids.clear()

        if self._rows:
            self.dataChanged.emit(
                self.index(0, 0),
                self.index(len(self._rows) - 1, 0),
                [UNGROUP_CHECK_VISUAL_ROLE]
            )
        self.headerDataChanged.emit(Qt.Orientation.Horizontal, 0, 0)

    def clear_checked(self) -> None:
        self.set_all_checked(False)

    def toggle_all_checked(self) -> None:
        self.set_all_checked(len(self._checked_tray_ids) < len(self._rows))

    def get_all_tray_ids(self) -> set[str]:
        """Return all tray IDs in ungrouped list."""
        return {r.tray_id for r in self._rows if r.tray_id}


class BoardViewModel(QObject):
    """
    Board view model with CONSISTENT summary computation.
    
    KEY FIX: Summary counts are now derived from the SAME data source
    as the displayed lists, ensuring consistency.
    """

    updated = Signal(object)

    def __init__(self) -> None:
        super().__init__()
        self.assembly_trolley_model = TrolleyListModel()
        self.queue_trolley_model = TrolleyListModel()
        self.precharge_trolley_model = TrolleyListModel()
        self.assembly_ungrouped_model = UngroupTrayTableModel()

        self._tray_cache: dict[str, dict[str, Any]] = {}
        self._last_event_seq = 0
        self._last_summary: dict[str, int] | None = None
        self._queued_event: dict[str, Any] | None = None
        self._apply_scheduled = False
        
        # Track the actual counts from last projection for consistency
        self._last_projection_ungrouped_count: int = 0
        self._last_projection_grouped_tray_ids: set[str] = set()

    @Slot(object)
    def on_event(self, payload: dict[str, Any]) -> None:
        """Queue event for coalesced processing."""
        seq = _to_int(payload.get("seq"), default=0)

        if seq > 0 and seq <= self._last_event_seq:
            return

        current_seq = _to_int(self._queued_event.get("seq"), default=0) if self._queued_event else 0
        if seq >= current_seq or seq == 0:
            self._queued_event = payload

        if not self._apply_scheduled:
            self._apply_scheduled = True
            QTimer.singleShot(32, self._process_event)

    def _process_event(self) -> None:
        """Process queued event."""
        self._apply_scheduled = False

        payload = self._queued_event
        self._queued_event = None

        if payload is None:
            return

        started_at = time.perf_counter()
        seq = _to_int(payload.get("seq"), default=0)

        if seq > 0 and seq <= self._last_event_seq:
            return

        if seq > 0:
            self._last_event_seq = seq

        event_type = str(payload.get("type", ""))

        if event_type == "snapshot":
            self._apply_snapshot(payload)
        elif event_type == "projection":
            self._apply_projection(payload)
        elif event_type == "trays_delta":
            self._apply_delta(payload.get("rows", []))

        # Emit summary - computed from current model state
        summary = self.summary()
        if summary != self._last_summary:
            self._last_summary = dict(summary)
            self.updated.emit(summary)

        elapsed = int((time.perf_counter() - started_at) * 1000)
        if elapsed > 50:
            log.warning("BoardViewModel slow type=%s elapsed_ms=%s", event_type, elapsed)

    def _apply_snapshot(self, payload: dict[str, Any]) -> None:
        """Apply full snapshot."""
        tray_rows = payload.get("trays", [])
        self._tray_cache = {
            str(row["tray_id"]): row
            for row in tray_rows
            if row.get("tray_id")
        }
        self._apply_projection(payload)
        log.info("BoardViewModel snapshot applied trays=%s", len(self._tray_cache))

    def _apply_projection(self, payload: dict[str, Any]) -> None:
        """
        Apply projection update.
        
        KEY FIX: Track grouped tray IDs and ungrouped count from the projection
        itself, not from model state which may be stale.
        """
        assembly = payload.get("assembly_trolleys", [])
        queue = self._annotate_queue_aging(payload.get("queue_trolleys", []))
        precharge = payload.get("precharge_trolleys", [])

        # Update trolley models
        self.assembly_trolley_model.replace_all(assembly)
        self.queue_trolley_model.replace_all(queue)
        self.precharge_trolley_model.replace_all(precharge)

        # Compute grouped tray IDs from projection data directly
        grouped_tray_ids: set[str] = set()
        for trolley_list in [assembly, queue, precharge]:
            for trolley in trolley_list:
                tray_ids = trolley.get("tray_ids", [])
                for tray_id in tray_ids:
                    tid = str(tray_id).strip()
                    if tid:
                        grouped_tray_ids.add(tid)
        
        self._last_projection_grouped_tray_ids = grouped_tray_ids

        # Handle ungrouped list
        if "assembly_ungrouped" in payload:
            ungrouped = [r for r in payload.get("assembly_ungrouped", []) if isinstance(r, dict)]
            
            # Update tray cache with ungrouped data
            for row in ungrouped:
                tray_id = str(row.get("tray_id", "")).strip()
                if tray_id:
                    self._tray_cache[tray_id] = row
            
            # Track ungrouped count from projection
            self._last_projection_ungrouped_count = len(ungrouped)
            
            # Update ungrouped model
            self.assembly_ungrouped_model.replace_from_tray_payloads(
                ungrouped, force=True, immediate=True
            )
            
            log.debug(
                "Projection applied: grouped=%d ungrouped=%d",
                len(grouped_tray_ids),
                len(ungrouped)
            )

    def _apply_delta(self, rows: list[dict[str, Any]]) -> None:
        """Apply incremental tray updates."""
        if not rows:
            return

        grouped_ids = self._grouped_tray_ids()
        rebuild_needed = False

        for row in rows:
            tray_id = str(row.get("tray_id", "")).strip()
            if not tray_id:
                continue

            prev = self._tray_cache.get(tray_id)
            if prev == row:
                continue

            self._tray_cache[tray_id] = row

            if tray_id not in grouped_ids:
                rebuild_needed = True

        if rebuild_needed:
            self._rebuild_ungrouped()

    def _rebuild_ungrouped(self) -> None:
        """Rebuild ungrouped list from cache."""
        grouped = self._grouped_tray_ids()
        rows = [
            row for tray_id, row in self._tray_cache.items()
            if tray_id not in grouped and row.get("ccu_payload")
        ]
        self._last_projection_ungrouped_count = len(rows)
        self.assembly_ungrouped_model.replace_from_tray_payloads(rows)

    def _annotate_queue_aging(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Add aging info to queue trolleys."""
        now = _coarse_now(30)
        out: list[dict[str, Any]] = []

        for row in rows:
            row_copy = dict(row)
            latest_end: datetime | None = None

            for tray_id in row.get("tray_ids", []):
                tray = self._tray_cache.get(str(tray_id))
                if not tray:
                    continue

                ccu = tray.get("ccu_payload") or {}
                fpc = tray.get("fpc_payload") or {}

                end_dt = _parse_cached(ccu.get("end_time")) or _parse_cached(fpc.get("precharge_end_time"))
                if end_dt and (latest_end is None or end_dt > latest_end):
                    latest_end = end_dt

            if latest_end is None:
                row_copy["aging_state"] = _AGE_STATE_UNKNOWN
                row_copy["aging_time"] = "-"
            else:
                delta = now - latest_end
                row_copy["aging_state"] = _aging_state(delta)
                row_copy["aging_time"] = _format_td(delta)

            out.append(row_copy)

        return out

    def _grouped_tray_ids(self) -> set[str]:
        """Get all tray IDs in trolleys."""
        return (
            self.assembly_trolley_model.all_tray_ids() |
            self.queue_trolley_model.all_tray_ids() |
            self.precharge_trolley_model.all_tray_ids()
        )

    def summary(self) -> dict[str, int]:
        """
        Get current summary metrics.
        
        KEY FIX: Use consistent sources for counts:
        - grouped_count comes from trolley models (authoritative for grouped)
        - ungrouped_count comes from ungrouped model row count (authoritative for ungrouped)
        - total = grouped + ungrouped (derived, not independently computed)
        """
        # Get grouped tray IDs from trolley models
        grouped_tray_ids = self._grouped_tray_ids()
        grouped_count = len(grouped_tray_ids)
        
        # Get ungrouped count directly from the ungrouped model
        # This ensures the count matches what's displayed
        ungrouped_count = self.assembly_ungrouped_model.rowCount()
        
        # Total is derived from the two authoritative counts
        total_count = grouped_count + ungrouped_count

        asm_t, asm_tray, asm_cell = self.assembly_trolley_model.totals()
        que_t, que_tray, que_cell = self.queue_trolley_model.totals()
        pre_t, pre_tray, pre_cell = self.precharge_trolley_model.totals()

        return {
            "tray_count": total_count,
            "group_count": grouped_count,
            "assembly_trolley_count": asm_t,
            "queue_trolley_count": que_t,
            "precharge_trolley_count": pre_t,
            "assembly_ungroup_count": ungrouped_count,
            "assembly_tray_count": asm_tray,
            "queue_tray_count": que_tray,
            "precharge_tray_count": pre_tray,
            "assembly_cell_count": asm_cell,
            "queue_cell_count": que_cell,
            "precharge_cell_count": pre_cell,
        }

    def tray_payload_by_id(self, tray_id: str) -> dict[str, Any] | None:
        return self._tray_cache.get(tray_id)

    def tray_payloads(self, tray_ids: list[str]) -> list[dict[str, Any]]:
        return [self._tray_cache[tid] for tid in tray_ids if tid in self._tray_cache]
    
    def force_refresh_summary(self) -> None:
        """Force emit current summary to UI."""
        summary = self.summary()
        self._last_summary = dict(summary)
        self.updated.emit(summary)


# ===== Utility Functions =====

def _to_int(value: Any, *, default: int) -> int:
    if value is None:
        return default
    if isinstance(value, int):
        return value
    try:
        return int(str(value).strip())
    except Exception:
        return default


def _format_td(delta: timedelta) -> str:
    """Format timedelta as HH:MM:SS."""
    total = max(0, int(delta.total_seconds()))
    h, rem = divmod(total, 3600)
    m, s = divmod(rem, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"


def _coarse_now(step_seconds: int) -> datetime:
    """Get coarse-grained current time for cache efficiency."""
    now = datetime.now()
    floored = (now.second // step_seconds) * step_seconds
    return now.replace(second=floored, microsecond=0)


@lru_cache(maxsize=10000)
def _parse_cached(text: str | None) -> datetime | None:
    """Cached datetime parsing."""
    if not text:
        return None
    return parse_datetime(text)


def _aging_state(delta: timedelta) -> str:
    """Get aging state based on settings."""
    hours = max(delta.total_seconds(), 0) / 3600
    target = max(float(settings.target_aging_hours), 0)
    tol = max(float(settings.target_aging_tolerance_hours), 0)

    if hours < max(target - tol, 0):
        return _AGE_STATE_WAITING
    if hours <= target + tol:
        return _AGE_STATE_READY
    return _AGE_STATE_EXCEED


def _aging_label(delta: timedelta) -> str:
    """Get aging label text."""
    state = _aging_state(delta)
    labels = {
        _AGE_STATE_WAITING: "Aging",
        _AGE_STATE_READY: "Aged",
        _AGE_STATE_EXCEED: "Aged Out",
    }
    return labels.get(state, "-")