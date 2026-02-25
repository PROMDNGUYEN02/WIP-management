from __future__ import annotations

from dataclasses import dataclass

from wip_management.domain.models.tray import TrayId
from wip_management.domain.models.trolley import Column


@dataclass(slots=True)
class ManualGroupingAssignment:
    trolley_id: str
    column: Column


@dataclass(slots=True)
class ManualGroupingState:
    assignments: dict[str, ManualGroupingAssignment]


class ManualGroupUseCase:
    def __init__(self) -> None:
        self._state = ManualGroupingState(assignments={})

    def group_to_trolley(self, tray_id: TrayId, trolley_id: str, column: Column) -> None:
        normalized_trolley_id = trolley_id.strip()
        if not normalized_trolley_id:
            raise ValueError("trolley_id must not be empty")
        self._state.assignments[str(tray_id)] = ManualGroupingAssignment(
            trolley_id=normalized_trolley_id,
            column=column,
        )

    def group_many_to_trolley(self, tray_ids: list[TrayId], trolley_id: str, column: Column) -> int:
        normalized_trolley_id = trolley_id.strip()
        if not normalized_trolley_id:
            raise ValueError("trolley_id must not be empty")
        applied = 0
        for tray_id in tray_ids:
            tray_key = str(tray_id).strip()
            if not tray_key:
                continue
            self._state.assignments[tray_key] = ManualGroupingAssignment(
                trolley_id=normalized_trolley_id,
                column=column,
            )
            applied += 1
        return applied

    def ungroup(self, tray_id: TrayId) -> None:
        self._state.assignments.pop(str(tray_id), None)

    def clear_trolley(self, trolley_id: str) -> int:
        trolley_key = trolley_id.strip()
        if not trolley_key:
            raise ValueError("trolley_id must not be empty")
        removed = 0
        for tray_id in list(self._state.assignments.keys()):
            assignment = self._state.assignments.get(tray_id)
            if assignment is None:
                continue
            if assignment.trolley_id != trolley_key:
                continue
            self._state.assignments.pop(tray_id, None)
            removed += 1
        return removed

    def rename_trolley(self, old_trolley_id: str, new_trolley_id: str) -> int:
        old_key = old_trolley_id.strip()
        new_key = new_trolley_id.strip()
        if not old_key or not new_key:
            raise ValueError("old_trolley_id and new_trolley_id must not be empty")
        if old_key == new_key:
            return 0
        changed = 0
        for assignment in self._state.assignments.values():
            if assignment.trolley_id != old_key:
                continue
            assignment.trolley_id = new_key
            changed += 1
        return changed

    def get_assignment(self, tray_id: TrayId) -> ManualGroupingAssignment | None:
        return self._state.assignments.get(str(tray_id))

    def dump(self) -> dict[str, dict[str, str]]:
        return {
            tray_id: {
                "trolley_id": assignment.trolley_id,
                "column": assignment.column.value,
            }
            for tray_id, assignment in self._state.assignments.items()
        }

    def projection_assignments(self) -> dict[str, tuple[Column, str]]:
        return {
            tray_id: (assignment.column, assignment.trolley_id)
            for tray_id, assignment in self._state.assignments.items()
        }

    def restore(self, assignments: dict[str, tuple[Column, str]]) -> None:
        restored: dict[str, ManualGroupingAssignment] = {}
        for tray_id, (column, trolley_id) in assignments.items():
            tray_key = tray_id.strip()
            trolley_key = trolley_id.strip()
            if not tray_key or not trolley_key:
                continue
            restored[tray_key] = ManualGroupingAssignment(trolley_id=trolley_key, column=column)
        self._state.assignments = restored
