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

    def ungroup(self, tray_id: TrayId) -> None:
        self._state.assignments.pop(str(tray_id), None)

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
