from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import NewType

from wip_management.domain.models.tray import TrayId

TrolleyId = NewType("TrolleyId", str)


class Column(StrEnum):
    ASSEMBLY = "Assembly"
    QUEUE = "Queue"
    PRECHARGE = "Precharge"


class TrolleyMode(StrEnum):
    AUTO = "auto"
    MANUAL = "manual"


class TrolleyState(StrEnum):
    STACKING = "Stacking"
    AGING = "Aging"
    WAITING = "Waiting"
    COMPLETED = "Completed"


@dataclass(slots=True)
class Trolley:
    trolley_id: TrolleyId
    column: Column
    tray_ids: list[TrayId] = field(default_factory=list)
    mode: TrolleyMode = TrolleyMode.AUTO
    state: TrolleyState = TrolleyState.WAITING
    tray_quantity: int = 0
    cell_quantity: int = 0

    def to_dict(self) -> dict:
        return {
            "trolley_id": str(self.trolley_id),
            "column": self.column.value,
            "tray_ids": [str(tray_id) for tray_id in self.tray_ids],
            "mode": self.mode.value,
            "state": self.state.value,
            "tray_quantity": self.tray_quantity,
            "cell_quantity": self.cell_quantity,
        }
