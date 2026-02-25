from __future__ import annotations

from dataclasses import dataclass

from wip_management.domain.models.tray import Tray
from wip_management.domain.models.trolley import Trolley


@dataclass(slots=True, frozen=True)
class TrayUpdated:
    tray: Tray


@dataclass(slots=True, frozen=True)
class TrolleyUpdated:
    trolleys: list[Trolley]
    assembly_trolleys: list[Trolley]
    queue_trolleys: list[Trolley]
    precharge_trolleys: list[Trolley]
    assembly_ungrouped: list[Tray]


@dataclass(slots=True, frozen=True)
class SnapshotReady:
    trays: list[Tray]
    trolleys: list[Trolley]
    assembly_trolleys: list[Trolley]
    queue_trolleys: list[Trolley]
    precharge_trolleys: list[Trolley]
    assembly_ungrouped: list[Tray]
