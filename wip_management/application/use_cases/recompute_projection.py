from __future__ import annotations

from dataclasses import dataclass

from wip_management.domain.models.tray import Tray
from wip_management.domain.models.trolley import Column, Trolley
from wip_management.domain.rules.grouping_rules import build_trolley_projection


@dataclass(slots=True, frozen=True)
class Projection:
    trays: list[Tray]
    trolleys: list[Trolley]
    assembly_trolleys: list[Trolley]
    queue_trolleys: list[Trolley]
    precharge_trolleys: list[Trolley]
    assembly_ungrouped: list[Tray]


class RecomputeProjectionUseCase:
    def execute(
        self,
        trays: list[Tray],
        *,
        manual_assignments: dict[str, tuple[Column, str]] | None = None,
        max_per_trolley: int = 30,
        assembly_auto_trolley_count: int = 1,
        auto_group_enabled: bool = True,
    ) -> Projection:
        grouped = build_trolley_projection(
            trays,
            manual_assignments=manual_assignments,
            max_per_trolley=max_per_trolley,
            assembly_auto_trolley_count=assembly_auto_trolley_count,
            auto_group_enabled=auto_group_enabled,
        )
        trolleys = [
            *grouped.assembly_trolleys,
            *grouped.queue_trolleys,
            *grouped.precharge_trolleys,
        ]
        return Projection(
            trays=trays,
            trolleys=trolleys,
            assembly_trolleys=grouped.assembly_trolleys,
            queue_trolleys=grouped.queue_trolleys,
            precharge_trolleys=grouped.precharge_trolleys,
            assembly_ungrouped=grouped.assembly_ungrouped,
        )
