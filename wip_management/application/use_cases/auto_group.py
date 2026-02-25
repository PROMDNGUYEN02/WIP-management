from __future__ import annotations

from wip_management.domain.models.tray import Tray
from wip_management.domain.rules.grouping_rules import TrolleyProjection, build_trolley_projection


class AutoGroupUseCase:
    def execute(
        self,
        trays: list[Tray],
        *,
        max_per_trolley: int,
        assembly_auto_trolley_count: int,
    ) -> TrolleyProjection:
        return build_trolley_projection(
            trays,
            max_per_trolley=max_per_trolley,
            assembly_auto_trolley_count=assembly_auto_trolley_count,
        )
