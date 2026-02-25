from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from wip_management.domain.models.tray import Tray, Watermark


@dataclass(slots=True, frozen=True)
class StoreSnapshot:
    created_at: datetime
    watermark: Watermark | None
    trays: list[dict]


def build_snapshot(trays: list[Tray], watermark: Watermark | None) -> StoreSnapshot:
    return StoreSnapshot(
        created_at=datetime.now(),
        watermark=watermark,
        trays=[tray.to_dict() for tray in trays],
    )

