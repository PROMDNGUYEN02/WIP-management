from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime

from wip_management.domain.models.tray import Tray, TrayId, TrayStatus
from wip_management.domain.models.trolley import Column, Trolley, TrolleyId, TrolleyMode


@dataclass(slots=True, frozen=True)
class TrolleyProjection:
    assembly_trolleys: list[Trolley]
    queue_trolleys: list[Trolley]
    precharge_trolleys: list[Trolley]
    assembly_ungrouped: list[Tray]


def choose_column(tray: Tray) -> Column:
    if tray.status is TrayStatus.PRECHARGE:
        return Column.PRECHARGE
    if tray.status is TrayStatus.QUEUE:
        return Column.QUEUE
    return Column.ASSEMBLY


def build_trolley_projection(
    trays: Iterable[Tray],
    *,
    manual_assignments: Mapping[str, tuple[Column, str]] | None = None,
    max_per_trolley: int = 30,
    assembly_auto_trolley_count: int = 1,
) -> TrolleyProjection:
    rows = sorted(trays, key=_tray_sort_key, reverse=True)
    assignments = manual_assignments or {}

    manual_groups: dict[tuple[Column, str], list[Tray]] = defaultdict(list)
    manually_assigned: set[str] = set()
    for tray in rows:
        tray_key = str(tray.tray_id)
        assignment = assignments.get(tray_key)
        if assignment is None:
            continue
        column, trolley_id = assignment
        if not trolley_id:
            continue
        manual_groups[(column, trolley_id)].append(tray)
        manually_assigned.add(tray_key)

    queue_auto: list[Tray] = []
    precharge_auto: list[Tray] = []
    assembly_auto_candidates: list[Tray] = []

    for tray in rows:
        if str(tray.tray_id) in manually_assigned:
            continue
        column = choose_column(tray)
        if column is Column.QUEUE:
            queue_auto.append(tray)
            continue
        if column is Column.PRECHARGE:
            precharge_auto.append(tray)
            continue
        assembly_auto_candidates.append(tray)

    queue_trolleys = [
        *_manual_trolleys_for_column(Column.QUEUE, manual_groups),
        *_chunk_to_auto_trolleys(Column.QUEUE, queue_auto, max_per_trolley=max_per_trolley),
    ]
    precharge_trolleys = [
        *_manual_trolleys_for_column(Column.PRECHARGE, manual_groups),
        *_chunk_to_auto_trolleys(Column.PRECHARGE, precharge_auto, max_per_trolley=max_per_trolley),
    ]

    assembly_capacity = max(assembly_auto_trolley_count, 0) * max_per_trolley
    if assembly_capacity > 0:
        assembly_auto = assembly_auto_candidates[:assembly_capacity]
        assembly_ungrouped = assembly_auto_candidates[assembly_capacity:]
    else:
        assembly_auto = []
        assembly_ungrouped = assembly_auto_candidates

    assembly_trolleys = [
        *_manual_trolleys_for_column(Column.ASSEMBLY, manual_groups),
        *_chunk_to_auto_trolleys(Column.ASSEMBLY, assembly_auto, max_per_trolley=max_per_trolley),
    ]
    return TrolleyProjection(
        assembly_trolleys=assembly_trolleys,
        queue_trolleys=queue_trolleys,
        precharge_trolleys=precharge_trolleys,
        assembly_ungrouped=assembly_ungrouped,
    )


def _manual_trolleys_for_column(
    column: Column,
    manual_groups: Mapping[tuple[Column, str], list[Tray]],
) -> list[Trolley]:
    out: list[Trolley] = []
    keys = sorted(
        [key for key in manual_groups if key[0] is column],
        key=lambda key: key[1],
    )
    for _, trolley_id in keys:
        trays = manual_groups[(column, trolley_id)]
        out.append(
            Trolley(
                trolley_id=TrolleyId(trolley_id),
                column=column,
                tray_ids=[tray.tray_id for tray in trays],
                mode=TrolleyMode.MANUAL,
            )
        )
    return out


def _chunk_to_auto_trolleys(
    column: Column,
    trays: list[Tray],
    *,
    max_per_trolley: int,
) -> list[Trolley]:
    if not trays:
        return []
    out: list[Trolley] = []
    prefix = _column_prefix(column)
    idx = 1
    for i in range(0, len(trays), max_per_trolley):
        chunk = trays[i : i + max_per_trolley]
        out.append(
            Trolley(
                trolley_id=TrolleyId(f"AUTO-{prefix}-{idx}"),
                column=column,
                tray_ids=[tray.tray_id for tray in chunk],
                mode=TrolleyMode.AUTO,
            )
        )
        idx += 1
    return out


def _column_prefix(column: Column) -> str:
    if column is Column.ASSEMBLY:
        return "ASM"
    if column is Column.QUEUE:
        return "QUE"
    return "PRE"


def _tray_sort_key(tray: Tray) -> tuple[datetime, str]:
    return (tray.latest_collected_time or datetime.min, str(tray.tray_id))
