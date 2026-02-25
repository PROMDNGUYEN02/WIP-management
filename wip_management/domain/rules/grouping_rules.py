from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime

from wip_management.domain.models.tray import Tray, TrayId, TrayStatus
from wip_management.domain.models.trolley import (
    Column,
    Trolley,
    TrolleyId,
    TrolleyMode,
    TrolleyState,
)


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
    auto_group_enabled: bool = True,
) -> TrolleyProjection:
    rows = sorted(trays, key=_tray_sort_key, reverse=True)
    assignments = manual_assignments or {}
    tray_by_id = {str(tray.tray_id): tray for tray in rows}

    manual_groups: dict[str, list[Tray]] = defaultdict(list)
    manually_assigned: set[str] = set()
    for tray in rows:
        tray_key = str(tray.tray_id)
        assignment = assignments.get(tray_key)
        if assignment is None:
            continue
        _, trolley_id = assignment
        if not trolley_id:
            continue
        manual_groups[trolley_id].append(tray)
        manually_assigned.add(tray_key)

    queue_candidates: list[Tray] = []
    precharge_candidates: list[Tray] = []
    assembly_ungrouped: list[Tray] = []

    for tray in rows:
        if str(tray.tray_id) in manually_assigned:
            continue
        if not auto_group_enabled:
            assembly_ungrouped.append(tray)
            continue
        column = choose_column(tray)
        if column is Column.QUEUE:
            queue_candidates.append(tray)
            continue
        if column is Column.PRECHARGE:
            precharge_candidates.append(tray)
            continue
        assembly_ungrouped.append(tray)

    if auto_group_enabled:
        assembly_trolleys, queue_auto_trolleys = _split_auto_queue_between_assembly_and_queue(
            queue_candidates,
            max_per_trolley=max_per_trolley,
            assembly_auto_trolley_count=assembly_auto_trolley_count,
        )
    else:
        assembly_trolleys, queue_auto_trolleys = [], []

    manual_queue_trolleys = _manual_trolleys(manual_groups, tray_by_id)
    queue_trolleys, moved_to_precharge = _split_queue_and_precharge(
        [*manual_queue_trolleys, *queue_auto_trolleys],
        tray_by_id=tray_by_id,
    )

    precharge_auto = (
        _chunk_to_auto_trolleys(
            Column.PRECHARGE,
            precharge_candidates,
            max_per_trolley=max_per_trolley,
            state=TrolleyState.WAITING,
        )
        if auto_group_enabled
        else []
    )
    precharge_trolleys = [
        *moved_to_precharge,
        *[
            _with_precharge_state(trolley, tray_by_id=tray_by_id)
            for trolley in precharge_auto
        ],
    ]

    return TrolleyProjection(
        assembly_trolleys=assembly_trolleys,
        queue_trolleys=queue_trolleys,
        precharge_trolleys=precharge_trolleys,
        assembly_ungrouped=assembly_ungrouped,
    )


def _manual_trolleys(
    manual_groups: Mapping[str, list[Tray]],
    tray_by_id: Mapping[str, Tray],
) -> list[Trolley]:
    out: list[Trolley] = []
    for trolley_id in sorted(manual_groups.keys()):
        trays = manual_groups[trolley_id]
        out.append(
            _build_trolley(
                trolley_id=trolley_id,
                column=Column.QUEUE,
                tray_ids=[tray.tray_id for tray in trays],
                tray_by_id=tray_by_id,
                mode=TrolleyMode.MANUAL,
                state=TrolleyState.AGING,
            )
        )
    return out


def _chunk_to_auto_trolleys(
    column: Column,
    trays: list[Tray],
    *,
    max_per_trolley: int,
    state: TrolleyState,
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
                state=state,
                tray_quantity=len(chunk),
                cell_quantity=sum(_tray_cell_quantity(tray) for tray in chunk),
            )
        )
        idx += 1
    return out


def _split_auto_queue_between_assembly_and_queue(
    queue_candidates: list[Tray],
    *,
    max_per_trolley: int,
    assembly_auto_trolley_count: int,
) -> tuple[list[Trolley], list[Trolley]]:
    chunks: list[list[Tray]] = []
    for i in range(0, len(queue_candidates), max_per_trolley):
        chunks.append(queue_candidates[i : i + max_per_trolley])

    assembly_trolleys: list[Trolley] = []
    queue_trolleys: list[Trolley] = []
    assembly_slots = max(assembly_auto_trolley_count, 0)
    assembly_assigned = 0
    asm_idx = 1
    que_idx = 1

    for chunk in chunks:
        if len(chunk) < max_per_trolley and assembly_assigned < assembly_slots:
            assembly_trolleys.append(
                Trolley(
                    trolley_id=TrolleyId(f"AUTO-ASM-{asm_idx}"),
                    column=Column.ASSEMBLY,
                    tray_ids=[tray.tray_id for tray in chunk],
                    mode=TrolleyMode.AUTO,
                    state=TrolleyState.STACKING,
                    tray_quantity=len(chunk),
                    cell_quantity=sum(_tray_cell_quantity(tray) for tray in chunk),
                )
            )
            assembly_assigned += 1
            asm_idx += 1
            continue

        queue_trolleys.append(
            Trolley(
                trolley_id=TrolleyId(f"AUTO-QUE-{que_idx}"),
                column=Column.QUEUE,
                tray_ids=[tray.tray_id for tray in chunk],
                mode=TrolleyMode.AUTO,
                state=TrolleyState.AGING,
                tray_quantity=len(chunk),
                cell_quantity=sum(_tray_cell_quantity(tray) for tray in chunk),
            )
        )
        que_idx += 1

    return assembly_trolleys, queue_trolleys


def _split_queue_and_precharge(
    queue_trolleys: list[Trolley],
    *,
    tray_by_id: Mapping[str, Tray],
) -> tuple[list[Trolley], list[Trolley]]:
    queue_out: list[Trolley] = []
    precharge_out: list[Trolley] = []
    for trolley in queue_trolleys:
        trays = [tray_by_id.get(str(tray_id)) for tray_id in trolley.tray_ids]
        trays = [tray for tray in trays if tray is not None]
        if any(tray.has_ccu and tray.has_fpc for tray in trays):
            moved = Trolley(
                trolley_id=trolley.trolley_id,
                column=Column.PRECHARGE,
                tray_ids=trolley.tray_ids,
                mode=trolley.mode,
                state=TrolleyState.WAITING,
                tray_quantity=trolley.tray_quantity,
                cell_quantity=trolley.cell_quantity,
            )
            precharge_out.append(_with_precharge_state(moved, tray_by_id=tray_by_id))
            continue
        queue_out.append(trolley)
    return queue_out, precharge_out


def _with_precharge_state(trolley: Trolley, *, tray_by_id: Mapping[str, Tray]) -> Trolley:
    trays = [tray_by_id.get(str(tray_id)) for tray_id in trolley.tray_ids]
    trays = [tray for tray in trays if tray is not None]
    completed = bool(trays) and all(tray.has_ccu and tray.has_fpc for tray in trays)
    return Trolley(
        trolley_id=trolley.trolley_id,
        column=Column.PRECHARGE,
        tray_ids=trolley.tray_ids,
        mode=trolley.mode,
        state=TrolleyState.COMPLETED if completed else TrolleyState.WAITING,
        tray_quantity=trolley.tray_quantity,
        cell_quantity=trolley.cell_quantity,
    )


def _build_trolley(
    *,
    trolley_id: str,
    column: Column,
    tray_ids: list[TrayId],
    tray_by_id: Mapping[str, Tray],
    mode: TrolleyMode,
    state: TrolleyState,
) -> Trolley:
    trays = [tray_by_id.get(str(tray_id)) for tray_id in tray_ids]
    trays = [tray for tray in trays if tray is not None]
    return Trolley(
        trolley_id=TrolleyId(trolley_id),
        column=column,
        tray_ids=tray_ids,
        mode=mode,
        state=state,
        tray_quantity=len(tray_ids),
        cell_quantity=sum(_tray_cell_quantity(tray) for tray in trays),
    )


def _tray_cell_quantity(tray: Tray) -> int:
    payload = tray.ccu_payload or {}
    raw = payload.get("quantity")
    if isinstance(raw, int):
        return max(raw, 0)
    try:
        return max(int(str(raw)), 0)
    except Exception:  # noqa: BLE001
        return 0


def _column_prefix(column: Column) -> str:
    if column is Column.ASSEMBLY:
        return "ASM"
    if column is Column.QUEUE:
        return "QUE"
    return "PRE"


def _tray_sort_key(tray: Tray) -> tuple[datetime, str]:
    return (tray.latest_collected_time or datetime.min, str(tray.tray_id))
