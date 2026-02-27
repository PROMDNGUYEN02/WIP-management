from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime
import re

from wip_management.config import settings
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


_TRO_ID_PATTERN = re.compile(r"^TRO-(\d+)$", re.IGNORECASE)


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
    total_trolley_count: int = 99,
) -> TrolleyProjection:
    rows = sorted(trays, key=_tray_sort_key, reverse=True)
    assignments = manual_assignments or {}
    tray_by_id = {str(tray.tray_id): tray for tray in rows}

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
    manual_trolley_ids = sorted({trolley_id for _, trolley_id in manual_groups.keys()})
    trolley_id_allocator = _TrolleyIdAllocator(
        total_count=max(1, int(total_trolley_count)),
        manual_trolley_ids=manual_trolley_ids,
    )

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
            if _tray_assembly_out_time(tray) is None:
                # Queue auto-group must follow Assembly_Out FIFO; keep trays without Assembly_Out in ungroup list.
                assembly_ungrouped.append(tray)
                continue
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
            gap_minutes=max(0, int(getattr(settings, "auto_group_gap_minutes", 10))),
            trolley_id_allocator=trolley_id_allocator,
        )
    else:
        assembly_trolleys, queue_auto_trolleys = [], []

    manual_trolleys = _manual_trolleys(manual_groups, tray_by_id)
    manual_assembly_trolleys = [trolley for trolley in manual_trolleys if trolley.column is Column.ASSEMBLY]
    manual_queue_trolleys = [trolley for trolley in manual_trolleys if trolley.column is Column.QUEUE]
    manual_precharge_trolleys = [trolley for trolley in manual_trolleys if trolley.column is Column.PRECHARGE]

    assembly_trolleys = [*manual_assembly_trolleys, *assembly_trolleys]
    queue_trolleys, moved_to_precharge = _split_queue_and_precharge(
        [*manual_queue_trolleys, *queue_auto_trolleys],
        tray_by_id=tray_by_id,
    )

    precharge_auto = (
        _chunk_to_auto_trolleys(
            Column.PRECHARGE,
            sorted(
                precharge_candidates,
                key=lambda tray: (_tray_assembly_out_time(tray) or datetime.max, str(tray.tray_id)),
                reverse=False,
            ),
            max_per_trolley=max_per_trolley,
            state=TrolleyState.WAITING,
            trolley_id_allocator=trolley_id_allocator,
        )
        if auto_group_enabled
        else []
    )
    precharge_trolleys = [
        *[
            _with_precharge_state(trolley, tray_by_id=tray_by_id)
            for trolley in manual_precharge_trolleys
        ],
        *moved_to_precharge,
        *[
            _with_precharge_state(trolley, tray_by_id=tray_by_id)
            for trolley in precharge_auto
        ],
    ]
    _reassign_auto_trolley_ids_by_assembly_out(
        assembly_trolleys=assembly_trolleys,
        queue_trolleys=queue_trolleys,
        precharge_trolleys=precharge_trolleys,
        total_trolley_count=max(1, int(total_trolley_count)),
        manual_trolley_ids=manual_trolley_ids,
        tray_by_id=tray_by_id,
    )
    assembly_trolleys = _sort_trolleys_by_latest_time(assembly_trolleys, tray_by_id=tray_by_id)
    queue_trolleys = _sort_trolleys_by_latest_time(queue_trolleys, tray_by_id=tray_by_id)
    precharge_trolleys = _sort_trolleys_by_latest_time(precharge_trolleys, tray_by_id=tray_by_id)

    return TrolleyProjection(
        assembly_trolleys=assembly_trolleys,
        queue_trolleys=queue_trolleys,
        precharge_trolleys=precharge_trolleys,
        assembly_ungrouped=assembly_ungrouped,
    )


def _manual_trolleys(
    manual_groups: Mapping[tuple[Column, str], list[Tray]],
    tray_by_id: Mapping[str, Tray],
) -> list[Trolley]:
    out: list[Trolley] = []
    for key in sorted(manual_groups.keys(), key=lambda item: (item[0].value, item[1])):
        column, trolley_id = key
        trays = manual_groups[key]
        if column is Column.ASSEMBLY:
            state = TrolleyState.STACKING
        elif column is Column.PRECHARGE:
            state = TrolleyState.WAITING
        else:
            state = TrolleyState.AGING
        out.append(
            _build_trolley(
                trolley_id=trolley_id,
                column=column,
                tray_ids=[tray.tray_id for tray in trays],
                tray_by_id=tray_by_id,
                mode=TrolleyMode.MANUAL,
                state=state,
            )
        )
    return out


def _chunk_to_auto_trolleys(
    column: Column,
    trays: list[Tray],
    *,
    max_per_trolley: int,
    state: TrolleyState,
    trolley_id_allocator: "_TrolleyIdAllocator",
) -> list[Trolley]:
    if not trays:
        return []
    out: list[Trolley] = []
    for i in range(0, len(trays), max_per_trolley):
        chunk = trays[i : i + max_per_trolley]
        out.append(
            Trolley(
                trolley_id=TrolleyId(trolley_id_allocator.next_id()),
                column=column,
                tray_ids=[tray.tray_id for tray in chunk],
                mode=TrolleyMode.AUTO,
                state=state,
                tray_quantity=len(chunk),
                cell_quantity=sum(_tray_cell_quantity(tray) for tray in chunk),
            )
        )
    return out


def _split_auto_queue_between_assembly_and_queue(
    queue_candidates: list[Tray],
    *,
    max_per_trolley: int,
    assembly_auto_trolley_count: int,
    gap_minutes: int,
    trolley_id_allocator: "_TrolleyIdAllocator",
) -> tuple[list[Trolley], list[Trolley]]:
    ordered_candidates = sorted(
        queue_candidates,
        key=lambda tray: (_tray_assembly_out_time(tray) or datetime.max, str(tray.tray_id)),
        reverse=False,
    )
    _ = gap_minutes
    chunks: list[list[Tray]] = []
    current_chunk: list[Tray] = []
    for tray in ordered_candidates:
        current_chunk.append(tray)
        if len(current_chunk) >= max_per_trolley:
            chunks.append(current_chunk)
            current_chunk = []
    if current_chunk:
        chunks.append(current_chunk)

    assembly_trolleys: list[Trolley] = []
    queue_trolleys: list[Trolley] = []
    assembly_slots = max(assembly_auto_trolley_count, 0)
    assembly_assigned = 0

    for chunk in chunks:
        if len(chunk) < max_per_trolley and assembly_assigned < assembly_slots:
            assembly_trolleys.append(
                Trolley(
                    trolley_id=TrolleyId(trolley_id_allocator.next_id()),
                    column=Column.ASSEMBLY,
                    tray_ids=[tray.tray_id for tray in chunk],
                    mode=TrolleyMode.AUTO,
                    state=TrolleyState.STACKING,
                    tray_quantity=len(chunk),
                    cell_quantity=sum(_tray_cell_quantity(tray) for tray in chunk),
                )
            )
            assembly_assigned += 1
            continue

        queue_trolleys.append(
            Trolley(
                trolley_id=TrolleyId(trolley_id_allocator.next_id()),
                column=Column.QUEUE,
                tray_ids=[tray.tray_id for tray in chunk],
                mode=TrolleyMode.AUTO,
                state=TrolleyState.AGING,
                tray_quantity=len(chunk),
                cell_quantity=sum(_tray_cell_quantity(tray) for tray in chunk),
            )
        )

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


class _TrolleyIdAllocator:
    def __init__(self, *, total_count: int, manual_trolley_ids: list[str]) -> None:
        self._total_count = max(1, int(total_count))
        self._width = max(2, len(str(self._total_count)))
        self._reserved_seqs: set[int] = {
            seq
            for trolley_id in manual_trolley_ids
            if (seq := _parse_tro_seq(trolley_id, total_count=self._total_count)) is not None
        }
        self._assigned_auto_seqs: set[int] = set()
        if self._reserved_seqs:
            start_after = max(self._reserved_seqs)
        else:
            start_after = 0
        self._cursor = (start_after % self._total_count) + 1

    def next_id(self) -> str:
        for _ in range(self._total_count):
            seq = self._cursor
            self._cursor = (self._cursor % self._total_count) + 1
            if seq in self._reserved_seqs:
                continue
            if seq in self._assigned_auto_seqs:
                continue
            self._assigned_auto_seqs.add(seq)
            return _format_tro_id(seq, width=self._width)
        seq = self._cursor
        self._cursor = (self._cursor % self._total_count) + 1
        return _format_tro_id(seq, width=self._width)


def _parse_tro_seq(trolley_id: str, *, total_count: int) -> int | None:
    text = str(trolley_id).strip().upper()
    if not text:
        return None
    match = _TRO_ID_PATTERN.match(text)
    if not match:
        return None
    try:
        seq = int(match.group(1))
    except Exception:  # noqa: BLE001
        return None
    if seq < 1 or seq > total_count:
        return None
    return seq


def _format_tro_id(seq: int, *, width: int) -> str:
    safe_seq = max(1, int(seq))
    safe_width = max(2, int(width))
    return f"TRO-{safe_seq:0{safe_width}d}"


def _sort_trolleys_by_latest_time(
    trolleys: list[Trolley],
    *,
    tray_by_id: Mapping[str, Tray],
) -> list[Trolley]:
    return sorted(
        trolleys,
        key=lambda trolley: (_trolley_latest_time(trolley, tray_by_id=tray_by_id), str(trolley.trolley_id)),
        reverse=True,
    )


def _reassign_auto_trolley_ids_by_assembly_out(
    *,
    assembly_trolleys: list[Trolley],
    queue_trolleys: list[Trolley],
    precharge_trolleys: list[Trolley],
    total_trolley_count: int,
    manual_trolley_ids: list[str],
    tray_by_id: Mapping[str, Tray],
) -> None:
    allocator = _TrolleyIdAllocator(
        total_count=max(1, int(total_trolley_count)),
        manual_trolley_ids=list(manual_trolley_ids),
    )
    indexed: list[tuple[datetime, str, str, int]] = []
    for idx, trolley in enumerate(assembly_trolleys):
        if trolley.mode is TrolleyMode.AUTO:
            indexed.append((_trolley_oldest_assembly_out_time(trolley, tray_by_id=tray_by_id), str(trolley.trolley_id), "assembly", idx))
    for idx, trolley in enumerate(queue_trolleys):
        if trolley.mode is TrolleyMode.AUTO:
            indexed.append((_trolley_oldest_assembly_out_time(trolley, tray_by_id=tray_by_id), str(trolley.trolley_id), "queue", idx))
    for idx, trolley in enumerate(precharge_trolleys):
        if trolley.mode is TrolleyMode.AUTO:
            indexed.append((_trolley_oldest_assembly_out_time(trolley, tray_by_id=tray_by_id), str(trolley.trolley_id), "precharge", idx))
    indexed.sort(key=lambda item: (item[0], item[1]))
    for _, _, bucket, idx in indexed:
        next_id = TrolleyId(allocator.next_id())
        if bucket == "assembly":
            assembly_trolleys[idx] = _replace_trolley_id(assembly_trolleys[idx], next_id)
        elif bucket == "queue":
            queue_trolleys[idx] = _replace_trolley_id(queue_trolleys[idx], next_id)
        else:
            precharge_trolleys[idx] = _replace_trolley_id(precharge_trolleys[idx], next_id)


def _replace_trolley_id(trolley: Trolley, trolley_id: TrolleyId) -> Trolley:
    return Trolley(
        trolley_id=trolley_id,
        column=trolley.column,
        tray_ids=trolley.tray_ids,
        mode=trolley.mode,
        state=trolley.state,
        tray_quantity=trolley.tray_quantity,
        cell_quantity=trolley.cell_quantity,
    )


def _trolley_oldest_assembly_out_time(trolley: Trolley, *, tray_by_id: Mapping[str, Tray]) -> datetime:
    oldest: datetime | None = None
    for tray_id in trolley.tray_ids:
        tray = tray_by_id.get(str(tray_id))
        if tray is None:
            continue
        tray_time = _tray_assembly_out_time(tray)
        if tray_time is None:
            continue
        if oldest is None or tray_time < oldest:
            oldest = tray_time
    return oldest or datetime.max


def _trolley_latest_time(trolley: Trolley, *, tray_by_id: Mapping[str, Tray]) -> datetime:
    latest = datetime.min
    for tray_id in trolley.tray_ids:
        tray = tray_by_id.get(str(tray_id))
        if tray is None:
            continue
        tray_time = tray.latest_collected_time or _tray_assembly_out_time(tray) or datetime.min
        if tray_time > latest:
            latest = tray_time
    return latest


def _tray_sort_key(tray: Tray) -> tuple[datetime, str]:
    return (tray.latest_collected_time or datetime.min, str(tray.tray_id))


def _tray_assembly_out_time(tray: Tray) -> datetime | None:
    payload = tray.ccu_payload or {}
    raw = payload.get("end_time")
    if isinstance(raw, datetime):
        return raw
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if parsed.tzinfo is not None:
            return parsed.astimezone().replace(tzinfo=None)
        return parsed
    except ValueError:
        return None
