from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
import logging

from wip_management.application.state.state_store import SingleWriterStateStore, StoreApplyResult
from wip_management.domain.models.tray import TrayId, TraySignal, Watermark

log = logging.getLogger(__name__)


class DeltaMerger:
    @staticmethod
    def merge_and_dedup(
        ccu_signals: Iterable[TraySignal],
        fpc_signals: Iterable[TraySignal],
    ) -> list[TraySignal]:
        merged = sorted(
            [*ccu_signals, *fpc_signals],
            key=lambda s: (s.collected_time, str(s.tray_id), s.source.value),
        )
        out: list[TraySignal] = []
        seen: set[tuple[str, str, object, str]] = set()
        for signal in merged:
            key = signal.dedup_key
            if key in seen:
                continue
            seen.add(key)
            out.append(signal)
        return out

    @staticmethod
    def next_watermark(previous: Watermark | None, merged: list[TraySignal]) -> Watermark | None:
        if not merged:
            return previous
        last = merged[-1]
        return Watermark(collected_time=last.collected_time, tray_id=str(last.tray_id))


@dataclass(slots=True, frozen=True)
class IngestResult:
    merged: list[TraySignal]
    store_result: StoreApplyResult
    watermark: Watermark | None


class IngestSignalsUseCase:
    def __init__(self, store: SingleWriterStateStore) -> None:
        self._store = store
        self._merger = DeltaMerger()

    async def execute(
        self,
        ccu_signals: Iterable[TraySignal],
        fpc_signals: Iterable[TraySignal],
        previous_watermark: Watermark | None,
    ) -> IngestResult:
        ccu_list = list(ccu_signals)
        fpc_list = list(fpc_signals)
        aligned_fpc, remapped_count, unmapped_count = self._align_fpc_to_ccu_by_lot(
            ccu_signals=ccu_list,
            fpc_signals=fpc_list,
        )

        merged = self._merger.merge_and_dedup(ccu_list, aligned_fpc)
        store_result = await self._store.apply_signals(merged)
        next_wm = self._merger.next_watermark(previous_watermark, merged)
        log.info(
            "Ingest execute merged=%s changed=%s missing_ccu=%s remapped_fpc_by_lot=%s unmapped_fpc_by_lot=%s next_wm=%s",
            len(merged),
            len(store_result.changed),
            len(store_result.missing_ccu_tray_ids),
            remapped_count,
            unmapped_count,
            next_wm.collected_time.isoformat() if next_wm else None,
        )
        return IngestResult(merged=merged, store_result=store_result, watermark=next_wm)

    def _align_fpc_to_ccu_by_lot(
        self,
        *,
        ccu_signals: list[TraySignal],
        fpc_signals: list[TraySignal],
    ) -> tuple[list[TraySignal], int, int]:
        ccu_by_lot: dict[str, set[TrayId]] = {}
        for signal in ccu_signals:
            lot_key = _lot_key(signal.payload)
            if not lot_key:
                continue
            ccu_by_lot.setdefault(lot_key, set()).add(signal.tray_id)

        unique_ccu_by_lot: dict[str, TrayId] = {}
        for lot_key, tray_ids in ccu_by_lot.items():
            if len(tray_ids) == 1:
                unique_ccu_by_lot[lot_key] = next(iter(tray_ids))

        remapped = 0
        unmapped = 0
        out: list[TraySignal] = []
        for signal in fpc_signals:
            lot_key = _lot_key(signal.payload)
            if not lot_key:
                out.append(signal)
                unmapped += 1
                continue
            mapped = unique_ccu_by_lot.get(lot_key)
            if mapped is None:
                out.append(signal)
                unmapped += 1
                continue
            mapped_tray_id = mapped
            if str(mapped_tray_id) == str(signal.tray_id):
                out.append(signal)
                continue
            payload = dict(signal.payload)
            payload["source_tray_id"] = str(signal.tray_id)
            payload["matched_by"] = "lot_no"
            out.append(
                TraySignal(
                    source=signal.source,
                    tray_id=mapped_tray_id,
                    collected_time=signal.collected_time,
                    payload=payload,
                )
            )
            remapped += 1
        return out, remapped, unmapped


def _lot_key(payload: dict[str, object]) -> str:
    raw = payload.get("lot_no")
    if raw is None:
        return ""
    text = str(raw).strip().upper()
    return text
