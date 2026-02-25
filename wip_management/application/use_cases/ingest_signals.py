from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

from wip_management.application.state.state_store import SingleWriterStateStore, StoreApplyResult
from wip_management.domain.models.tray import TraySignal, Watermark


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
        merged = self._merger.merge_and_dedup(ccu_signals, fpc_signals)
        store_result = await self._store.apply_signals(merged)
        next_wm = self._merger.next_watermark(previous_watermark, merged)
        return IngestResult(merged=merged, store_result=store_result, watermark=next_wm)

