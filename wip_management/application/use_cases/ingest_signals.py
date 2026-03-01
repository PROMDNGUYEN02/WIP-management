# wip_management/application/use_cases/ingest_signals.py
from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
import logging

from wip_management.application.ports import DashboardRepoPort
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
    def __init__(
        self,
        store: SingleWriterStateStore,
        *,
        dashboard_repo: DashboardRepoPort | None = None,
    ) -> None:
        self._store = store
        self._merger = DeltaMerger()
        self._dashboard_repo = dashboard_repo

    async def execute(
        self,
        ccu_signals: Iterable[TraySignal],
        fpc_signals: Iterable[TraySignal],
        previous_watermark: Watermark | None,
    ) -> IngestResult:
        ccu_list = list(ccu_signals)
        fpc_list = list(fpc_signals)

        aligned_fpc, remapped_count, unmapped_count = self._align_fpc_to_ccu(
            ccu_signals=ccu_list,
            fpc_signals=fpc_list,
        )

        merged = self._merger.merge_and_dedup(ccu_list, aligned_fpc)
        if self._dashboard_repo is not None and merged:
            await self._dashboard_repo.ingest_signals(merged)
        store_result = await self._store.apply_signals(merged)
        next_wm = self._merger.next_watermark(previous_watermark, merged)
        log.info(
            "Ingest execute merged=%s changed=%s missing_ccu=%s "
            "remapped_fpc_by_lot=%s unmapped_fpc_by_lot=%s next_wm=%s",
            len(merged),
            len(store_result.changed),
            len(store_result.missing_ccu_tray_ids),
            remapped_count,
            unmapped_count,
            next_wm.collected_time.isoformat() if next_wm else None,
        )
        return IngestResult(merged=merged, store_result=store_result, watermark=next_wm)

    def _align_fpc_to_ccu(
        self,
        *,
        ccu_signals: list[TraySignal],
        fpc_signals: list[TraySignal],
    ) -> tuple[list[TraySignal], int, int]:
        """
        Align FPC signals to CCU tray IDs using two strategies:

        1. Direct tray_id match  – FPC tray_id already equals a known CCU
           tray_id → pass through unchanged (fastest, most accurate).

        2. Lot-number remap      – FPC tray_id unknown but its lot_no matches
           exactly one CCU tray.  Only applied when CCU lot is unambiguous
           (one CCU tray per lot).  The CCU lot_no field name ("LOT_NO") is
           used; the FPC field used for lot matching is also "lot_no" from the
           FPC payload (populated from Cell_barcode or LOT_NO field).
        """
        # ── Build CCU lookup indexes ─────────────────────────────────────────
        ccu_tray_ids: set[str] = {str(s.tray_id).upper() for s in ccu_signals}

        # lot_no → set of CCU tray_ids that share that lot
        ccu_by_lot: dict[str, set[str]] = {}
        for signal in ccu_signals:
            lot_key = _lot_key_from_payload(signal.payload)
            if not lot_key:
                continue
            ccu_by_lot.setdefault(lot_key, set()).add(str(signal.tray_id).upper())

        # Only use lot mapping when a lot maps to exactly ONE CCU tray
        unique_ccu_by_lot: dict[str, str] = {
            lot: next(iter(tray_ids))
            for lot, tray_ids in ccu_by_lot.items()
            if len(tray_ids) == 1
        }

        remapped = 0
        unmapped = 0
        out: list[TraySignal] = []

        for signal in fpc_signals:
            fpc_tray_id = str(signal.tray_id).upper()

            # ── Strategy 1: direct tray_id match ────────────────────────────
            if fpc_tray_id in ccu_tray_ids:
                out.append(signal)
                continue

            # ── Strategy 2: lot-number remap ────────────────────────────────
            lot_key = _lot_key_from_payload(signal.payload)
            if lot_key:
                mapped_tray_id = unique_ccu_by_lot.get(lot_key)
                if mapped_tray_id and mapped_tray_id != fpc_tray_id:
                    payload = dict(signal.payload)
                    payload["source_tray_id"] = fpc_tray_id
                    payload["matched_by"] = "lot_no"
                    out.append(
                        TraySignal(
                            source=signal.source,
                            tray_id=TrayId(mapped_tray_id),
                            collected_time=signal.collected_time,
                            payload=payload,
                        )
                    )
                    remapped += 1
                    continue

            # ── No match found ───────────────────────────────────────────────
            out.append(signal)
            unmapped += 1

        return out, remapped, unmapped


def _lot_key_from_payload(payload: dict) -> str:
    """
    Extract normalized lot key from a signal payload.

    Both CCU and FPC payloads store the lot under the key ``"lot_no"``.
    CCU populates it from the LOT_NO JSON field;
    FPC populates it from Cell_barcode (or LOT_NO as fallback).
    We only need to read the unified key here.
    """
    raw = payload.get("lot_no")
    if raw is None:
        return ""
    return str(raw).strip().upper()