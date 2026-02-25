from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any, NewType

TrayId = NewType("TrayId", str)


class SignalSource(StrEnum):
    CCU = "ccu"
    FPC = "fpc"


class TrayStatus(StrEnum):
    UNKNOWN = "Unknown"
    PRECHARGE = "Precharge"
    QUEUE = "Queue"


@dataclass(slots=True, frozen=True)
class Watermark:
    collected_time: datetime
    tray_id: str


@dataclass(slots=True, frozen=True)
class TraySignal:
    source: SignalSource
    tray_id: TrayId
    collected_time: datetime
    payload: dict[str, Any]

    @property
    def dedup_key(self) -> tuple[str, str, datetime, str]:
        payload_fingerprint = json.dumps(self.payload, default=str, sort_keys=True)
        return (self.source.value, str(self.tray_id), self.collected_time, payload_fingerprint)


@dataclass(slots=True)
class Tray:
    tray_id: TrayId
    status: TrayStatus = TrayStatus.UNKNOWN
    latest_collected_time: datetime | None = None
    ccu_payload: dict[str, Any] | None = None
    fpc_payload: dict[str, Any] | None = None
    updated_at: datetime | None = None

    def apply_signal(self, signal: TraySignal) -> bool:
        changed = False

        if self.latest_collected_time is None or signal.collected_time >= self.latest_collected_time:
            self.latest_collected_time = signal.collected_time
            changed = True

        if signal.source is SignalSource.CCU:
            if self.ccu_payload != signal.payload:
                self.ccu_payload = signal.payload
                changed = True
        else:
            if self.fpc_payload != signal.payload:
                self.fpc_payload = signal.payload
                changed = True

        if changed:
            self.updated_at = datetime.now()
        return changed

    @property
    def has_ccu(self) -> bool:
        return self.ccu_payload is not None

    @property
    def has_fpc(self) -> bool:
        return self.fpc_payload is not None

    def clone(self) -> "Tray":
        return Tray(
            tray_id=self.tray_id,
            status=self.status,
            latest_collected_time=self.latest_collected_time,
            ccu_payload=self.ccu_payload.copy() if self.ccu_payload else None,
            fpc_payload=self.fpc_payload.copy() if self.fpc_payload else None,
            updated_at=self.updated_at,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "tray_id": str(self.tray_id),
            "status": self.status.value,
            "latest_collected_time": self.latest_collected_time.isoformat()
            if self.latest_collected_time
            else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
            "ccu_payload": self.ccu_payload,
            "fpc_payload": self.fpc_payload,
        }
