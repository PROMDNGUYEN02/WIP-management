from __future__ import annotations

from datetime import datetime
from typing import Any

from wip_management.domain.models.tray import Tray, TrayStatus


def tray_to_row_payload(tray: Tray) -> dict[str, Any]:
    return {
        "tray_id": str(tray.tray_id),
        "status": tray.status.value,
        "latest_collected_time": tray.latest_collected_time.isoformat()
        if tray.latest_collected_time
        else None,
        "updated_at": tray.updated_at.isoformat() if tray.updated_at else None,
        "ccu_payload": tray.ccu_payload,
        "fpc_payload": tray.fpc_payload,
    }


def filter_trays_for_precharge(trays: list[Tray]) -> list[Tray]:
    return [tray for tray in trays if tray.status is TrayStatus.PRECHARGE]


def filter_trays_for_queue(trays: list[Tray]) -> list[Tray]:
    return [tray for tray in trays if tray.status is TrayStatus.QUEUE]


def filter_trays_for_assembly(trays: list[Tray]) -> list[Tray]:
    return [tray for tray in trays if tray.status is TrayStatus.UNKNOWN]


def parse_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    text = str(value).strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None
