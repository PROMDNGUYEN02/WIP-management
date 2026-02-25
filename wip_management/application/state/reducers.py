from __future__ import annotations

from datetime import datetime

from wip_management.domain.models.tray import Tray, TraySignal
from wip_management.domain.state_machine import TrayStateMachine


def apply_signal_to_tray(tray: Tray, signal: TraySignal, state_machine: TrayStateMachine) -> bool:
    changed = tray.apply_signal(signal)
    next_status = state_machine.evaluate(tray)
    if tray.status != next_status:
        tray.status = next_status
        tray.updated_at = datetime.now()
        changed = True
    return changed


def tray_desc_sort_key(tray: Tray) -> tuple[datetime, str]:
    return (tray.latest_collected_time or datetime.min, str(tray.tray_id))

