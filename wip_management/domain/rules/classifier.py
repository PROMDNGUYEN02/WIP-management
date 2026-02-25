from __future__ import annotations

from wip_management.domain.models.tray import Tray, TrayStatus


def classify_tray_status(tray: Tray) -> TrayStatus:
    if tray.has_ccu and tray.has_fpc:
        return TrayStatus.PRECHARGE
    if tray.has_ccu and (not tray.has_fpc):
        return TrayStatus.QUEUE
    if (not tray.has_ccu) and tray.has_fpc:
        return TrayStatus.PRECHARGE
    return TrayStatus.UNKNOWN
