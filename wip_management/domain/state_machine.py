from __future__ import annotations

from wip_management.domain.models.tray import Tray, TrayStatus
from wip_management.domain.rules.classifier import classify_tray_status


class TrayStateMachine:
    @staticmethod
    def evaluate(tray: Tray) -> TrayStatus:
        return classify_tray_status(tray)

