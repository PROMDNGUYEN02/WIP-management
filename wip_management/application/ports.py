from __future__ import annotations

import asyncio
from collections.abc import Iterable
from datetime import datetime
from typing import Any, Protocol

from wip_management.domain.models.tray import TraySignal, Watermark


class ClockPort(Protocol):
    def now(self) -> datetime: ...


class EventBusPort(Protocol):
    async def publish(self, event: Any) -> None: ...

    async def subscribe(self, maxsize: int | None = None) -> asyncio.Queue[Any]: ...

    async def unsubscribe(self, queue: asyncio.Queue[Any]) -> None: ...


class SignalRepoPort(Protocol):
    async def fetch_initial(self, start_time: datetime, end_time: datetime) -> list[TraySignal]: ...

    async def fetch_delta(self, watermark: Watermark, end_time: datetime) -> list[TraySignal]: ...

    async def peek_latest_signal_time(self, end_time: datetime) -> datetime | None: ...


class CcuRepoPort(SignalRepoPort, Protocol):
    async def fetch_by_tray_ids(self, tray_ids: Iterable[str], end_time: datetime) -> list[TraySignal]: ...

    async def fetch_tray_cells(
        self,
        tray_id: str,
        start_time: datetime,
        end_time: datetime,
    ) -> list[dict[str, str | None]]: ...

    async def fetch_cell_owner(
        self,
        cell_id: str,
        start_time: datetime,
        end_time: datetime,
    ) -> dict[str, str | None] | None: ...


class FpcRepoPort(SignalRepoPort, Protocol):
    pass


class DeltaTrackerPort(Protocol):
    async def get(self) -> Watermark | None: ...

    async def set(self, watermark: Watermark) -> None: ...


class NotifierPort(Protocol):
    async def notify(self, payload: dict[str, Any]) -> None: ...


class GroupingStateRepoPort(Protocol):
    async def load_manual_assignments(self) -> dict[str, tuple[str, str]]: ...

    async def set_manual_assignment(self, tray_id: str, column: str, trolley_id: str) -> None: ...

    async def remove_manual_assignment(self, tray_id: str) -> None: ...

    async def replace_manual_assignments(self, assignments: dict[str, tuple[str, str]]) -> None: ...

    async def save_projection(self, projection: dict[str, Any]) -> None: ...
