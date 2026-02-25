from __future__ import annotations

from pydantic import BaseModel, Field
from fastapi import APIRouter

from wip_management.application.services.orchestrator import OrchestratorService
from wip_management.domain.models.trolley import Column


class ManualGroupRequest(BaseModel):
    tray_id: str = Field(min_length=1)
    trolley_id: str = Field(min_length=1)
    column: Column


class ManualUngroupRequest(BaseModel):
    tray_id: str = Field(min_length=1)


def build_http_router(orchestrator: OrchestratorService) -> APIRouter:
    router = APIRouter()

    @router.get("/health")
    async def health() -> dict:
        wm = await orchestrator.current_watermark()
        return {
            "status": "ok",
            "tray_count": await orchestrator.tray_count(),
            "watermark": {
                "collected_time": wm.collected_time.isoformat() if wm else None,
                "tray_id": wm.tray_id if wm else None,
            },
        }

    @router.get("/trays")
    async def trays(limit: int = 500) -> dict:
        rows = await orchestrator.tray_snapshot(limit=limit)
        return {"count": len(rows), "rows": rows}

    @router.get("/projection")
    async def projection() -> dict:
        return await orchestrator.projection_snapshot()

    @router.post("/refresh/manual")
    async def manual_refresh(full_scan: bool = False) -> dict:
        return await orchestrator.manual_refresh(full_scan=full_scan)

    @router.post("/group/manual")
    async def manual_group(req: ManualGroupRequest) -> dict:
        await orchestrator.group_tray_manual(req.tray_id, req.trolley_id, req.column)
        return {"status": "ok"}

    @router.post("/group/manual/ungroup")
    async def manual_ungroup(req: ManualUngroupRequest) -> dict:
        await orchestrator.ungroup_tray_manual(req.tray_id)
        return {"status": "ok"}

    return router
