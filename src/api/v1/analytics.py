from typing import Annotated

from fastapi import APIRouter, Depends

from src.api.deps import get_current_superuser, get_hotspot_tracker
from src.models import User
from src.schemas.hotspot import HotspotReport
from src.services.hotspot import HotspotTracker

router = APIRouter()


@router.get("/hot-events", response_model=HotspotReport, summary="查看规则热点活动")
async def get_hot_events(
    admin_user: Annotated[User, Depends(get_current_superuser)],
    tracker: Annotated[HotspotTracker, Depends(get_hotspot_tracker)],
) -> HotspotReport:
    """管理员查看近期成功详情访问形成的热点候选及比较窗口可用性。"""
    return await tracker.report()
