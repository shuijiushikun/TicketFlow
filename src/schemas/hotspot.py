from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field


class HotEvent(BaseModel):
    """根据已观测访问量识别的预热候选，不代表购票或支付需求。"""

    event_id: int
    recent_views: int
    previous_views: int | None
    growth_ratio: float | None
    reason: Literal["volume", "growth"]


class HotspotReport(BaseModel):
    """最近两个完整分钟的热点及统计窗口可用性。"""

    generated_at: datetime
    window_start: datetime
    window_end: datetime
    observed_buckets: int = Field(ge=0, le=4)
    comparison_available: bool
    hot_events: list[HotEvent]
