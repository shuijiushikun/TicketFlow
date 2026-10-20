from pydantic import BaseModel, ConfigDict, Field, PositiveInt

from src.models.ticket import TicketStatus


class TicketBase(BaseModel):
    """票据的公共字段，ticket_type_id 表示票种 ID。"""

    ticket_type_id: PositiveInt = Field(..., description="票种 ID，与具体票据 ID 不同")


class TicketCreate(TicketBase):
    """预订一张票的请求体，只需提供票种 ID。"""

    pass


class TicketResponse(TicketBase):
    """票据响应，包含票种 ID、具体票据 ID 和当前状态。"""

    model_config = ConfigDict(from_attributes=True)

    id: PositiveInt = Field(description="具体票据 ID，用于查询、支付和取消")
    status: TicketStatus = Field(
        description="票据状态：RESERVED（已预留）、SOLD（已售出）、CANCELED（已取消）"
    )


class TicketDetailResponse(TicketResponse):
    """票据详情响应，在基本票据信息上增加活动标题。"""

    event_title: str = Field(description="票据所属活动的标题")
