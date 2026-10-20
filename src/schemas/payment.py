from pydantic import BaseModel, ConfigDict, Field, PositiveInt

from src.models.ticket import TicketStatus


class TicketPaymentSchema(BaseModel):
    """模拟支付成功后的响应，包含票据 ID 和更新后的状态。"""

    model_config = ConfigDict(from_attributes=True)

    id: PositiveInt = Field(description="完成模拟支付的票据 ID")
    status: TicketStatus = Field(
        description="票据状态，模拟支付成功后为 SOLD（已售出）"
    )
