"""抢票申请与查询响应，不把排队凭证解释为购票成功。"""

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field, PositiveInt

from src.models.seckill import SeckillStatus
from src.schemas.ticket import TicketCreate


class SeckillCreate(TicketCreate):
    """申请一张秒杀票，用户身份从登录凭证读取。"""


class SeckillResponse(BaseModel):
    """申请当前状态；成功后使用 ticket_id 进入原有票据和支付流程。"""

    model_config = ConfigDict(from_attributes=True)

    id: PositiveInt = Field(description="申请编号，用于查询本人申请；不是排队名次")
    ticket_type_id: PositiveInt = Field(description="申请的票种 ID")
    status: SeckillStatus = Field(description="排队中、成功、售罄、超时或已拒绝")
    expires_at: datetime = Field(
        description="排队截止时间，后台扣票时校验，与支付期限独立"
    )
    ticket_id: PositiveInt | None = Field(default=None, description="成功创建的票据 ID")
