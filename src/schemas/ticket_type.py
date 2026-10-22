from pydantic import BaseModel, ConfigDict, Field, NonNegativeInt, PositiveInt


class TicketTypeBase(BaseModel):
    """票种公共字段：名称、以分计价的价格和总票数。"""

    model_config = ConfigDict(str_strip_whitespace=True)

    name: str = Field(
        ...,
        min_length=1,
        max_length=50,
        description="票种名称，例如普通票、VIP 票",
        examples=["普通票", "VIP 票"],
    )
    price: NonNegativeInt = Field(
        ..., ge=0, description="价格，单位为分；例如 5000 表示 50 元", examples=[10000]
    )

    tickets_quantity: int = Field(..., gt=0, description="该票种的总票数，必须大于零")


class TicketTypeCreate(TicketTypeBase):
    """创建票种的请求体，需要指定所属活动 ID。"""

    event_id: PositiveInt = Field(..., description="所属活动 ID")


class TicketTypeResponse(TicketTypeCreate):
    """票种基本信息响应，包含活动 ID 和票种 ID。"""

    model_config = ConfigDict(from_attributes=True)

    id: PositiveInt = Field(description="票种 ID，预订时作为 ticket_type_id 提交")


class TicketTypeDetailResponse(TicketTypeResponse):
    """票种详情响应，额外返回预留与已售出共同占用的数量。"""

    tickets_sold: NonNegativeInt = Field(
        description=(
            "已预留和已售出共同占用的数量；剩余库存为 tickets_quantity - tickets_sold"
        )
    )


class TicketTypeInventoryResponse(BaseModel):
    """供展示的余票快照；最终购票结果以数据库扣票为准。"""

    ticket_type_id: PositiveInt = Field(description="票种 ID")
    remaining: NonNegativeInt = Field(
        description="展示余票数，可能存在同步延迟，不作为购票成功或售罄的最终依据"
    )


class TicketTypeUpdate(BaseModel):
    """修改票种的请求体，可按需提供名称、价格和总票数。"""

    model_config = ConfigDict(str_strip_whitespace=True)

    name: str | None = Field(
        None,
        min_length=1,
        max_length=50,
        description="票种名称，例如普通票、VIP 票",
        examples=["普通票", "VIP 票"],
    )
    price: NonNegativeInt | None = Field(
        None, ge=0, description="价格，单位为分；例如 5000 表示 50 元", examples=[10000]
    )
    tickets_quantity: PositiveInt | None = Field(
        None, gt=0, description="该票种的总票数，必须大于零"
    )
