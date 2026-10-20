from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, PositiveInt

from src.schemas.ticket_type import TicketTypeResponse


class EventBase(BaseModel):
    """活动的公共字段定义，用于校验标题、带时区的日期和举办地址。"""

    model_config = ConfigDict(str_strip_whitespace=True)

    title: str = Field(
        ...,
        min_length=1,
        max_length=100,
        description="活动标题",
        examples=["校园音乐会"],
    )
    date: AwareDatetime = Field(
        ...,
        description="活动时间，必须包含时区，例如 +08:00 表示北京时间",
        examples=["2026-01-01T14:15:45Z"],
    )

    country: str = Field(
        ...,
        min_length=1,
        max_length=100,
        description="活动所在国家",
        examples=["中国"],
    )
    city: str = Field(
        ...,
        min_length=1,
        max_length=100,
        description="活动所在城市",
        examples=["北京"],
    )
    street_address: str = Field(
        ...,
        min_length=1,
        max_length=100,
        description="活动详细地址",
        examples=["大学生活动中心"],
    )


class EventCreate(EventBase):
    """创建活动的请求体，继承活动的公共字段。"""

    pass


class EventResponse(EventBase):
    """活动基本信息响应，包含活动 ID。"""

    model_config = ConfigDict(from_attributes=True)

    id: PositiveInt = Field(description="活动 ID，创建票种时通过 event_id 关联")


class EventDetailResponse(EventResponse):
    """活动详情响应，包含可用票种列表。"""

    ticket_types: list[TicketTypeResponse] = Field(description="该活动下的票种列表")

    model_config = ConfigDict(from_attributes=True)


class EventUpdate(BaseModel):
    """修改活动的请求体，所有字段均可按需提供。"""

    model_config = ConfigDict(str_strip_whitespace=True)

    title: str | None = Field(
        None,
        min_length=1,
        max_length=100,
        description="活动标题",
        examples=["校园音乐会"],
    )
    date: AwareDatetime | None = Field(
        None, description="活动时间，必须包含时区", examples=["2026-01-01T14:15:45Z"]
    )

    country: str | None = Field(
        None,
        min_length=1,
        max_length=100,
        description="活动所在国家",
        examples=["中国"],
    )
    city: str | None = Field(
        None,
        min_length=1,
        max_length=100,
        description="活动所在城市",
        examples=["北京"],
    )
    street_address: str | None = Field(
        None,
        min_length=1,
        max_length=100,
        description="活动详细地址",
        examples=["大学生活动中心"],
    )
