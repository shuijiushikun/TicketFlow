from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import DateTime, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from src.db.base import Base, TimestampMixin

if TYPE_CHECKING:
    from src.models.ticket_type import TicketType


class Event(Base, TimestampMixin):
    """活动数据模型，保存标题、时间和举办地址，并关联该活动的多个票种。"""

    __tablename__ = "events"

    id: Mapped[int] = mapped_column(primary_key=True)

    title: Mapped[str] = mapped_column(String(100), index=True, nullable=False)
    date: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    country: Mapped[str] = mapped_column(String(100), nullable=False)
    city: Mapped[str] = mapped_column(String(100), nullable=False)
    street_address: Mapped[str] = mapped_column(String(100), nullable=False)

    # 一对多关系（1:N）
    ticket_types: Mapped[list["TicketType"]] = relationship(
        "TicketType", back_populates="event"
    )
