import enum
from typing import TYPE_CHECKING

from sqlalchemy import ForeignKey, Index
from sqlalchemy.orm import Mapped, mapped_column, relationship

from src.db.base import Base, TimestampMixin

if TYPE_CHECKING:
    from src.models.ticket_type import TicketType
    from src.models.user import User


class TicketStatus(enum.Enum):
    """票据的生命周期状态。

    RESERVED：已预留，暂时占用库存，等待付款。
    SOLD：已售出，模拟支付完成。
    CANCELED：已取消，因手动操作或超时而释放库存。
    状态值保留英文，供接口、数据库和业务逻辑共同使用。
    """

    RESERVED = "RESERVED"
    SOLD = "SOLD"
    CANCELED = "CANCELED"


class Ticket(Base, TimestampMixin):
    """分配给用户的具体票据，关联一个用户和一个票种，并记录当前状态。"""

    __tablename__ = "tickets"
    __table_args__ = (Index("ix_tickets_expiry", "status", "created_at", "id"),)

    id: Mapped[int] = mapped_column(primary_key=True)

    ticket_type_id: Mapped[int] = mapped_column(
        ForeignKey("ticket_types.id", ondelete="RESTRICT"), nullable=False
    )
    # 多对一关系（N:1）
    ticket_type: Mapped["TicketType"] = relationship(
        "TicketType", back_populates="tickets"
    )

    owner_id: Mapped[int] = mapped_column(
        ForeignKey("users.id", ondelete="RESTRICT"), nullable=False
    )
    owner: Mapped["User"] = relationship("User", back_populates="tickets")

    status: Mapped[TicketStatus] = mapped_column(
        default=TicketStatus.RESERVED, nullable=False, index=True
    )

    @property
    def event_title(self) -> str:
        """通过票据、票种和活动的关联获取活动标题；关联不可用时返回兜底文字。"""
        if self.ticket_type is not None and self.ticket_type.event is not None:
            return self.ticket_type.event.title
        return "未知活动"
