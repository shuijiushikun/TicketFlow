from typing import TYPE_CHECKING

from sqlalchemy import CheckConstraint, ForeignKey, Integer, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from src.db.base import Base, TimestampMixin

if TYPE_CHECKING:
    from src.models.event import Event
    from src.models.ticket import Ticket


class TicketType(Base, TimestampMixin):
    """活动票种模型，保存名称、价格、总票数和已占用数量。

    price 以分为单位；tickets_quantity 是总票数；tickets_sold 包含预留和已售出的票。
    数据库约束要求价格非负、总票数大于零、占用数量介于零和总票数之间。
    """

    __tablename__ = "ticket_types"

    id: Mapped[int] = mapped_column(primary_key=True)

    name: Mapped[str] = mapped_column(String(50), nullable=False)
    price: Mapped[int] = mapped_column(Integer, nullable=False)

    tickets_quantity: Mapped[int] = mapped_column(Integer, nullable=False)
    tickets_sold: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    # 一对多关系（1:N）
    tickets: Mapped[list["Ticket"]] = relationship(
        "Ticket", back_populates="ticket_type"
    )

    event_id: Mapped[int] = mapped_column(
        ForeignKey("events.id", ondelete="RESTRICT"), nullable=False
    )
    # 多对一关系（N:1）
    event: Mapped["Event"] = relationship("Event", back_populates="ticket_types")

    __table_args__ = (
        CheckConstraint(sqltext="price >= 0", name="check_ticket_price"),
        CheckConstraint(sqltext="tickets_quantity > 0", name="check_ticket_quantity"),
        CheckConstraint(sqltext="tickets_sold >= 0", name="check_ticket_sold"),
        CheckConstraint(
            sqltext="tickets_quantity >= tickets_sold", name="check_sold_limit"
        ),
    )
