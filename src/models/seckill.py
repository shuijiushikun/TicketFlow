"""异步抢票申请；接收申请与实际占用库存是两个独立步骤。"""

import enum
from datetime import datetime

from sqlalchemy import CheckConstraint, DateTime, ForeignKey, Index, String
from sqlalchemy.orm import Mapped, mapped_column

from src.db.base import Base, TimestampMixin


class SeckillStatus(enum.StrEnum):
    """申请状态；成功表示已经生成待支付票据。"""

    QUEUED = "QUEUED"
    SUCCEEDED = "SUCCEEDED"
    SOLD_OUT = "SOLD_OUT"
    EXPIRED = "EXPIRED"
    REJECTED = "REJECTED"


class SeckillRequest(Base, TimestampMixin):
    """保存申请所有者、截止时间和最终票据，支持队列故障后恢复。"""

    __tablename__ = "seckill_requests"
    __table_args__ = (
        CheckConstraint(
            "status IN ('QUEUED','SUCCEEDED','SOLD_OUT','EXPIRED','REJECTED')",
            name="seckill_status",
        ),
        CheckConstraint(
            "(status = 'SUCCEEDED' AND ticket_id IS NOT NULL) OR "
            "(status <> 'SUCCEEDED' AND ticket_id IS NULL)",
            name="seckill_result",
        ),
        Index("ix_seckill_requests_recovery", "status", "next_dispatch_at", "id"),
        Index("ix_seckill_requests_owner", "owner_id", "id"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    owner_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="RESTRICT"))
    ticket_type_id: Mapped[int] = mapped_column(
        ForeignKey("ticket_types.id", ondelete="RESTRICT")
    )
    status: Mapped[str] = mapped_column(
        String(16), default=SeckillStatus.QUEUED.value, server_default="QUEUED"
    )
    admission_token: Mapped[str] = mapped_column(String(32), unique=True)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    next_dispatch_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    ticket_id: Mapped[int | None] = mapped_column(
        ForeignKey("tickets.id", ondelete="RESTRICT"), unique=True
    )
