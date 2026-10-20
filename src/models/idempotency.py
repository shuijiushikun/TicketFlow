from datetime import datetime
from typing import Any

from sqlalchemy import JSON, DateTime, String, UniqueConstraint, func
from sqlalchemy.orm import Mapped, mapped_column

from src.db.base import Base


class IdempotencyRequest(Base):
    """与业务事务一起提交的永久请求去重记录及原始响应快照。"""

    __tablename__ = "idempotency_requests"
    __table_args__ = (
        UniqueConstraint(
            "user_id", "action", "idempotency_key", name="uq_idempotency_request_scope"
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    # 不设用户外键，保留用户删除后的操作审计和已使用幂等键。
    user_id: Mapped[int] = mapped_column(nullable=False)
    action: Mapped[str] = mapped_column(String(64), nullable=False)
    idempotency_key: Mapped[str] = mapped_column(String(100), nullable=False)
    payload_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    owner_token: Mapped[str] = mapped_column(String(32), nullable=False)
    response: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
