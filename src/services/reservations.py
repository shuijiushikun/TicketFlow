"""请求与后台任务共用的预订状态变更，调用方负责锁定票据和提交。"""

from datetime import UTC, datetime, timedelta

from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession

from src.core import settings
from src.models import Ticket, TicketType
from src.models.ticket import TicketStatus
from src.services.inventory import INVENTORY_SYNC, RESERVATION_CLEANUP
from src.services.outbox import enqueue_outbox


def reservation_expired(ticket: Ticket, now: datetime | None = None) -> bool:
    """按创建时间计算支付期限；SQLite 的无时区测试值按 UTC 解释。"""
    created_at = ticket.created_at
    if created_at.tzinfo is None:
        created_at = created_at.replace(tzinfo=UTC)
    return (now or datetime.now(UTC)) >= created_at + timedelta(
        seconds=settings.TICKET_RESERVATION_TIME_SECONDS
    )


async def cancel_reservation(session: AsyncSession, ticket: Ticket) -> list[int]:
    """只释放一次数据库占用；同步和旧标记清理随本次事务持久化。"""
    if ticket.status == TicketStatus.SOLD:
        raise ValueError("不能取消已支付票据")
    if ticket.status == TicketStatus.RESERVED:
        result = await session.scalar(
            update(TicketType)
            .where(TicketType.id == ticket.ticket_type_id, TicketType.tickets_sold > 0)
            .values(
                tickets_sold=TicketType.tickets_sold - 1,
                inventory_version=TicketType.inventory_version + 1,
            )
            .returning(TicketType.id)
        )
        if result is None:
            raise ValueError("票据占用数量异常，拒绝释放并等待人工核对")
        ticket.status = TicketStatus.CANCELED
    sync = await enqueue_outbox(session, INVENTORY_SYNC, ticket.ticket_type_id)
    cleanup = await enqueue_outbox(session, RESERVATION_CLEANUP, ticket.id)
    return [sync.id, cleanup.id]
