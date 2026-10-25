"""请求与后台任务共用的预订状态变更，调用方负责锁定票据和提交。"""

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from src.core import settings
from src.core.exception import (
    BookingModeError,
    TicketsSoldOutError,
    TicketTypeNotFoundError,
)
from src.models import Ticket, TicketType
from src.models.ticket import TicketStatus
from src.services.inventory import INVENTORY_SYNC, RESERVATION_CLEANUP
from src.services.outbox import enqueue_outbox


@dataclass
class ReservedTicket:
    """尚未提交的扣票结果及待同步的库存快照。"""

    ticket: Ticket
    outbox_id: int
    version: int
    remaining: int


async def reserve_ticket(
    session: AsyncSession, owner_id: int, ticket_type_id: int, *, seckill: bool = False
) -> ReservedTicket:
    """普通预约与秒杀共用条件扣票；由调用方统一提交业务事务。"""
    stock = (
        await session.execute(
            update(TicketType)
            .where(
                TicketType.id == ticket_type_id,
                TicketType.is_seckill == seckill,
                TicketType.tickets_sold < TicketType.tickets_quantity,
            )
            .values(
                tickets_sold=TicketType.tickets_sold + 1,
                inventory_version=TicketType.inventory_version + 1,
            )
            .returning(
                TicketType.tickets_quantity,
                TicketType.tickets_sold,
                TicketType.inventory_version,
            )
        )
    ).one_or_none()
    if stock is None:
        mode = await session.scalar(
            select(TicketType.is_seckill).where(TicketType.id == ticket_type_id)
        )
        if mode is None:
            raise TicketTypeNotFoundError("票种不存在")
        if mode != seckill:
            raise BookingModeError(
                "该票种请通过秒杀入口排队申请"
                if mode
                else "该票种请通过普通预约入口购票"
            )
        raise TicketsSoldOutError("该票种已售罄，暂无可用库存")
    ticket = Ticket(owner_id=owner_id, ticket_type_id=ticket_type_id)
    session.add(ticket)
    await session.flush()
    pending = await enqueue_outbox(session, INVENTORY_SYNC, ticket_type_id)
    return ReservedTicket(
        ticket,
        pending.id,
        stock.inventory_version,
        stock.tickets_quantity - stock.tickets_sold,
    )


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
