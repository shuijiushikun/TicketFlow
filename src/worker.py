import time
from collections import Counter
from datetime import UTC, datetime, timedelta
from typing import Any

from arq import cron
from arq.connections import RedisSettings
from redis.asyncio import Redis
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from src.core import logger, settings
from src.core.redis_keys import RedisKeys
from src.models import Ticket, TicketType
from src.models.ticket import TicketStatus


async def startup(ctx: dict[str, Any]) -> None:
    """初始化后台任务需要的数据库引擎、会话工厂和业务 Redis 连接。

    ARQ 自带的队列连接使用 1 号库；缓存、库存和预订记录通过独立连接访问 0 号库。
    """
    logger.info("正在启动 ARQ 后台任务进程……")

    engine = create_async_engine(
        settings.DATABASE_URL, echo=settings.ENVIRONMENT == "dev", future=True
    )

    ctx["engine"] = engine
    ctx["session_maker"] = async_sessionmaker(
        bind=engine, class_=AsyncSession, expire_on_commit=False, autoflush=False
    )
    # ARQ 的 ctx["redis"] 连接 1 号队列库；库存和预订记录使用 0 号业务库。
    ctx["business_redis"] = Redis.from_url(settings.REDIS_URL, decode_responses=True)

    logger.info("后台任务进程已启动")


async def shutdown(ctx: dict[str, Any]) -> None:
    """后台进程关闭时释放业务 Redis 连接和数据库引擎。"""
    logger.info("正在关闭 ARQ 后台任务进程……")

    try:
        await ctx["business_redis"].aclose()
    finally:
        await ctx["engine"].dispose()

    logger.info("后台任务进程已关闭")


async def release_unpaid_ticket(ctx: dict[Any, Any], ticket_id: int) -> None:
    """到期检查指定票据，并回收尚未支付的预订。

    ctx 提供数据库会话工厂和业务 Redis，ticket_id 是待检查的票据 ID。
    已付款或不存在的票据直接跳过；未付款的票据改为 CANCELED 并减少占用数量。
    先提交数据库，再幂等回补 Redis；已取消票据重试时不会再次减少占用数量。
    """
    session_maker = ctx["session_maker"]
    redis: Redis = ctx["business_redis"]

    async with session_maker() as session:
        try:
            query = select(Ticket).where(Ticket.id == ticket_id).with_for_update()
            ticket = await session.scalar(query)

            if not ticket:
                logger.info(f"后台回收：票据 {ticket_id} 不存在，跳过处理")
                return

            if ticket.status == TicketStatus.SOLD:
                logger.info(
                    f"后台回收：票据 {ticket_id} 当前状态为 {ticket.status.name}，"
                    f"跳过处理"
                )
                return

            ticket_type_id = ticket.ticket_type_id
            if ticket.status == TicketStatus.RESERVED:
                ticket.status = TicketStatus.CANCELED
                update_query = (
                    update(TicketType)
                    .where(TicketType.id == ticket_type_id)
                    .values(tickets_sold=TicketType.tickets_sold - 1)
                )
                await session.execute(update_query)

            await session.commit()

            await RedisKeys.refund_inventory_idempotent(
                redis=redis, ticket_type_id=ticket_type_id, ticket_id=ticket.id
            )

            logger.info(
                f"后台回收：票据 {ticket_id} 因超时已取消（CANCELED），库存回补完成"
            )
        except Exception as e:
            await session.rollback()
            logger.error(f"后台回收：票据 {ticket_id} 释放失败，错误：{e}")
            raise


async def reconcile_hung_tickets(ctx: dict[Any, Any]) -> None:
    """定时扫描数据库中超过预留期限的 RESERVED 票据，补偿遗漏的延迟任务。

    批量取消票据并按票种减少占用数量，提交数据库后逐张幂等回补 Redis 库存。
    """
    session_maker = ctx["session_maker"]
    redis: Redis = ctx["business_redis"]

    threshold = datetime.now(UTC) - timedelta(
        seconds=settings.TICKET_RESERVATION_TIME_SECONDS
    )

    async with session_maker() as session:
        cancel_query = (
            update(Ticket)
            .where(Ticket.status == TicketStatus.RESERVED)
            .where(Ticket.created_at <= threshold)
            .values(status=TicketStatus.CANCELED)
            .returning(Ticket.id, Ticket.ticket_type_id)
        )
        result = await session.execute(cancel_query)
        canceled_tickets = result.all()

        if not canceled_tickets:
            return

        logger.info(f"定时补偿：发现 {len(canceled_tickets)} 张超时未支付的票据")

        type_count = Counter(t.ticket_type_id for t in canceled_tickets)

        for ticket_type_id, count in type_count.items():
            refund_query = (
                update(TicketType)
                .where(TicketType.id == ticket_type_id)
                .values(tickets_sold=TicketType.tickets_sold - count)
            )
            await session.execute(refund_query)

        await session.commit()

        for ticket_id, ticket_type_id in canceled_tickets:
            await RedisKeys.refund_inventory_idempotent(
                redis=redis, ticket_type_id=ticket_type_id, ticket_id=ticket_id
            )

        logger.info("定时补偿完成：数据库占用数量和 Redis 库存已更新")


async def reconcile_redis_orphans(ctx: dict[Any, Any]) -> None:
    """排查 Redis 中没有有效数据库预订对应的异常占用。

    预留 60 秒宽限期以避开尚未提交的事务；数据库票据不存在或已取消时，幂等回补库存。
    """
    session_maker = ctx["session_maker"]
    redis: Redis = ctx["business_redis"]

    active_redis_locks = await RedisKeys.get_all_active_reservations(redis)

    if not active_redis_locks:
        return

    current_time = int(time.time())
    GRACE_PERIOD_SECONDS = 60

    suspicious_locks = [
        lock
        for lock in active_redis_locks
        if (current_time - lock["timestamp"]) > GRACE_PERIOD_SECONDS
    ]

    if not suspicious_locks:
        return

    redis_ticket_ids = [lock["ticket_id"] for lock in suspicious_locks]

    async with session_maker() as session:
        query = select(Ticket.id, Ticket.status).where(Ticket.id.in_(redis_ticket_ids))
        result = await session.execute(query)
        db_tickets = {row.id: row.status for row in result.all()}

    orphaned_locks = []

    for lock in suspicious_locks:
        ticket_id = lock["ticket_id"]
        db_status = db_tickets.get(ticket_id)

        if db_status is None or db_status == TicketStatus.CANCELED:
            orphaned_locks.append(lock)

    logger.info(f"Redis 补偿：发现 {len(orphaned_locks)} 条异常预订占用")

    if not orphaned_locks:
        return

    for lock in orphaned_locks:
        await RedisKeys.refund_inventory_idempotent(
            redis=redis,
            ticket_type_id=lock["ticket_type_id"],
            ticket_id=lock["ticket_id"],
        )

    logger.info("Redis 补偿完成：异常预订占用已回补")


class WorkerSettings:
    """ARQ 后台进程配置，注册延迟回收函数和定时补偿任务。"""

    redis_settings = RedisSettings(
        host=settings.REDIS_HOST, port=settings.REDIS_PORT, database=1
    )

    functions = [release_unpaid_ticket]
    on_startup = startup
    on_shutdown = shutdown

    cron_jobs = [
        cron(reconcile_hung_tickets, minute=set(range(0, 60, 5))),
        cron(reconcile_redis_orphans, minute=set(range(2, 60, 5))),
    ]
