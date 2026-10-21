import asyncio
from collections import deque
from collections.abc import Awaitable
from datetime import UTC, datetime, timedelta
from typing import Any, cast

from arq import cron
from arq.connections import RedisSettings
from prometheus_client import start_http_server
from redis.asyncio import Redis
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from src.core import logger, settings
from src.core.cache_policy import CacheCircuitBreaker, DatabaseAdmission
from src.core.metrics import registry
from src.core.redis_keys import RedisClient, RedisKeys
from src.models import Ticket, TicketType
from src.models.ticket import TicketStatus
from src.services.cache_warmup import warm_hot_events
from src.services.inventory import (
    RESERVATION_CLEANUP,
    project_inventory,
    transaction_outbox,
)
from src.services.outbox import enqueue_outbox
from src.services.reservations import cancel_reservation, reservation_expired


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
    ctx["business_redis"] = Redis.from_url(
        settings.REDIS_URL,
        decode_responses=True,
        max_connections=10,
        socket_connect_timeout=settings.TRANSACTION_REDIS_TIMEOUT_SECONDS,
        socket_timeout=settings.TRANSACTION_REDIS_TIMEOUT_SECONDS,
        retry_on_timeout=False,
    )
    ctx["cache_redis"] = Redis.from_url(
        settings.REDIS_URL,
        decode_responses=True,
        max_connections=10,
        socket_connect_timeout=settings.CACHE_REDIS_TIMEOUT_SECONDS,
        socket_timeout=settings.CACHE_REDIS_TIMEOUT_SECONDS,
        retry_on_timeout=False,
    )
    ctx["cache_circuit"] = CacheCircuitBreaker(settings.CACHE_CIRCUIT_COOLDOWN_SECONDS)
    # 每个预热进程最多一个数据库查询，无排队；多进程部署需合并预算。
    ctx["warmup_admission"] = DatabaseAdmission(
        1,
        0,
        settings.CACHE_DB_QUEUE_TIMEOUT_SECONDS,
        settings.CACHE_DB_QUERY_TIMEOUT_SECONDS,
        budget="warmup",
    )
    if settings.WORKER_METRICS_ENABLED:
        ctx["metrics_server"] = start_http_server(
            settings.WORKER_METRICS_PORT, addr="0.0.0.0", registry=registry
        )

    logger.info("后台任务进程已启动")


async def shutdown(ctx: dict[str, Any]) -> None:
    """后台进程关闭时释放业务 Redis 连接和数据库引擎。"""
    logger.info("正在关闭 ARQ 后台任务进程……")

    try:
        await ctx["business_redis"].aclose()
    finally:
        try:
            if "cache_redis" in ctx:
                await ctx["cache_redis"].aclose()
        finally:
            await ctx["engine"].dispose()
            if "metrics_server" in ctx:
                server, thread = ctx["metrics_server"]
                server.shutdown()
                server.server_close()
                thread.join(timeout=1)

    logger.info("后台任务进程已关闭")


async def release_unpaid_ticket(ctx: dict[Any, Any], ticket_id: int) -> None:
    """锁定票据并复核实际期限，原子取消超时预订并持久化投递待办。"""
    session_maker = ctx["session_maker"]
    redis: RedisClient = ctx["business_redis"]

    async with session_maker() as session:
        try:
            query = (
                select(Ticket)
                .where(Ticket.id == ticket_id)
                .with_for_update(skip_locked=True)
                .execution_options(populate_existing=True)
            )
            ticket = await session.scalar(query)
            if ticket is None:
                return
            if ticket.status == TicketStatus.RESERVED and not reservation_expired(
                ticket
            ):
                logger.info("后台回收：票据 %s 尚未到期，保留预订", ticket_id)
                return
            if ticket.status == TicketStatus.SOLD:
                cleanup = await enqueue_outbox(session, RESERVATION_CLEANUP, ticket_id)
                pending_ids = [cleanup.id]
            else:
                pending_ids = await cancel_reservation(session, ticket)
            await session.commit()
        except BaseException:
            await session.rollback()
            raise
        dispatcher = transaction_outbox(session, redis, ctx.get("cache_redis"))

    for pending_id in pending_ids:
        await dispatcher.dispatch_best_effort(pending_id)
    logger.info("后台回收：票据 %s 状态已确认，后续同步可恢复", ticket_id)


async def reconcile_hung_tickets(ctx: dict[Any, Any]) -> None:
    """最多扫描 100 张超时票据，每票独立事务；一轮最多执行 5 秒。"""
    threshold = datetime.now(UTC) - timedelta(
        seconds=settings.TICKET_RESERVATION_TIME_SECONDS
    )
    try:
        async with asyncio.timeout(5):
            async with ctx["session_maker"]() as session:
                ids = list(
                    await session.scalars(
                        select(Ticket.id)
                        .where(
                            Ticket.status == TicketStatus.RESERVED,
                            Ticket.created_at <= threshold,
                        )
                        .order_by(Ticket.created_at, Ticket.id)
                        .limit(100)
                    )
                )
            for ticket_id in ids:
                try:
                    await release_unpaid_ticket(ctx, ticket_id)
                except Exception:
                    logger.exception("超时票据处理失败，继续下一张 id=%s", ticket_id)
    except TimeoutError:
        logger.warning("超时预订扫描达到 5 秒预算，剩余票据留待下轮")


async def reconcile_redis_orphans(ctx: dict[Any, Any]) -> None:
    """按游标重建库存副本并清理旧预订标记，不根据 Redis 记录增加库存。

    一轮最多投影 100 个票种、核对 100 个哈希字段，总预算为 5 秒。
    """
    try:
        async with asyncio.timeout(5):
            await _reconcile_inventory(ctx)
            await _cleanup_legacy_reservations(ctx)
    except TimeoutError:
        logger.warning("库存副本核对达到 5 秒预算，剩余项目留待下轮")


async def _reconcile_inventory(ctx: dict[Any, Any]) -> None:
    redis: RedisClient = ctx["business_redis"]
    async with ctx["session_maker"]() as session:
        ids = list(
            await session.scalars(
                select(TicketType.id)
                .where(TicketType.id > ctx.get("inventory_reconcile_cursor", 0))
                .order_by(TicketType.id)
                .limit(100)
            )
        )
    if not ids:
        ctx["inventory_reconcile_cursor"] = 0
        return
    for ticket_type_id in ids:
        async with ctx["session_maker"]() as session:
            try:
                await project_inventory(session, redis, ticket_type_id)
                await session.commit()
            except Exception:
                await session.rollback()
                logger.exception("库存副本投影失败，继续下一票种 id=%s", ticket_type_id)
            except BaseException:
                await session.rollback()
                raise
        # 单个坏记录不阻塞后续票种；下次完整游标轮转时仍会重试。
        ctx["inventory_reconcile_cursor"] = ticket_type_id


async def _cleanup_legacy_reservations(ctx: dict[Any, Any]) -> None:
    redis: RedisClient = ctx["business_redis"]
    pending: deque[str] = ctx.setdefault("legacy_reservation_fields", deque())
    if not pending:
        async with asyncio.timeout(settings.TRANSACTION_REDIS_TIMEOUT_SECONDS):
            cursor, fields = await redis.hscan(
                RedisKeys.active_reservations_hash(),
                cursor=ctx.get("legacy_reservation_cursor", 0),
                count=100,
            )
        ctx["legacy_reservation_cursor"] = cursor
        # COUNT 为提示；保留整页溢出字段，下轮继续且不丢弃游标进度。
        pending.extend(fields)
    batch = [pending.popleft() for _ in range(min(len(pending), 100))]
    if not batch:
        return
    ids: dict[str, int] = {}
    for field in batch:
        try:
            ticket_id = int(field)
        except (ValueError, TypeError):
            continue
        if ticket_id > 0 and str(ticket_id) == field:
            ids[field] = ticket_id
    async with ctx["session_maker"]() as session:
        result = await session.execute(
            select(Ticket.id, Ticket.status).where(Ticket.id.in_(ids.values()))
        )
        statuses = dict(result.all())
    cleanup = [
        field
        for field in batch
        if field not in ids or statuses.get(ids[field]) != TicketStatus.RESERVED
    ]
    if cleanup:
        async with asyncio.timeout(settings.TRANSACTION_REDIS_TIMEOUT_SECONDS):
            await cast(
                Awaitable[int],
                redis.hdel(RedisKeys.active_reservations_hash(), *cleanup),
            )


async def process_outbox(ctx: dict[Any, Any]) -> None:
    """每轮最多领取 1000 条，库存按票种合并；共用 5 秒执行预算。"""
    async with ctx["session_maker"]() as session:
        dispatcher = transaction_outbox(
            session, ctx["business_redis"], ctx.get("cache_redis")
        )
    await dispatcher.dispatch_batch(batch_size=1000, time_budget_seconds=5)


class WorkerSettings:
    """ARQ 后台进程配置，注册延迟回收函数和定时补偿任务。"""

    redis_settings = RedisSettings(
        host=settings.REDIS_HOST, port=settings.REDIS_PORT, database=1
    )

    functions = [release_unpaid_ticket, warm_hot_events]
    on_startup = startup
    on_shutdown = shutdown

    cron_jobs = [
        cron(warm_hot_events, second=10, unique=True, timeout=25),
        cron(process_outbox, second=set(range(0, 60, 10)), unique=True, timeout=10),
        cron(reconcile_hung_tickets, minute=set(range(0, 60, 5)), timeout=10),
        cron(reconcile_redis_orphans, minute=set(range(2, 60, 5)), timeout=10),
    ]
