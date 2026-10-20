"""数据库库存的可恢复 Redis 投影；不参与扣票或结算决策。"""

import asyncio
from collections.abc import Awaitable
from typing import Any, cast

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.core.cache_runtime import cache_circuit
from src.core.cache_store import EventCacheStore
from src.core.config import settings
from src.core.redis_keys import RedisClient, RedisKeys
from src.models import Ticket, TicketType
from src.models.outbox import OutboxEvent
from src.models.ticket import TicketStatus
from src.services.outbox import (
    EVENT_CACHE_INVALIDATE,
    OutboxDispatcher,
    event_cache_handler,
)

INVENTORY_SYNC = "inventory_sync"
RESERVATION_CLEANUP = "reservation_cleanup"

# 补零后的十进制字符串支持完整 PostgreSQL BIGINT，避免 Lua 浮点数精度丢失。
PROJECT_INVENTORY = """
    local current = redis.call('GET', KEYS[2])
    if current and current > ARGV[1] then
        return 0
    end
    redis.call('SET', KEYS[2], ARGV[1])
    if ARGV[2] == 'deleted' then
        redis.call('DEL', KEYS[1])
    else
        redis.call('SET', KEYS[1], ARGV[2])
    end
    return 1
"""


async def project_inventory(
    session: AsyncSession, redis: RedisClient, ticket_type_id: int
) -> None:
    """锁定当前数据库票种后同步最新剩余量，旧版本命令不能覆盖新版本。"""
    ticket_type = await session.scalar(
        select(TicketType)
        .where(TicketType.id == ticket_type_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    # 删除后的 ID 不复用；墓碑高于全部有效 BIGINT 版本，拒绝迟到写入。
    version = ticket_type.inventory_version if ticket_type else 2**63
    available = (
        str(ticket_type.tickets_quantity - ticket_type.tickets_sold)
        if ticket_type
        else "deleted"
    )
    async with asyncio.timeout(settings.TRANSACTION_REDIS_TIMEOUT_SECONDS):
        await cast(
            Awaitable[Any],
            redis.eval(  # type: ignore[no-untyped-call]
                PROJECT_INVENTORY,
                2,
                RedisKeys.ticket_type_inventory(ticket_type_id),
                RedisKeys.inventory_version(ticket_type_id),
                str(version).zfill(20),
                available,
            ),
        )


def transaction_outbox(
    session: AsyncSession,
    redis: RedisClient,
    cache_redis: RedisClient | None = None,
) -> OutboxDispatcher:
    """为写服务装配独立投递事务，库存和查询缓存使用各自连接。"""

    async def inventory_handler(db: AsyncSession, event: OutboxEvent) -> None:
        await project_inventory(db, redis, event.aggregate_id)

    async def cleanup_handler(db: AsyncSession, event: OutboxEvent) -> None:
        ticket = await db.scalar(
            select(Ticket)
            .where(Ticket.id == event.aggregate_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        if ticket is not None and ticket.status == TicketStatus.RESERVED:
            raise ValueError("不能清理仍在预订状态的票据")
        async with asyncio.timeout(settings.TRANSACTION_REDIS_TIMEOUT_SECONDS):
            await cast(
                Awaitable[int],
                redis.hdel(
                    RedisKeys.active_reservations_hash(), str(event.aggregate_id)
                ),
            )

    return OutboxDispatcher(
        async_sessionmaker(session.bind, expire_on_commit=False),
        {
            EVENT_CACHE_INVALIDATE: event_cache_handler(
                EventCacheStore(
                    cache_redis if cache_redis is not None else redis, cache_circuit
                )
            ),
            INVENTORY_SYNC: inventory_handler,
            RESERVATION_CLEANUP: cleanup_handler,
        },
    )
