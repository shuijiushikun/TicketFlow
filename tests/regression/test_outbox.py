"""Outbox 的必要快速检查，只使用临时 SQLite 与内存 Redis。"""

from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock

import pytest
from fakeredis.aioredis import FakeRedis
from redis.exceptions import ConnectionError
from sqlalchemy import func, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.core.cache_policy import CacheCircuitBreaker
from src.core.cache_store import EventCacheStore
from src.core.redis_keys import RedisKeys
from src.models import Event
from src.repositories.event import CachedEventRepository, EventRepository
from src.schemas.event import EventCreate
from src.services.event import EventService


def event_data() -> EventCreate:
    return EventCreate(
        title="Outbox 检查",
        date=datetime(2027, 1, 1, tzinfo=UTC),
        country="中国",
        city="上海",
        street_address="礼堂",
    )


def service(session: AsyncSession, redis: FakeRedis) -> EventService:
    class SQLiteEventRepository(EventRepository):
        async def create(self, event_data: EventCreate) -> Event:
            event = await super().create(event_data)
            # SQLite refresh 会丢失时区；恢复 PostgreSQL 的带时区返回契约。
            event.date = event.date.replace(tzinfo=UTC)
            return event

    base = SQLiteEventRepository(session)
    return EventService(session, base, CachedEventRepository(base, redis))


async def test_business_rollback_discards_pending_invalidation(
    session: AsyncSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    from src.models.outbox import OutboxEvent

    monkeypatch.setattr(session, "commit", AsyncMock(side_effect=SQLAlchemyError()))
    async with FakeRedis(decode_responses=True) as redis:
        with pytest.raises(SQLAlchemyError):
            await service(session, redis).create(event_data())
    assert await session.scalar(select(func.count()).select_from(Event)) == 0
    assert await session.scalar(select(func.count()).select_from(OutboxEvent)) == 0


async def test_committed_event_keeps_failed_invalidation_until_retry(
    session: AsyncSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    from src.models.outbox import OutboxEvent
    from src.services.outbox import OutboxDispatcher, event_cache_handler

    async with FakeRedis(decode_responses=True) as redis:
        real_eval = redis.eval
        monkeypatch.setattr(redis, "eval", AsyncMock(side_effect=ConnectionError()))
        started = datetime.now(UTC)
        created = await service(session, redis).create(event_data())
        assert await session.get(Event, created.id) is not None
        session.expire_all()  # 投递在独立会话中更新，业务会话不能复用旧 identity map。
        pending = (await session.scalars(select(OutboxEvent))).one()
        assert pending.aggregate_id == created.id and pending.attempts == 1
        assert pending.next_attempt_at.replace(tzinfo=UTC) > started
        dispatcher = OutboxDispatcher(
            async_sessionmaker(session.bind, expire_on_commit=False),
            {
                "event_cache_invalidate": event_cache_handler(
                    EventCacheStore(redis, CacheCircuitBreaker(0.1))
                )
            },
        )
        pending.next_attempt_at = datetime.now(UTC) + timedelta(days=1)
        await session.commit()
        assert not await dispatcher.dispatch(pending.id)
        monkeypatch.setattr(redis, "eval", real_eval)
        pending.next_attempt_at = datetime.now(UTC) - timedelta(seconds=1)
        await session.commit()
        assert await dispatcher.dispatch_batch(batch_size=1) == 1
        session.expire_all()
        assert await session.scalar(select(func.count()).select_from(OutboxEvent)) == 0


async def test_repeated_invalidation_is_safe_after_delivery_interruption(
    session: AsyncSession,
) -> None:
    from src.models.outbox import OutboxEvent
    from src.services.outbox import OutboxDispatcher, enqueue_outbox

    async with FakeRedis(decode_responses=True) as redis:
        store = EventCacheStore(redis, CacheCircuitBreaker(0.1))
        pending = await enqueue_outbox(session, "event_cache_invalidate", 999)
        await session.commit()
        first = True

        async def invalidate(db: AsyncSession, event: OutboxEvent) -> None:
            nonlocal first
            await store.invalidate(event.aggregate_id)
            if first:
                first = False
                raise RuntimeError("模拟投递成功后进程中断")

        dispatcher = OutboxDispatcher(
            async_sessionmaker(session.bind, expire_on_commit=False),
            {"event_cache_invalidate": invalidate},
        )
        assert not await dispatcher.dispatch(pending.id)
        generation = await redis.get(RedisKeys.event_generation(999))
        await session.refresh(pending)
        assert pending.attempts == 1
        pending.next_attempt_at = datetime.now(UTC) - timedelta(seconds=1)
        await session.commit()
        assert await dispatcher.dispatch(pending.id)
        assert await redis.get(RedisKeys.event_generation(999)) != generation
        assert not await dispatcher.dispatch(pending.id)
        assert await session.scalar(select(func.count()).select_from(OutboxEvent)) == 0


@pytest.mark.parametrize("fail_projection", [False, True])
async def test_inventory_batch_merges_only_selected_rows_and_preserves_retry(
    session: AsyncSession, ticket, redis_clients, fail_projection: bool
) -> None:
    from src.models.outbox import OutboxEvent
    from src.services.inventory import INVENTORY_SYNC, transaction_outbox
    from src.services.outbox import enqueue_outbox

    business, _ = redis_clients
    ticket_type_id = ticket.ticket_type_id
    for _ in range(5):
        await enqueue_outbox(session, INVENTORY_SYNC, ticket_type_id)
    await session.commit()
    real_eval = business.eval
    calls = 0

    async def project(*args, **kwargs):
        nonlocal calls
        calls += 1
        if fail_projection:
            raise ConnectionError("模拟整组投影失败")
        return await real_eval(*args, **kwargs)

    with pytest.MonkeyPatch.context() as patcher:
        patcher.setattr(business, "eval", project)
        # 本轮只领取 3 条，同票种的另外 2 条不能因合并投影被误删。
        delivered = await transaction_outbox(session, business).dispatch_batch(
            batch_size=3
        )
    assert calls == 1
    assert delivered == (0 if fail_projection else 3)
    session.expire_all()
    pending = list(await session.scalars(select(OutboxEvent).order_by(OutboxEvent.id)))
    assert len(pending) == (5 if fail_projection else 2)
    assert [item.attempts for item in pending] == (
        [1, 1, 1, 0, 0] if fail_projection else [0, 0]
    )
    # 后续恢复时，相同票种的剩余待办仍能成功投递。
    for item in pending:
        item.next_attempt_at = datetime.now(UTC) - timedelta(seconds=1)
    await session.commit()
    assert await transaction_outbox(session, business).dispatch_batch() == len(pending)
    assert await business.get(RedisKeys.ticket_type_inventory(ticket_type_id)) == "4"
    assert await session.scalar(select(func.count(OutboxEvent.id))) == 0
