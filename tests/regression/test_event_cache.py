"""缓存治理的快速行为检查；完整故障矩阵和压测留最终验收。"""

import asyncio
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest
import pytest_asyncio
from fakeredis.aioredis import FakeRedis
from redis.exceptions import ConnectionError
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from src.api import deps
from src.core.cache_lock import CacheLock
from src.core.cache_policy import (
    CacheCircuitBreaker,
    CacheUnavailableError,
    DatabaseAdmission,
    QueryRateLimiter,
)
from src.core.config import settings
from src.core.exception import EventNotFoundError
from src.core.redis_keys import RedisKeys
from src.main import app
from src.models import Ticket, TicketType
from src.repositories.event import CachedEventRepository, EventRepository
from src.schemas.event import EventDetailResponse
from src.schemas.ticket_type import TicketTypeCreate, TicketTypeUpdate
from src.services.ticket_type import TicketTypeService


def detail(title: str = "音乐会") -> EventDetailResponse:
    return EventDetailResponse(
        id=1,
        title=title,
        date=datetime(2027, 1, 1, tzinfo=UTC),
        country="中国",
        city="北京",
        street_address="礼堂",
        ticket_types=[],
    )


@pytest_asyncio.fixture
async def cache() -> AsyncIterator[FakeRedis]:
    client = FakeRedis(decode_responses=True)
    yield client
    await client.aclose()


def repository(cache: FakeRedis) -> CachedEventRepository:
    base = MagicMock(spec=EventRepository)
    base.get = AsyncMock(return_value=detail())
    base.get_all = AsyncMock(return_value=[detail()])
    return CachedEventRepository(
        base,
        cache,
        admission=DatabaseAdmission(2, 4, 0.05, 1),
        circuit=CacheCircuitBreaker(0.1),
    )


async def test_negative_cache_and_create_invalidation(cache: FakeRedis) -> None:
    repo = repository(cache)
    repo.repo.get.return_value = None
    for _ in range(2):
        with pytest.raises(EventNotFoundError):
            await repo.get(1)
    assert repo.repo.get.await_count == 1
    ttl = await cache.ttl(RedisKeys.event_static(1))
    assert 0 < ttl <= 60
    repo.repo.get.return_value = detail()
    await repo.invalidate_event(1)
    assert (await repo.get(1)).title == "音乐会"
    assert repo.repo.get.await_count == 2


@pytest.mark.parametrize("listing", [False, True])
async def test_cold_hot_key_only_loads_once(cache: FakeRedis, listing: bool) -> None:
    repo = repository(cache)
    started, release = asyncio.Event(), asyncio.Event()

    async def load(*args: int) -> object:
        started.set()
        await release.wait()
        return [detail()] if listing else detail()

    loader = repo.repo.get_all if listing else repo.repo.get
    loader.side_effect = load

    async def query() -> object:
        return await repo.get_all(0, 20) if listing else await repo.get(1)

    tasks = [asyncio.create_task(query()) for _ in range(12)]
    await asyncio.wait_for(started.wait(), 1)
    release.set()
    results = await asyncio.gather(*tasks)
    assert len(results) == 12
    assert loader.await_count == 1


@pytest.mark.parametrize("listing", [False, True])
async def test_old_reader_cannot_fill_after_invalidation(
    cache: FakeRedis, listing: bool
) -> None:
    repo = repository(cache)
    started, release = asyncio.Event(), asyncio.Event()
    current = detail("旧标题")

    async def load(*args: int) -> object:
        snapshot = current
        if not started.is_set():
            started.set()
            await release.wait()
        return [snapshot] if listing else snapshot

    loader = repo.repo.get_all if listing else repo.repo.get
    loader.side_effect = load

    async def title() -> str:
        result = (await repo.get_all(0, 20))[0] if listing else await repo.get(1)
        return result.title

    pending = asyncio.create_task(title())
    await asyncio.wait_for(started.wait(), 1)
    current = detail("新标题")
    await repo.invalidate_event(1)
    release.set()
    assert await pending == "旧标题"  # 与更新重叠的原请求允许返回先前快照。
    assert await title() == "新标题"
    assert await title() == "新标题"
    assert loader.await_count == 2


@pytest.mark.parametrize(
    "error", [SQLAlchemyError("数据库异常"), ConnectionRefusedError()]
)
async def test_database_error_is_not_negative_cached(
    cache: FakeRedis, error: Exception
) -> None:
    repo = repository(cache)
    repo.repo.get.side_effect = error
    with pytest.raises(CacheUnavailableError):
        await repo.get(1)
    assert not await cache.exists(RedisKeys.event_static(1))
    assert not await cache.exists(RedisKeys.cache_lock(RedisKeys.event_static(1)))


async def test_lost_lease_cannot_fill_or_release_new_lock(cache: FakeRedis) -> None:
    repo = repository(cache)
    key, version_key = RedisKeys.event_static(1), RedisKeys.event_generation(1)
    generation, _ = await repo.store.read(version_key, key)
    old = CacheLock(cache, RedisKeys.cache_lock(key), 5000)
    assert await old.acquire()
    # 直接替换 token，确定性模拟租期结束后新持有者已获锁。
    await cache.set(old.key, "new-owner", px=5000)
    assert not await repo.store.fill(
        version_key, key, generation, old.key, old.token, "旧数据", 60
    )
    await old.release()
    assert await cache.get(old.key) == "new-owner"
    assert not await cache.exists(key)


async def test_cache_outage_uses_bounded_database_budget() -> None:
    redis = MagicMock()
    redis.eval = AsyncMock(side_effect=ConnectionError("离线"))
    repo = repository(redis)
    repo.admission = DatabaseAdmission(1, 0, 0.05, 1)
    started, release = asyncio.Event(), asyncio.Event()

    async def load(event_id: int) -> EventDetailResponse:
        started.set()
        await release.wait()
        return detail()

    repo.repo.get.side_effect = load
    pending = asyncio.create_task(repo.get(1))
    await asyncio.wait_for(started.wait(), 1)
    try:
        with pytest.raises(CacheUnavailableError):
            await repo.get(2)
    finally:
        release.set()
        await pending
    assert repo.repo.get.await_count == 1


@pytest.mark.parametrize("action", ["create", "update", "delete"])
async def test_ticket_type_mutations_invalidate_detail_and_lists(
    cache: FakeRedis, action: str, session: AsyncSession, ticket: Ticket
) -> None:
    repo = repository(cache)
    await repo.get(1)
    await repo.get_all(0, 20)
    detail_key, generation_key = (
        RedisKeys.event_static(1),
        RedisKeys.event_generation(1),
    )
    old_generation, _ = await repo.store.read(generation_key, detail_key)
    old_list, _ = await repo.store.read(
        RedisKeys.event_list_generation(), RedisKeys.event_list(0, 20)
    )
    service = TicketTypeService(session, cache)
    if action == "create":
        await service.create(
            1,
            TicketTypeCreate(event_id=1, name="普通票", price=100, tickets_quantity=5),
        )
    elif action == "update":
        await service.update(ticket.ticket_type_id, TicketTypeUpdate(price=200))
        assert (
            await cache.get(RedisKeys.ticket_type_inventory(ticket.ticket_type_id))
            == "4"
        )
    else:
        unused = TicketType(
            event_id=1,
            name="待删除票种",
            price=100,
            tickets_quantity=5,
            tickets_sold=0,
        )
        session.add(unused)
        await session.commit()
        await service.delete(unused.id)
    generation, body = await repo.store.read(generation_key, detail_key)
    list_generation, list_body = await repo.store.read(
        RedisKeys.event_list_generation(), RedisKeys.event_list(0, 20)
    )
    assert generation != old_generation and body is None
    assert list_generation != old_list and list_body is None


async def test_query_http_errors_and_validation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        deps, "event_query_limiter", QueryRateLimiter(0.01, 1, 10, 10, 10)
    )
    service = MagicMock()
    service.get = AsyncMock(side_effect=CacheUnavailableError("暂时繁忙"))
    original = app.dependency_overrides.copy()
    app.dependency_overrides[deps.get_event_service] = lambda: service
    app.dependency_overrides[deps.get_hotspot_tracker] = lambda: MagicMock()
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            response = await client.get(f"{settings.API_V1_STR}/events/1")
            assert response.status_code == 503
            assert response.headers["Retry-After"] == "1"
            response = await client.get(f"{settings.API_V1_STR}/events/2")
            assert response.status_code == 429
            assert int(response.headers["Retry-After"]) >= 1
    finally:
        app.dependency_overrides.clear()
        app.dependency_overrides.update(original)
