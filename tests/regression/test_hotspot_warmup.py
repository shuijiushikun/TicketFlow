"""规则热点和预热的少量快速检查，均使用内存资源。"""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import pytest
import pytest_asyncio
from fakeredis.aioredis import FakeRedis
from redis.exceptions import ConnectionError
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.cache_policy import (
    CacheCircuitBreaker,
    CacheUnavailableError,
    DatabaseAdmission,
)
from src.core.config import settings
from src.core.redis_keys import RedisKeys
from src.repositories.event import CachedEventRepository, EventRepository
from src.schemas.event import EventDetailResponse
from src.services.cache_warmup import warm_hot_events
from src.services.hotspot import HotspotTracker


@pytest_asyncio.fixture
async def memory_cache() -> AsyncIterator[FakeRedis]:
    redis = FakeRedis(decode_responses=True)
    yield redis
    await redis.aclose()


async def test_record_has_ttl_cardinality_cap_and_failure_is_optional(
    memory_cache: FakeRedis, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(settings, "HOTSPOT_MAX_EVENTS_PER_MINUTE", 1)
    tracker = HotspotTracker(memory_cache, circuit=CacheCircuitBreaker(1))
    assert await tracker.record(1, now=60000)
    assert await tracker.record(1, now=60000)
    assert not await tracker.record(2, now=60000)
    assert await memory_cache.zscore(tracker.bucket_key(1000), "1") == 2
    assert 0 < await memory_cache.ttl(tracker.bucket_key(1000)) <= 900
    broken = MagicMock()
    broken.eval = AsyncMock(side_effect=ConnectionError("统计离线"))
    assert not await HotspotTracker(broken, circuit=CacheCircuitBreaker(1)).record(1)


async def test_hotspot_volume_growth_and_missing_buckets(
    memory_cache: FakeRedis,
) -> None:
    tracker = HotspotTracker(memory_cache)
    for minute, values in [
        (999, {"1": 15, "2": 6}),
        (998, {"1": 15, "2": 6}),
        (997, {"1": 15, "2": 2}),
        (996, {"1": 15, "2": 2}),
    ]:
        await memory_cache.zadd(tracker.bucket_key(minute), values)
    report = await tracker.report(now=60000)
    assert [(item.event_id, item.reason) for item in report.hot_events] == [
        (1, "volume"),
        (2, "growth"),
    ]
    assert report.hot_events[1].growth_ratio == 3
    await memory_cache.delete(tracker.bucket_key(996))
    report = await tracker.report(now=60000)
    assert not report.comparison_available and report.observed_buckets == 3
    assert [(item.event_id, item.growth_ratio) for item in report.hot_events] == [
        (1, None)
    ]


async def test_previous_counts_are_not_limited_to_previous_top_k(
    memory_cache: FakeRedis, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(settings, "HOTSPOT_CANDIDATES_PER_BUCKET", 1)
    tracker = HotspotTracker(memory_cache)
    for minute in (999, 998):
        await memory_cache.zadd(tracker.bucket_key(minute), {"1": 6})
    for minute in (997, 996):
        await memory_cache.zadd(tracker.bucket_key(minute), {"1": 10, "9": 100})
    report = await tracker.report(now=60000)
    assert report.comparison_available and report.hot_events == []


async def test_refresh_reuses_locks_and_cold_cache_does_not_force_database_on_outage(
    memory_cache: FakeRedis,
) -> None:
    db = MagicMock(spec=EventRepository)
    db.get.return_value = EventDetailResponse(
        id=1,
        title="活动",
        date=datetime(2027, 1, 1, tzinfo=UTC),
        country="中国",
        city="北京",
        street_address="礼堂",
        ticket_types=[],
    )
    repo = CachedEventRepository(
        db,
        memory_cache,
        admission=DatabaseAdmission(1, 0, 0.05, 1),
        circuit=CacheCircuitBreaker(1),
    )
    await repo.get(1)
    await repo.get(1, source="warmup", refresh_before_seconds=300)
    assert db.get.await_count == 1  # 新鲜缓存不回源。
    await memory_cache.expire(RedisKeys.event_static(1), 100)
    await repo.get(1, source="warmup", refresh_before_seconds=300)
    assert db.get.await_count == 2
    assert await memory_cache.ttl(RedisKeys.event_static(1)) >= 86400
    repo.store.circuit.fail()
    with pytest.raises(CacheUnavailableError):
        await repo.get(2, source="warmup", refresh_before_seconds=300)
    assert db.get.await_count == 2


async def test_warmup_batch_is_deduplicated_and_releases_lock(
    memory_cache: FakeRedis,
) -> None:
    session = MagicMock(spec=AsyncSession)
    session.scalars.return_value = MagicMock()
    session.scalars.return_value.all.return_value = []

    @asynccontextmanager
    async def session_factory() -> AsyncIterator[AsyncSession]:
        yield session

    ctx = {
        "cache_redis": memory_cache,
        "cache_circuit": CacheCircuitBreaker(1),
        "warmup_admission": DatabaseAdmission(1, 0, 0.05, 1, budget="warmup"),
        "session_maker": session_factory,
    }
    lock_key = "ticketflow:cache:v2:warmup:batch:lock"
    await memory_cache.set(lock_key, "another-worker", px=30000)
    await warm_hot_events(ctx)
    session.scalars.assert_not_awaited()
    assert await memory_cache.get(lock_key) == "another-worker"
    await memory_cache.delete(lock_key)
    await warm_hot_events(ctx)
    session.scalars.assert_awaited_once()
    assert not await memory_cache.exists(lock_key)
    assert await memory_cache.exists(
        RedisKeys.event_list(0, settings.DEFAULT_PAGE_LIMIT)
    )
