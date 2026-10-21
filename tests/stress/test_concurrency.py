import asyncio
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from fastapi import Depends, Request
from httpx import AsyncClient, Response
from redis.exceptions import ConnectionError as RedisConnectionError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.api.deps import (
    DBDep,
    get_current_superuser,
    get_current_user,
    get_event_service,
    get_hotspot_tracker,
)
from src.core import settings
from src.core.cache_lock import CacheLock
from src.core.cache_policy import CacheCircuitBreaker, DatabaseAdmission
from src.core.redis_keys import RedisClient, RedisKeys
from src.db.redis import get_cache_redis
from src.db.session import get_db
from src.main import app
from src.models import Event, IdempotencyRequest, Ticket, TicketType, User
from src.models.ticket import TicketStatus
from src.repositories.event import CachedEventRepository, EventRepository
from src.services.event import EventService
from src.services.hotspot import HotspotTracker
from src.services.idempotency import IdempotencyService
from tests.factories import EventFactory, TicketFactory, TicketTypeFactory, UserFactory


@pytest.mark.asyncio
async def test_race_condition_overselling(
    stress_client: AsyncClient,
    test_session_factory: async_sessionmaker[AsyncSession],
    cleanup_physical_db: None,
) -> None:

    async with test_session_factory() as session:
        user = UserFactory.build()
        session.add(user)
        await session.commit()

        event = EventFactory.build()
        session.add(event)
        await session.commit()

        ticket_type = TicketTypeFactory.build(
            event=event, tickets_quantity=5, tickets_sold=0
        )
        session.add(ticket_type)
        await session.commit()

        await session.refresh(user)
        await session.refresh(ticket_type)

        ticket_type_id = ticket_type.id
        physical_user_id = user.id

    app.dependency_overrides[get_current_user] = lambda: User(
        id=physical_user_id, email="stress@test.com", is_active=True
    )

    try:
        ticket_payload = {"ticket_type_id": ticket_type_id}

        async def buy_ticket() -> Response:
            return await stress_client.post("/api/v1/tickets", json=ticket_payload)

        buy_ticket_tasks = [buy_ticket() for _ in range(50)]
        responses = await asyncio.gather(*buy_ticket_tasks)

        first_error = next(
            (r for r in responses if r.status_code not in (201, 409)), None
        )
        if first_error:
            pytest.fail(
                f"Request failed! Status: {first_error.status_code}, "
                f"Body: {first_error.json()}"
            )

        success_count = sum(1 for r in responses if r.status_code == 201)
        conflict_count = sum(1 for r in responses if r.status_code == 409)

        assert success_count == 5
        assert conflict_count == 45

        async with test_session_factory() as session:
            updated_ticket_type = await session.get(TicketType, ticket_type_id)
            assert updated_ticket_type is not None
            assert updated_ticket_type.tickets_sold == 5

    finally:
        app.dependency_overrides.pop(get_current_user, None)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "existing_event", [False, True], ids=["negative_cache", "hot_key"]
)
async def test_concurrent_event_cache_rebuild(
    stress_client: AsyncClient,
    test_session_factory: async_sessionmaker[AsyncSession],
    cleanup_physical_db: None,
    monkeypatch: pytest.MonkeyPatch,
    existing_event: bool,
) -> None:
    """真实数据库和 Redis 下，同键冷缓存并发只回源一次，重试命中缓存。"""
    event_id = 999999
    if existing_event:
        async with test_session_factory() as session:
            event = EventFactory.build()
            session.add(event)
            await session.commit()
            event_id = event.id

    cache_key = RedisKeys.event_static(event_id)
    lock_key = RedisKeys.cache_lock(cache_key)
    prepared = asyncio.Event()
    prepared_count = 0
    contended = asyncio.Event()
    database_loads = 0
    original_get = EventRepository.get
    original_acquire = CacheLock.acquire
    # 同一用例中的请求共享保护状态；不同参数用例不继承前一轮的熔断。
    circuit = CacheCircuitBreaker(settings.CACHE_CIRCUIT_COOLDOWN_SECONDS)
    record_circuit = CacheCircuitBreaker(settings.CACHE_CIRCUIT_COOLDOWN_SECONDS)
    admission = DatabaseAdmission(
        concurrency=settings.CACHE_DB_CONCURRENCY,
        queue_size=settings.CACHE_DB_QUEUE_SIZE,
        queue_timeout=settings.CACHE_DB_QUEUE_TIMEOUT_SECONDS,
        query_timeout=settings.CACHE_DB_QUERY_TIMEOUT_SECONDS,
    )

    async def isolated_event_service(
        session: DBDep, redis: RedisClient = Depends(get_cache_redis)
    ) -> EventService:
        base = EventRepository(session)
        cached = CachedEventRepository(
            base, redis, admission=admission, circuit=circuit
        )
        return EventService(session=session, base_repo=base, cached_repo=cached)

    async def prepared_tracker(
        redis: RedisClient = Depends(get_cache_redis),
    ) -> HotspotTracker:
        nonlocal prepared_count
        # 此依赖在活动服务之后解析；所有请求准备好后，才一起进入查询。
        # 同步点位于缓存计时之前，不放宽生产超时，也不减少并发请求数。
        if not prepared.is_set():
            prepared_count += 1
            if prepared_count == 50:
                prepared.set()
        await prepared.wait()
        return HotspotTracker(redis, circuit=record_circuit)

    monkeypatch.setitem(
        app.dependency_overrides, get_event_service, isolated_event_service
    )
    monkeypatch.setitem(app.dependency_overrides, get_hotspot_tracker, prepared_tracker)

    async def observed_acquire(lock: CacheLock) -> bool:
        acquired = await original_acquire(lock)
        if lock.key == lock_key and not acquired:
            contended.set()
        return acquired

    async def observed_get(
        repository: EventRepository, requested_id: int
    ) -> Event | None:
        nonlocal database_loads
        assert requested_id == event_id
        database_loads += 1
        # 等另一个请求真实争锁后再查数据库，避免用串行命中冒充并发重建。
        await contended.wait()
        return await original_get(repository, requested_id)

    monkeypatch.setattr(CacheLock, "acquire", observed_acquire)
    monkeypatch.setattr(EventRepository, "get", observed_get)
    url = f"/api/v1/events/{event_id}"
    expected_code = 200 if existing_event else 404
    async with await get_cache_redis() as redis:
        assert not await redis.exists(cache_key)
        responses = await asyncio.wait_for(
            asyncio.gather(*[stress_client.get(url) for _ in range(50)]),
            timeout=30,
        )
        assert prepared_count == 50
        assert contended.is_set(), "必须实际发生缓存重建锁竞争"
        completed = []
        for response in responses:
            # 竞争超过生产等待预算时允许 503，但不能绕过重建锁大量回源。
            assert response.status_code in (expected_code, 503), response.text
            if response.status_code == expected_code:
                completed.append(response.json())
        assert completed, "至少应有一次重建成功并返回活动或确认不存在"
        assert all(body == completed[0] for body in completed)
        assert database_loads == 1

        # 重建完成后的重试必须命中正文或空值，不能继续繁忙或再次回源。
        for _ in range(10):
            retry = await stress_client.get(url)
            assert retry.status_code == expected_code, retry.text
            assert retry.json() == completed[0]
        assert database_loads == 1
        assert not await redis.exists(lock_key)
        maximum_ttl = (
            settings.CACHE_DETAIL_TTL_SECONDS + settings.CACHE_DETAIL_JITTER_SECONDS
            if existing_event
            else settings.CACHE_NEGATIVE_TTL_SECONDS
            + settings.CACHE_NEGATIVE_JITTER_SECONDS
        )
        assert 0 < await redis.pttl(cache_key) <= maximum_ttl * 1000


@pytest.mark.asyncio
async def test_event_cache_outage_is_bounded_and_recovers(
    stress_client: AsyncClient,
    test_session_factory: async_sessionmaker[AsyncSession],
    cleanup_physical_db: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """查询缓存连接失败时有界回源，恢复并冷却后重新回填、命中真实 Redis。"""
    async with test_session_factory() as session:
        event = EventFactory.build()
        session.add(event)
        await session.commit()
        event_id, title = event.id, event.title

    circuit = CacheCircuitBreaker(settings.CACHE_CIRCUIT_COOLDOWN_SECONDS)
    record_circuit = CacheCircuitBreaker(settings.CACHE_CIRCUIT_COOLDOWN_SECONDS)
    admission = DatabaseAdmission(
        concurrency=settings.CACHE_DB_CONCURRENCY,
        queue_size=settings.CACHE_DB_QUEUE_SIZE,
        queue_timeout=settings.CACHE_DB_QUEUE_TIMEOUT_SECONDS,
        query_timeout=settings.CACHE_DB_QUERY_TIMEOUT_SECONDS,
    )
    assert settings.CACHE_DB_CONCURRENCY < 50, "本用例需要并发请求超过回源名额"
    all_attempted = asyncio.Event()
    attempts = database_loads = active = peak = 0
    original_acquire = admission._acquire
    original_get = EventRepository.get

    async def observed_acquire() -> None:
        nonlocal attempts
        attempts += 1
        if attempts == 50:
            all_attempted.set()
        await original_acquire()

    async def observed_get(
        repository: EventRepository, requested_id: int
    ) -> Event | None:
        nonlocal database_loads, active, peak
        assert requested_id == event_id
        database_loads += 1
        active += 1
        peak = max(peak, active)
        try:
            # 先占住真实回源名额，直到所有请求尝试准入，确保实际触发过载保护。
            await all_attempted.wait()
            return await original_get(repository, requested_id)
        finally:
            active -= 1

    async def isolated_event_service(
        session: DBDep, redis: RedisClient = Depends(get_cache_redis)
    ) -> EventService:
        base = EventRepository(session)
        cached = CachedEventRepository(
            base, redis, admission=admission, circuit=circuit
        )
        return EventService(session=session, base_repo=base, cached_repo=cached)

    async def isolated_tracker(
        redis: RedisClient = Depends(get_cache_redis),
    ) -> HotspotTracker:
        return HotspotTracker(redis, circuit=record_circuit)

    monkeypatch.setattr(admission, "_acquire", observed_acquire)
    monkeypatch.setattr(EventRepository, "get", observed_get)
    monkeypatch.setitem(
        app.dependency_overrides, get_event_service, isolated_event_service
    )
    monkeypatch.setitem(app.dependency_overrides, get_hotspot_tracker, isolated_tracker)

    url = f"/api/v1/events/{event_id}"
    cache_key = RedisKeys.event_static(event_id)
    failed_eval = AsyncMock(side_effect=RedisConnectionError("注入查询缓存连接故障"))
    async with await get_cache_redis() as redis:

        async def shared_cache_redis() -> RedisClient:
            return redis

        monkeypatch.setitem(
            app.dependency_overrides, get_cache_redis, shared_cache_redis
        )
        assert not await redis.exists(cache_key)
        # 只让查询客户端的命令失败，不停止 Redis 服务或影响库存、Arq 客户端。
        with monkeypatch.context() as outage:
            outage.setattr(redis, "eval", failed_eval)
            responses = await asyncio.wait_for(
                asyncio.gather(*[stress_client.get(url) for _ in range(50)]),
                timeout=30,
            )
        assert failed_eval.await_count > 0
        assert attempts == 50
        assert peak == settings.CACHE_DB_CONCURRENCY
        assert active == 0
        assert 0 < database_loads < 50
        codes = {response.status_code for response in responses}
        assert codes == {200, 503}, [response.text for response in responses]
        for response in responses:
            if response.status_code == 200:
                assert response.json()["id"] == event_id
                assert response.json()["title"] == title
            else:
                assert response.headers["Retry-After"] == "1"
                assert "请稍后重试" in response.json()["detail"]
        assert not await redis.exists(cache_key)

        # 不手动重置熔断：等待生产冷却周期，再使用恢复后的真实 Redis。
        await asyncio.sleep(settings.CACHE_CIRCUIT_COOLDOWN_SECONDS)
        loads_before_recovery = database_loads
        recovered = await stress_client.get(url)
        assert recovered.status_code == 200, recovered.text
        assert recovered.json()["id"] == event_id
        assert recovered.json()["title"] == title
        assert database_loads == loads_before_recovery + 1
        assert await redis.exists(cache_key)
        assert not await redis.exists(RedisKeys.cache_lock(cache_key))

        cached = await stress_client.get(url)
        assert cached.status_code == 200, cached.text
        assert cached.json() == recovered.json()
        assert database_loads == loads_before_recovery + 1


@pytest.mark.asyncio
async def test_event_update_rejects_stale_cache_fill(
    stress_client: AsyncClient,
    test_session_factory: async_sessionmaker[AsyncSession],
    cleanup_physical_db: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """真实更新与旧详情重建交错时，旧快照不能回填到更新后的缓存版本。"""
    old_title, new_title = "修改前的活动", "修改后的活动"
    async with test_session_factory() as session:
        event = EventFactory.build(title=old_title)
        session.add(event)
        await session.commit()
        event_id = event.id

    circuit = CacheCircuitBreaker(settings.CACHE_CIRCUIT_COOLDOWN_SECONDS)
    admission = DatabaseAdmission(
        concurrency=settings.CACHE_DB_CONCURRENCY,
        queue_size=settings.CACHE_DB_QUEUE_SIZE,
        queue_timeout=settings.CACHE_DB_QUEUE_TIMEOUT_SECONDS,
        query_timeout=settings.CACHE_DB_QUERY_TIMEOUT_SECONDS,
    )
    started, release = asyncio.Event(), asyncio.Event()
    database_loads = 0
    original_get = EventRepository.get

    async def observed_get(
        repository: EventRepository, requested_id: int
    ) -> Event | None:
        nonlocal database_loads
        assert requested_id == event_id
        database_loads += 1
        snapshot = await original_get(repository, requested_id)
        if not started.is_set():
            assert snapshot is not None and snapshot.title == old_title
            # 已从 PostgreSQL 取到旧快照，再暂停；后续更新使用另一数据库会话。
            started.set()
            await release.wait()
        return snapshot

    async def isolated_event_service(
        session: DBDep, redis: RedisClient = Depends(get_cache_redis)
    ) -> EventService:
        base = EventRepository(session)
        cached = CachedEventRepository(
            base, redis, admission=admission, circuit=circuit
        )
        return EventService(session=session, base_repo=base, cached_repo=cached)

    # 权限校验由其他用例覆盖；这里保留真实 HTTP 更新、数据库提交与 Outbox 失效。
    monkeypatch.setitem(
        app.dependency_overrides,
        get_current_superuser,
        lambda: User(id=1, email="cache-update@example.com", is_superuser=True),
    )
    monkeypatch.setitem(
        app.dependency_overrides, get_event_service, isolated_event_service
    )
    monkeypatch.setattr(EventRepository, "get", observed_get)

    url = f"/api/v1/events/{event_id}"
    cache_key = RedisKeys.event_static(event_id)
    generation_key = RedisKeys.event_generation(event_id)
    lock_key = RedisKeys.cache_lock(cache_key)
    async with await get_cache_redis() as redis:
        assert not await redis.exists(cache_key)
        pending = asyncio.create_task(stress_client.get(url))
        try:
            await asyncio.wait_for(started.wait(), timeout=5)
            old_generation = await redis.get(generation_key)
            assert old_generation is not None
            assert await redis.exists(lock_key)

            updated = await stress_client.patch(url, json={"title": new_title})
            assert updated.status_code == 200, updated.text
            assert updated.json()["title"] == new_title
            new_generation = await redis.get(generation_key)
            assert new_generation is not None and new_generation != old_generation

            release.set()
            old_response = await asyncio.wait_for(pending, timeout=5)
            assert old_response.status_code == 200, old_response.text
            # 与更新重叠的原请求允许返回旧快照，但它不能污染后续查询。
            assert old_response.json()["title"] == old_title
            assert not await redis.exists(cache_key), "旧快照不应被重新写回缓存"
            assert not await redis.exists(lock_key)

            loads_before_retry = database_loads
            refreshed = await stress_client.get(url)
            assert refreshed.status_code == 200, refreshed.text
            assert refreshed.json()["title"] == new_title
            assert database_loads == loads_before_retry + 1
            assert await redis.exists(cache_key)

            cached = await stress_client.get(url)
            assert cached.status_code == 200, cached.text
            assert cached.json() == refreshed.json()
            assert database_loads == loads_before_retry + 1
        finally:
            release.set()
            if not pending.done():
                pending.cancel()
            await asyncio.gather(pending, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "first_operation", ["pay", "cancel"], ids=["pay_first", "cancel_first"]
)
async def test_payment_cancel_race(
    stress_client: AsyncClient,
    test_session_factory: async_sessionmaker[AsyncSession],
    cleanup_physical_db: None,
    monkeypatch: pytest.MonkeyPatch,
    first_operation: str,
) -> None:
    """同票支付与取消竞争时，只有先获得票据行锁的一方能改变状态。"""
    monkeypatch.setattr(settings, "TICKET_RESERVATION_TIME_SECONDS", 900)
    async with test_session_factory() as session:
        user = UserFactory.build()
        ticket_type = TicketTypeFactory.build(
            event=EventFactory.build(),
            tickets_quantity=5,
            tickets_sold=1,
            inventory_version=1,
        )
        ticket = TicketFactory.build(
            owner=user,
            ticket_type=ticket_type,
            status=TicketStatus.RESERVED,
            created_at=datetime.now(UTC),
        )
        session.add(ticket)
        await session.commit()
        user_id, ticket_type_id, ticket_id = user.id, ticket_type.id, ticket.id

    locked = asyncio.Event()
    contender_ready = asyncio.Event()

    async def coordinated_db(request: Request) -> AsyncIterator[AsyncSession]:
        operation = "cancel" if request.method == "DELETE" else "pay"
        async with test_session_factory() as session:
            scalar = session.scalar

            async def first_query(statement: Any) -> Any:
                # 两条业务路径的首个 scalar 查询均是票据 SELECT FOR UPDATE。
                # 保留真实业务查询与行锁，只控制两个请求到达这里的顺序。
                monkeypatch.setattr(session, "scalar", scalar)
                if operation == first_operation:
                    result = await scalar(statement)
                    locked.set()
                    await contender_ready.wait()
                    return result
                await locked.wait()
                contender_ready.set()
                return await scalar(statement)

            monkeypatch.setattr(session, "scalar", first_query)
            yield session

    previous_db = app.dependency_overrides[get_db]
    previous_user = app.dependency_overrides.get(get_current_user)
    app.dependency_overrides[get_db] = coordinated_db
    app.dependency_overrides[get_current_user] = lambda: User(
        id=user_id, email="payment-race@example.com", is_active=True
    )
    try:
        pay_response, cancel_response = await asyncio.wait_for(
            asyncio.gather(
                stress_client.post(f"/api/v1/tickets/{ticket_id}/payments"),
                stress_client.delete(f"/api/v1/tickets/{ticket_id}"),
            ),
            timeout=30,
        )
        if first_operation == "pay":
            assert pay_response.status_code == 201, pay_response.text
            assert cancel_response.status_code == 409, cancel_response.text
            expected_status, expected_sold, expected_version = TicketStatus.SOLD, 1, 1
        else:
            assert cancel_response.status_code == 204, cancel_response.text
            assert pay_response.status_code == 400, pay_response.text
            expected_status = TicketStatus.CANCELED
            expected_sold, expected_version = 0, 2

        async with test_session_factory() as session:
            saved_ticket = await session.get(Ticket, ticket_id)
            saved_type = await session.get(TicketType, ticket_type_id)
            assert saved_ticket is not None and saved_type is not None
            assert saved_ticket.status == expected_status
            assert saved_type.tickets_sold == expected_sold
            assert saved_type.inventory_version == expected_version
    finally:
        app.dependency_overrides[get_db] = previous_db
        if previous_user is None:
            app.dependency_overrides.pop(get_current_user, None)
        else:
            app.dependency_overrides[get_current_user] = previous_user


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "redis_lock_failure", [False, True], ids=["redis_lock_ok", "redis_lock_write_error"]
)
async def test_concurrent_idempotent_booking(
    stress_client: AsyncClient,
    test_session_factory: async_sessionmaker[AsyncSession],
    cleanup_physical_db: None,
    monkeypatch: pytest.MonkeyPatch,
    redis_lock_failure: bool,
) -> None:
    """正常或 Redis 锁写入失败时，同键并发仍只建一张票并复用响应。"""
    failed_set = AsyncMock(side_effect=RedisConnectionError("注入幂等锁写入故障"))
    claimants = 0
    if redis_lock_failure:
        all_missing = asyncio.Event()
        acquire = IdempotencyService._acquire

        async def acquire_with_failed_write(
            service: IdempotencyService, key: str, token: str
        ) -> bool:
            nonlocal claimants
            # 所有请求先读到记录不存在，再同时竞争数据库唯一约束。
            # 同步点在生产锁超时范围之外，不改变生产超时配置。
            claimants += 1
            if claimants == 50:
                all_missing.set()
            await all_missing.wait()
            monkeypatch.setattr(service.redis, "set", failed_set)
            return await acquire(service, key, token)

        monkeypatch.setattr(IdempotencyService, "_acquire", acquire_with_failed_write)

    async with test_session_factory() as session:
        user = UserFactory.build()
        ticket_type = TicketTypeFactory.build(
            event=EventFactory.build(),
            tickets_quantity=5,
            tickets_sold=0,
            inventory_version=0,
        )
        session.add_all([user, ticket_type])
        await session.commit()
        user_id, ticket_type_id = user.id, ticket_type.id

    app.dependency_overrides[get_current_user] = lambda: User(
        id=user_id, email="idempotency-stress@example.com", is_active=True
    )
    key = f"concurrent-booking-{uuid4().hex}"
    headers = {"Idempotency-Key": key}
    payload = {"ticket_type_id": ticket_type_id}

    try:
        responses = await asyncio.wait_for(
            asyncio.gather(
                *[
                    stress_client.post("/api/v1/tickets", json=payload, headers=headers)
                    for _ in range(50)
                ]
            ),
            timeout=30,
        )
        successful = []
        for response in responses:
            assert response.status_code in (201, 409), response.text
            if response.status_code == 201:
                successful.append(response.json())
            else:
                # 同一请求的暂时冲突应为正在处理，不能掩盖错误的重复扣票售罄。
                assert "处理中" in response.json()["detail"]
        assert successful, "并发请求中至少应有一次预订成功"
        if redis_lock_failure:
            assert claimants == 50
            assert failed_set.await_count == 50

        retry = await stress_client.post(
            "/api/v1/tickets", json=payload, headers=headers
        )
        assert retry.status_code == 201, retry.text
        snapshot = retry.json()
        assert all(response == snapshot for response in successful)

        async with test_session_factory() as session:
            tickets = list(
                await session.scalars(
                    select(Ticket).where(Ticket.ticket_type_id == ticket_type_id)
                )
            )
            assert len(tickets) == 1
            assert tickets[0].id == snapshot["id"]
            updated_type = await session.get(TicketType, ticket_type_id)
            assert updated_type is not None
            assert updated_type.tickets_sold == 1
            assert updated_type.inventory_version == 1

            records = list(
                await session.scalars(
                    select(IdempotencyRequest).where(
                        IdempotencyRequest.user_id == user_id,
                        IdempotencyRequest.action == "create_ticket",
                        IdempotencyRequest.idempotency_key == key,
                    )
                )
            )
            assert len(records) == 1
            assert records[0].response == snapshot
    finally:
        app.dependency_overrides.pop(get_current_user, None)
