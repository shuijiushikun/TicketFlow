"""最小收尾验收：真实 PostgreSQL/Redis，使用专用资源并限制为 20 个连接。"""

import asyncio
from collections import Counter
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock

import pytest
import pytest_asyncio
from httpx import AsyncClient
from sqlalchemy import func, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from src.api.deps import get_current_user
from src.core import settings
from src.core.redis_keys import RedisKeys
from src.db.redis import get_redis
from src.db.session import get_db
from src.main import app
from src.models import OutboxEvent, Ticket, TicketType, User
from src.models.ticket import TicketStatus
from src.schemas import TicketCreate
from src.services.inventory import project_inventory
from src.services.ticket import TicketService
from src.worker import release_unpaid_ticket
from tests.factories import EventFactory, TicketFactory, TicketTypeFactory, UserFactory

pytestmark = pytest.mark.asyncio


@pytest_asyncio.fixture
async def acceptance_sessions(
    db_connection: AsyncSession,
) -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    """保留真实行锁，避免把 1000 请求错误地变成 1000 条数据库连接。"""
    assert db_connection.bind is not None
    engine = create_async_engine(
        db_connection.bind.url,
        pool_size=20,
        max_overflow=0,
        pool_timeout=10,
        pool_pre_ping=True,
        connect_args={"server_settings": {"jit": "off"}},
    )
    try:
        yield async_sessionmaker(engine, expire_on_commit=False)
    finally:
        await engine.dispose()


async def test_thousand_requests_for_hundred_tickets(
    client: AsyncClient,
    acceptance_sessions: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """1000 个 ASGI 并发请求争抢 100 张票，验证真实扣票而非网络容量。"""
    async with acceptance_sessions() as session:
        user = UserFactory.build()
        ticket_type = TicketTypeFactory.build(
            event=EventFactory.build(),
            tickets_quantity=100,
            tickets_sold=0,
            inventory_version=0,
        )
        session.add_all([user, ticket_type])
        await session.commit()
        user_id, type_id = user.id, ticket_type.id

    async def concurrent_db() -> AsyncIterator[AsyncSession]:
        async with acceptance_sessions() as session:
            try:
                yield session
            except BaseException:
                await session.rollback()
                raise

    with monkeypatch.context() as patch:
        patch.setitem(app.dependency_overrides, get_db, concurrent_db)
        patch.setitem(
            app.dependency_overrides,
            get_current_user,
            lambda: User(id=user_id, email="acceptance@example.com", is_active=True),
        )
        ready = asyncio.Event()

        async def buy():
            await ready.wait()
            return await client.post(
                "/api/v1/tickets", json={"ticket_type_id": type_id}
            )

        requests = [asyncio.create_task(buy()) for _ in range(1000)]
        ready.set()
        responses = await asyncio.wait_for(asyncio.gather(*requests), timeout=60)

    counts = Counter(response.status_code for response in responses)
    assert counts[201] == 100, counts
    assert counts[409] + counts[503] == 900, counts
    assert set(counts) <= {201, 409, 503}, counts
    for response in responses:
        if response.status_code == 503:
            assert response.headers["Retry-After"] == "1"
            assert "数据库繁忙" in response.json()["detail"]
    successes = [
        response.json() for response in responses if response.status_code == 201
    ]
    assert len({item["id"] for item in successes}) == 100
    assert all(item["status"] == "RESERVED" for item in successes)
    assert all(
        "售罄" in response.json()["detail"]
        for response in responses
        if response.status_code == 409
    )
    async with acceptance_sessions() as session:
        stock = await session.get(TicketType, type_id)
        assert stock is not None and stock.tickets_sold == 100
        assert stock.inventory_version == 100
        assert await session.scalar(select(func.count(Ticket.id))) == 100
        redis = await get_redis()
        # 最终核对一次已提交库存，不把并发返回顺序当作 Redis 写入顺序。
        await project_inventory(session, redis, type_id)
        assert await redis.get(RedisKeys.ticket_type_inventory(type_id)) == "0"
    print(
        f"1000 并发请求：{counts[201]} 次有效预订、{counts[409]} 次售罄、"
        f"{counts[503]} 次有界过载拒绝，数据库占用 100，Redis 剩余 0"
    )


async def test_booking_commit_failure_rolls_back_inventory_and_order(
    acceptance_sessions: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """扣票、订单及待办已写入事务后，提交失败应全部回滚且不投递。"""
    async with acceptance_sessions() as session:
        user = UserFactory.build()
        ticket_type = TicketTypeFactory.build(
            event=EventFactory.build(),
            tickets_quantity=5,
            tickets_sold=0,
            inventory_version=0,
        )
        session.add_all([user, ticket_type])
        await session.commit()
        user_id, type_id = user.id, ticket_type.id
    redis = await get_redis()
    await redis.set(RedisKeys.ticket_type_inventory(type_id), 5)
    queue = AsyncMock()
    async with acceptance_sessions() as session:
        with monkeypatch.context() as patch:
            patch.setattr(
                session,
                "commit",
                AsyncMock(side_effect=SQLAlchemyError("注入提交失败")),
            )
            with pytest.raises(SQLAlchemyError, match="注入提交失败"):
                await TicketService(session, queue, redis).create(
                    user_id, TicketCreate(ticket_type_id=type_id)
                )
    async with acceptance_sessions() as session:
        stock = await session.get(TicketType, type_id)
        assert stock is not None and stock.tickets_sold == 0
        assert stock.inventory_version == 0
        assert await session.scalar(select(func.count(Ticket.id))) == 0
        assert await session.scalar(select(func.count(OutboxEvent.id))) == 0
    assert await redis.get(RedisKeys.ticket_type_inventory(type_id)) == "5"
    queue.enqueue_job.assert_not_awaited()


@pytest.mark.parametrize("first", ["pay", "expiry"])
async def test_payment_races_with_actual_expiry_worker(
    client: AsyncClient,
    acceptance_sessions: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
    first: str,
) -> None:
    """在截止时间两侧控制真实票据行锁，支付或到期回收只能有一个生效。"""
    import src.services.payment as payment_module
    import src.services.reservations as reservations_module

    monkeypatch.setattr(settings, "TICKET_RESERVATION_TIME_SECONDS", 900)
    deadline = datetime.now(UTC)
    async with acceptance_sessions() as session:
        owner = UserFactory.build()
        ticket_type = TicketTypeFactory.build(
            event=EventFactory.build(),
            tickets_quantity=5,
            tickets_sold=1,
            inventory_version=1,
        )
        ticket = TicketFactory.build(
            owner=owner,
            ticket_type=ticket_type,
            status=TicketStatus.RESERVED,
            created_at=deadline - timedelta(seconds=900),
        )
        session.add(ticket)
        await session.commit()
        owner_id, type_id, ticket_id = owner.id, ticket_type.id, ticket.id

    class ExpiredClock(datetime):
        @classmethod
        def now(cls, tz=None):
            return deadline + timedelta(seconds=1)

    class PaymentClock(datetime):
        @classmethod
        def now(cls, tz=None):
            return deadline + timedelta(seconds=-1 if first == "pay" else 1)

    monkeypatch.setattr(payment_module, "datetime", PaymentClock)
    monkeypatch.setattr(reservations_module, "datetime", ExpiredClock)
    locked, contender_ready = asyncio.Event(), asyncio.Event()
    worker_skipped = False

    def coordinated_session(operation: str) -> AsyncSession:
        session = acceptance_sessions()
        scalar = session.scalar

        async def first_query(statement):
            nonlocal worker_skipped
            session.scalar = scalar
            if operation == first:
                result = await scalar(statement)
                locked.set()
                await contender_ready.wait()
                return result
            await locked.wait()
            if first == "pay":
                result = await scalar(statement)
                worker_skipped = result is None
                contender_ready.set()
                return result
            contender_ready.set()
            return await scalar(statement)

        session.scalar = first_query
        return session

    async def payment_db() -> AsyncIterator[AsyncSession]:
        async with coordinated_session("pay") as session:
            yield session

    redis = await get_redis()
    context = {
        "session_maker": lambda: coordinated_session("expiry"),
        "business_redis": redis,
    }
    with monkeypatch.context() as patch:
        patch.setitem(app.dependency_overrides, get_db, payment_db)
        patch.setitem(
            app.dependency_overrides,
            get_current_user,
            lambda: User(id=owner_id, email="expiry-race@example.com", is_active=True),
        )
        response, _ = await asyncio.wait_for(
            asyncio.gather(
                client.post(f"/api/v1/tickets/{ticket_id}/payments"),
                release_unpaid_ticket(context, ticket_id),
            ),
            timeout=30,
        )
    expected_status = TicketStatus.SOLD if first == "pay" else TicketStatus.CANCELED
    assert response.status_code == (201 if first == "pay" else 400), response.text
    if first == "pay":
        assert worker_skipped, "支付持锁时，超时任务应跳过同一票据"
    # 真实回收函数重复运行，验证既不取消已付款票也不重复释放已取消库存。
    context["session_maker"] = acceptance_sessions
    await release_unpaid_ticket(context, ticket_id)
    await release_unpaid_ticket(context, ticket_id)
    async with acceptance_sessions() as session:
        ticket = await session.get(Ticket, ticket_id)
        stock = await session.get(TicketType, type_id)
        assert ticket is not None and ticket.status == expected_status
        assert stock is not None and stock.tickets_sold == (1 if first == "pay" else 0)
        assert stock.inventory_version == (1 if first == "pay" else 2)
        await project_inventory(session, redis, type_id)
        assert await redis.get(RedisKeys.ticket_type_inventory(type_id)) == (
            "4" if first == "pay" else "5"
        )
