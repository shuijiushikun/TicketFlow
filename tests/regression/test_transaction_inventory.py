"""交易改造的必要快速检查；真实 PostgreSQL 并发与容量留最终验收。"""

from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, patch

import httpx
import pytest
from fakeredis.aioredis import FakeRedis
from redis.exceptions import ConnectionError
from sqlalchemy import func, select, update
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.exc import TimeoutError as DatabasePoolTimeout
from sqlalchemy.ext.asyncio import AsyncSession

from src.api.deps import get_current_user
from src.core.exception import TicketsSoldOutError, TicketTypeQuantity
from src.core.redis_keys import RedisKeys
from src.main import app
from src.models import OutboxEvent, Ticket, TicketType, User
from src.schemas import TicketCreate
from src.schemas.ticket_type import TicketTypeUpdate
from src.services.inventory import PROJECT_INVENTORY, transaction_outbox
from src.services.ticket import TicketService
from src.services.ticket_type import TicketTypeService

pytestmark = pytest.mark.asyncio


async def test_database_pool_timeout_is_retryable_with_same_idempotency_key(
    lifecycle_client: httpx.AsyncClient,
    session: AsyncSession,
    ticket: Ticket,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """连接池超时给出 503 和重试提示，原幂等键可在恢复后完成预订。"""
    headers = {"Idempotency-Key": "pool-timeout-retry-001"}
    payload = {"ticket_type_id": ticket.ticket_type_id}
    # 请求之间使用独立身份对象，避免本地共享 Session 回滚使夹具的 owner 过期。
    owner_id = ticket.owner_id
    monkeypatch.setitem(
        app.dependency_overrides,
        get_current_user,
        lambda: User(id=owner_id, email="pool-retry@example.com", is_active=True),
    )
    with monkeypatch.context() as patcher:
        patcher.setattr(
            TicketService,
            "create",
            AsyncMock(side_effect=DatabasePoolTimeout("不向客户端暴露内部连接信息")),
        )
        failed = await lifecycle_client.post(
            "/api/v1/tickets", json=payload, headers=headers
        )
    assert failed.status_code == 503
    assert failed.headers["Retry-After"] == "1"
    assert failed.headers["X-Request-ID"]
    assert "原幂等键" in failed.json()["detail"]
    assert "内部连接信息" not in failed.text
    assert await session.scalar(select(func.count(Ticket.id))) == 1
    assert await session.scalar(select(TicketType.tickets_sold)) == 1
    retry = await lifecycle_client.post(
        "/api/v1/tickets", json=payload, headers=headers
    )
    repeated = await lifecycle_client.post(
        "/api/v1/tickets", json=payload, headers=headers
    )
    assert retry.status_code == repeated.status_code == 201
    assert retry.json() == repeated.json()
    assert await session.scalar(select(func.count(Ticket.id))) == 2
    assert await session.scalar(select(TicketType.tickets_sold)) == 2


async def test_booking_retry_survives_redis_outage_and_recovers_projection(
    lifecycle_client: httpx.AsyncClient,
    session: AsyncSession,
    ticket: Ticket,
    redis_clients: tuple[FakeRedis, FakeRedis],
) -> None:
    business, _ = redis_clients
    type_id = ticket.ticket_type_id
    with patch.object(business, "eval", side_effect=ConnectionError("offline")):
        responses = [
            await lifecycle_client.post(
                "/api/v1/tickets",
                json={"ticket_type_id": type_id},
                headers={"Idempotency-Key": "booking-retry-001"},
            )
            for _ in range(2)
        ]
    assert [r.status_code for r in responses] == [201, 201]
    assert responses[0].json() == responses[1].json()
    assert await session.scalar(select(func.count(Ticket.id))) == 2
    assert await session.scalar(select(TicketType.tickets_sold)) == 2
    assert await session.scalar(select(func.count(OutboxEvent.id))) == 1
    await session.execute(
        update(OutboxEvent).values(
            next_attempt_at=datetime.now(UTC) - timedelta(seconds=1)
        )
    )
    await session.commit()
    assert await transaction_outbox(session, business).dispatch_batch() == 1
    assert await business.get(RedisKeys.ticket_type_inventory(type_id)) == "3"


async def test_last_ticket_and_admin_quantity_use_database_limits(
    session: AsyncSession, ticket: Ticket, redis_clients: tuple[FakeRedis, FakeRedis]
) -> None:
    business, _ = redis_clients
    type_id, owner_id = ticket.ticket_type_id, ticket.owner_id
    await TicketTypeService(session, business).update(
        type_id, TicketTypeUpdate(tickets_quantity=2)
    )
    service = TicketService(session, AsyncMock(), business)
    await service.create(owner_id, TicketCreate(ticket_type_id=type_id))
    # 即使 Redis 副本错误地声称有余票，数据库也拒绝再次扣票。
    await business.set(RedisKeys.ticket_type_inventory(type_id), 1000)
    with pytest.raises(TicketsSoldOutError):
        await service.create(owner_id, TicketCreate(ticket_type_id=type_id))
    with pytest.raises(TicketTypeQuantity):
        await TicketTypeService(session, business).update(
            type_id, TicketTypeUpdate(tickets_quantity=1)
        )
    assert await session.scalar(select(TicketType.tickets_sold)) == 2
    assert await session.scalar(select(func.count(Ticket.id))) == 2


async def test_inventory_projection_rejects_late_version_without_float_rounding(
    redis_clients: tuple[FakeRedis, FakeRedis],
) -> None:
    business, _ = redis_clients
    keys = (RedisKeys.ticket_type_inventory(1), RedisKeys.inventory_version(1))
    new_version = str(2**53 + 1).zfill(20)
    old_version = str(2**53).zfill(20)
    assert await business.eval(PROJECT_INVENTORY, 2, *keys, new_version, "2") == 1
    assert await business.eval(PROJECT_INVENTORY, 2, *keys, old_version, "3") == 0
    assert await business.get(keys[0]) == "2"
    # 删除与快照读取交错时，迟到快照也不能让已删除的库存重新出现。
    tombstone = str(2**63).zfill(20)
    assert await business.eval(PROJECT_INVENTORY, 2, *keys, tombstone, "deleted") == 1
    assert await business.eval(PROJECT_INVENTORY, 2, *keys, new_version, "2") == 0
    assert await business.get(keys[0]) is None
    assert await business.get(keys[1]) == tombstone


async def test_booking_projects_committed_snapshot_without_dispatch_queries(
    session: AsyncSession, ticket: Ticket, redis_clients: tuple[FakeRedis, FakeRedis]
) -> None:
    from sqlalchemy import event

    business, _ = redis_clients
    statements: list[str] = []

    def capture(conn, cursor, statement, parameters, context, executemany):
        statements.append(statement.lower())

    engine = session.bind.sync_engine
    event.listen(engine, "before_cursor_execute", capture)
    try:
        response = await TicketService(session, AsyncMock(), business).create(
            ticket.owner_id, TicketCreate(ticket_type_id=ticket.ticket_type_id)
        )
    finally:
        event.remove(engine, "before_cursor_execute", capture)
    assert response.status.value == "RESERVED"
    assert (
        await business.get(RedisKeys.ticket_type_inventory(ticket.ticket_type_id))
        == "3"
    )
    assert await session.scalar(select(func.count(OutboxEvent.id))) == 0
    # 回填使用已提交 UPDATE 的返回值，不再另开投递事务重查票种与待办。
    assert not any("select" in sql and "ticket_types" in sql for sql in statements)
    assert not any("select" in sql and "outbox_events" in sql for sql in statements)


async def test_projection_ack_failure_keeps_committed_booking_and_pending_retry(
    session: AsyncSession, ticket: Ticket, redis_clients: tuple[FakeRedis, FakeRedis]
) -> None:
    from sqlalchemy import event

    business, _ = redis_clients

    def fail_ack(conn, cursor, statement, parameters, context, executemany):
        if statement.lower().startswith("delete from outbox_events"):
            raise SQLAlchemyError("模拟 Redis 已写入、待办确认失败")

    engine = session.bind.sync_engine
    event.listen(engine, "before_cursor_execute", fail_ack)
    try:
        await TicketService(session, AsyncMock(), business).create(
            ticket.owner_id, TicketCreate(ticket_type_id=ticket.ticket_type_id)
        )
    finally:
        event.remove(engine, "before_cursor_execute", fail_ack)
    assert await session.scalar(select(func.count(Ticket.id))) == 2
    assert await session.scalar(select(TicketType.tickets_sold)) == 2
    assert (
        await business.get(RedisKeys.ticket_type_inventory(ticket.ticket_type_id))
        == "3"
    )
    assert await session.scalar(select(func.count(OutboxEvent.id))) == 1
    await session.rollback()
    assert await transaction_outbox(session, business).dispatch_batch() == 1
    assert await session.scalar(select(func.count(OutboxEvent.id))) == 0


async def test_payment_cleanup_failure_keeps_success_and_recovers_without_refund(
    lifecycle_client: httpx.AsyncClient,
    session: AsyncSession,
    ticket: Ticket,
    redis_clients: tuple[FakeRedis, FakeRedis],
) -> None:
    business, _ = redis_clients
    ticket_id, type_id = ticket.id, ticket.ticket_type_id
    with patch.object(business, "hdel", side_effect=ConnectionError("offline")):
        responses = [
            await lifecycle_client.post(
                f"/api/v1/tickets/{ticket_id}/payments",
                headers={"Idempotency-Key": "payment-retry-001"},
            )
            for _ in range(2)
        ]
    assert [r.status_code for r in responses] == [201, 201]
    assert responses[0].json() == responses[1].json()
    assert responses[0].json()["status"] == "SOLD"
    assert await business.hget(RedisKeys.active_reservations_hash(), str(ticket_id))
    assert await session.scalar(select(func.count(OutboxEvent.id))) == 1
    await session.execute(
        update(OutboxEvent).values(
            next_attempt_at=datetime.now(UTC) - timedelta(seconds=1)
        )
    )
    await session.commit()
    assert await transaction_outbox(session, business).dispatch_batch() == 1
    assert (
        await business.hget(RedisKeys.active_reservations_hash(), str(ticket_id))
        is None
    )
    assert await business.get(RedisKeys.ticket_type_inventory(type_id)) == "4"
