"""交易改造的必要快速检查；真实 PostgreSQL 并发与容量留最终验收。"""

from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, patch

import httpx
import pytest
from fakeredis.aioredis import FakeRedis
from redis.exceptions import ConnectionError
from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.exception import TicketsSoldOutError, TicketTypeQuantity
from src.core.redis_keys import RedisKeys
from src.models import OutboxEvent, Ticket, TicketType
from src.schemas import TicketCreate
from src.schemas.ticket_type import TicketTypeUpdate
from src.services.inventory import PROJECT_INVENTORY, transaction_outbox
from src.services.ticket import TicketService
from src.services.ticket_type import TicketTypeService

pytestmark = pytest.mark.asyncio


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
