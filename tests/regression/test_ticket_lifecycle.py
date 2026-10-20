"""通过本地临时数据库验证票据取消、过期和异常恢复流程。"""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import AsyncMock, patch

import httpx
import pytest
from fakeredis.aioredis import FakeRedis
from redis.exceptions import ConnectionError as RedisConnectionError
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from src.core import settings
from src.core.exception import TicketAlreadyPaidError, TicketReservationExpireError
from src.core.redis_keys import RedisKeys
from src.models import Ticket, TicketType
from src.models.ticket import TicketStatus
from src.schemas import TicketCreate
from src.services.payment import PaymentService
from src.services.ticket import TicketService
from src.worker import (
    reconcile_hung_tickets,
    reconcile_redis_orphans,
    release_unpaid_ticket,
    shutdown,
    startup,
)

pytestmark = pytest.mark.asyncio


def worker_context(
    session: AsyncSession, business: FakeRedis, queue: FakeRedis
) -> dict[str, Any]:
    @asynccontextmanager
    async def session_maker() -> AsyncIterator[AsyncSession]:
        yield session

    return {"session_maker": session_maker, "business_redis": business, "redis": queue}


async def test_cancel_preserves_record_and_restores_inventory_once(
    session: AsyncSession, ticket: Ticket, redis_clients: tuple[FakeRedis, FakeRedis]
) -> None:
    business, queue = redis_clients
    service = TicketService(session, queue, business)
    await service.delete(ticket.owner_id, ticket.id)
    await service.delete(ticket.owner_id, ticket.id)

    saved = await session.get(Ticket, ticket.id)
    assert saved is not None and saved.status == TicketStatus.CANCELED
    ticket_type = await session.get(TicketType, ticket.ticket_type_id)
    assert ticket_type is not None and ticket_type.tickets_sold == 0
    assert (
        int(await business.get(RedisKeys.ticket_type_inventory(ticket.ticket_type_id)))
        == 5
    )
    assert (
        await business.hget(RedisKeys.active_reservations_hash(), str(ticket.id))
        is None
    )


async def test_paid_ticket_cannot_be_canceled(
    session: AsyncSession, ticket: Ticket, redis_clients: tuple[FakeRedis, FakeRedis]
) -> None:
    business, queue = redis_clients
    ticket.status = TicketStatus.SOLD
    await session.commit()
    with pytest.raises(TicketAlreadyPaidError):
        await TicketService(session, queue, business).delete(ticket.owner_id, ticket.id)
    assert await session.get(Ticket, ticket.id) is ticket
    assert ticket.status == TicketStatus.SOLD
    assert (
        int(await business.get(RedisKeys.ticket_type_inventory(ticket.ticket_type_id)))
        == 4
    )


async def test_failed_redis_refund_is_recoverable_after_cancel_commit(
    session: AsyncSession, ticket: Ticket, redis_clients: tuple[FakeRedis, FakeRedis]
) -> None:
    business, queue = redis_clients
    with patch.object(
        business, "eval", side_effect=RedisConnectionError("unavailable")
    ):
        with pytest.raises(RedisConnectionError):
            await TicketService(session, queue, business).delete(
                ticket.owner_id, ticket.id
            )
    assert await session.get(Ticket, ticket.id) is not None
    assert ticket.status == TicketStatus.CANCELED
    ctx = worker_context(session, business, queue)
    await reconcile_redis_orphans(ctx)
    await reconcile_redis_orphans(ctx)
    assert (
        int(await business.get(RedisKeys.ticket_type_inventory(ticket.ticket_type_id)))
        == 5
    )
    ticket_type = await session.get(TicketType, ticket.ticket_type_id)
    assert ticket_type is not None and ticket_type.tickets_sold == 0


@pytest.mark.parametrize("elapsed", [900, 901])
async def test_payment_rejects_expired_reservation_before_worker_runs(
    session: AsyncSession,
    ticket: Ticket,
    redis_clients: tuple[FakeRedis, FakeRedis],
    monkeypatch: pytest.MonkeyPatch,
    elapsed: int,
) -> None:
    now = datetime(2026, 10, 11, 12, tzinfo=UTC)

    class Clock:
        @staticmethod
        def now(tz: object) -> datetime:
            return now

    monkeypatch.setattr("src.services.payment.datetime", Clock, raising=False)
    monkeypatch.setattr(settings, "TICKET_RESERVATION_TIME_SECONDS", 900)
    ticket.created_at = now - timedelta(seconds=elapsed)
    await session.commit()
    business, _ = redis_clients
    with pytest.raises(TicketReservationExpireError):
        await PaymentService(session, business).pay_for_ticket(
            ticket.id, ticket.owner_id
        )
    assert ticket.status == TicketStatus.RESERVED


async def test_payment_accepts_unexpired_reservation(
    session: AsyncSession, ticket: Ticket, redis_clients: tuple[FakeRedis, FakeRedis]
) -> None:
    business, _ = redis_clients
    response = await PaymentService(session, business).pay_for_ticket(
        ticket.id, ticket.owner_id
    )
    assert response.status == TicketStatus.SOLD
    assert (
        int(await business.get(RedisKeys.ticket_type_inventory(ticket.ticket_type_id)))
        == 4
    )


async def test_worker_refunds_business_database_instead_of_queue_database(
    session: AsyncSession, ticket: Ticket, redis_clients: tuple[FakeRedis, FakeRedis]
) -> None:
    business, queue = redis_clients
    await release_unpaid_ticket(worker_context(session, business, queue), ticket.id)
    await release_unpaid_ticket(worker_context(session, business, queue), ticket.id)
    assert ticket.status == TicketStatus.CANCELED
    assert (
        int(await business.get(RedisKeys.ticket_type_inventory(ticket.ticket_type_id)))
        == 5
    )
    assert (
        await queue.get(RedisKeys.ticket_type_inventory(ticket.ticket_type_id)) is None
    )


@pytest.mark.parametrize("task", ["single", "batch"])
async def test_worker_does_not_refund_before_database_commit(
    session: AsyncSession,
    ticket: Ticket,
    redis_clients: tuple[FakeRedis, FakeRedis],
    task: str,
) -> None:
    ticket.created_at = datetime.now(UTC) - timedelta(days=1)
    await session.commit()
    business, _ = redis_clients
    # 两处使用同一客户端，只验证事务提交顺序，避免数据库选择影响测试结果。
    ctx = worker_context(session, business, business)
    ticket_id, ticket_type_id = ticket.id, ticket.ticket_type_id
    with patch.object(session, "commit", side_effect=SQLAlchemyError("commit failed")):
        with pytest.raises(SQLAlchemyError):
            if task == "single":
                await release_unpaid_ticket(ctx, ticket_id)
            else:
                await reconcile_hung_tickets(ctx)
    await session.rollback()
    saved = await session.get(Ticket, ticket_id)
    assert saved is not None and saved.status == TicketStatus.RESERVED
    assert int(await business.get(RedisKeys.ticket_type_inventory(ticket_type_id))) == 4


async def test_worker_retries_refund_for_already_canceled_ticket(
    session: AsyncSession, ticket: Ticket, redis_clients: tuple[FakeRedis, FakeRedis]
) -> None:
    business, queue = redis_clients
    ticket.status = TicketStatus.CANCELED
    ticket.ticket_type.tickets_sold = 0
    await session.commit()
    ctx = worker_context(session, business, queue)
    await release_unpaid_ticket(ctx, ticket.id)
    await release_unpaid_ticket(ctx, ticket.id)
    assert (
        int(await business.get(RedisKeys.ticket_type_inventory(ticket.ticket_type_id)))
        == 5
    )


async def test_worker_opens_and_closes_separate_business_redis(
    redis_clients: tuple[FakeRedis, FakeRedis],
) -> None:
    business, queue = redis_clients
    ctx = {"redis": queue}
    with patch("src.worker.Redis.from_url", return_value=business):
        await startup(ctx)
    assert ctx["business_redis"] is business
    assert ctx["redis"] is queue
    with patch.object(business, "aclose", new_callable=AsyncMock) as close:
        await shutdown(ctx)
    close.assert_awaited_once()


async def test_failed_reservation_and_orphan_sweep_do_not_refund_twice(
    session: AsyncSession, ticket: Ticket, redis_clients: tuple[FakeRedis, FakeRedis]
) -> None:
    business, _ = redis_clients
    owner_id, ticket_type_id = ticket.owner_id, ticket.ticket_type_id
    service = TicketService(session, AsyncMock(), business)
    with patch.object(session, "commit", side_effect=SQLAlchemyError("commit failed")):
        with pytest.raises(SQLAlchemyError):
            await service.create(owner_id, TicketCreate(ticket_type_id=ticket_type_id))
    with patch("src.worker.time.time", return_value=10**10):
        await reconcile_redis_orphans(worker_context(session, business, business))
    assert int(await business.get(RedisKeys.ticket_type_inventory(ticket_type_id))) == 4


async def test_post_commit_refresh_failure_keeps_reserved_inventory(
    session: AsyncSession, ticket: Ticket, redis_clients: tuple[FakeRedis, FakeRedis]
) -> None:
    business, queue = redis_clients
    owner_id, ticket_type_id = ticket.owner_id, ticket.ticket_type_id
    with patch.object(
        session, "refresh", side_effect=SQLAlchemyError("refresh failed")
    ):
        with pytest.raises(SQLAlchemyError):
            await TicketService(session, queue, business).create(
                owner_id, TicketCreate(ticket_type_id=ticket_type_id)
            )
    assert int(await business.get(RedisKeys.ticket_type_inventory(ticket_type_id))) == 3
    saved_type = await session.get(TicketType, ticket_type_id)
    assert saved_type is not None and saved_type.tickets_sold == 2


async def test_cancel_api_returns_204_for_retries_and_keeps_canceled_record(
    lifecycle_client: httpx.AsyncClient, ticket: Ticket
) -> None:
    for _ in range(2):
        response = await lifecycle_client.delete(f"/api/v1/tickets/{ticket.id}")
        assert response.status_code == 204
    response = await lifecycle_client.get(f"/api/v1/tickets/{ticket.id}")
    assert response.status_code == 200
    assert response.json()["status"] == "CANCELED"


async def test_cancel_api_rejects_paid_ticket_with_409(
    session: AsyncSession, ticket: Ticket, lifecycle_client: httpx.AsyncClient
) -> None:
    ticket.status = TicketStatus.SOLD
    await session.commit()
    response = await lifecycle_client.delete(f"/api/v1/tickets/{ticket.id}")
    assert response.status_code == 409
    assert ticket.status == TicketStatus.SOLD


async def test_payment_api_rejects_expired_ticket_with_400(
    session: AsyncSession, ticket: Ticket, lifecycle_client: httpx.AsyncClient
) -> None:
    ticket.created_at = datetime.now(UTC) - timedelta(
        seconds=settings.TICKET_RESERVATION_TIME_SECONDS + 1
    )
    await session.commit()
    response = await lifecycle_client.post(f"/api/v1/tickets/{ticket.id}/payments")
    assert response.status_code == 400
    assert ticket.status == TicketStatus.RESERVED
