"""异步秒杀的必要本地检查；不连接业务 PostgreSQL 或 Redis。"""

from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, patch

import httpx
import pytest
import pytest_asyncio
from arq import Retry
from redis.exceptions import ConnectionError
from sqlalchemy import func, select, update
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.api.deps import get_current_user
from src.core.cache_policy import QueryRateLimiter
from src.core.config import settings
from src.core.seckill_policy import SubmissionAdmission
from src.main import app
from src.models import OutboxEvent, Ticket, TicketType, User
from src.models.seckill import SeckillRequest
from src.services.inventory import transaction_outbox
from src.services.seckill_queue import SECKILL_DISPATCH

pytestmark = pytest.mark.asyncio


async def test_full_entry_rejects_before_user_lookup_or_database_writes(
    lifecycle_client, session, ticket, monkeypatch
):
    gate = SubmissionAdmission(1, 0, 1)
    monkeypatch.setattr(
        "src.api.v1.seckill.SUBMIT_ADMISSION",
        gate,
    )
    lookup = AsyncMock(return_value=User(id=ticket.owner_id))

    async def current_user():
        return await lookup()

    monkeypatch.setitem(app.dependency_overrides, get_current_user, current_user)
    ticket.ticket_type.is_seckill = True
    await session.commit()
    async with gate.slot():
        response = await lifecycle_client.post(
            "/api/v1/seckill/requests",
            json={"ticket_type_id": ticket.ticket_type_id},
            headers={"Idempotency-Key": "seckill-entry-full-001"},
        )
    assert response.status_code == 503
    assert response.headers["Retry-After"] == "1"
    lookup.assert_not_awaited()
    assert await session.scalar(select(func.count(SeckillRequest.id))) == 0
    assert await session.scalar(select(func.count(OutboxEvent.id))) == 0


@pytest_asyncio.fixture(autouse=True)
async def isolated_request_state(lifecycle_client, ticket, monkeypatch):
    """错误响应会回滚共享测试会话，身份对象需独立于 ORM 夹具。"""
    owner_id = ticket.owner_id
    monkeypatch.setitem(
        app.dependency_overrides,
        get_current_user,
        lambda: User(id=owner_id, email="seckill@example.com", is_active=True),
    )
    monkeypatch.setattr(settings, "SECKILL_ENABLED", True)
    monkeypatch.setattr(
        "src.api.v1.seckill.SUBMIT_ADMISSION", SubmissionAdmission(8, 56, 1)
    )
    for name in ("SUBMIT_LIMITER", "POLL_LIMITER"):
        monkeypatch.setattr(
            f"src.api.v1.seckill.{name}", QueryRateLimiter(100, 200, 100, 200, 100)
        )


async def test_seckill_receipt_is_durable_idempotent_and_owned(
    lifecycle_client: httpx.AsyncClient,
    session: AsyncSession,
    ticket: Ticket,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(settings, "SECKILL_ENABLED", True)
    ticket.ticket_type.is_seckill = True
    await session.commit()
    payload = {"ticket_type_id": ticket.ticket_type_id}
    headers = {"Idempotency-Key": "seckill-receipt-001"}
    first = await lifecycle_client.post(
        "/api/v1/seckill/requests", json=payload, headers=headers
    )
    assert first.status_code == 202, first.text
    # 队列已经满时，同一次重试仍先复用持久响应，不重新占用名额。
    monkeypatch.setattr(settings, "SECKILL_MAX_PENDING", 1)
    second = await lifecycle_client.post(
        "/api/v1/seckill/requests", json=payload, headers=headers
    )
    assert first.json() == second.json()
    assert first.headers["Location"].endswith(str(first.json()["id"]))
    assert await session.scalar(select(TicketType.tickets_sold)) == 1
    assert await session.scalar(select(func.count(Ticket.id))) == 1
    found = await lifecycle_client.get(first.headers["Location"])
    assert found.status_code == 200
    assert found.json()["status"] == "QUEUED"
    assert found.headers["Cache-Control"] == "no-store"
    changed = await lifecycle_client.post(
        "/api/v1/seckill/requests",
        json={"ticket_type_id": payload["ticket_type_id"] + 1},
        headers=headers,
    )
    assert changed.status_code == 409
    monkeypatch.setitem(
        app.dependency_overrides,
        get_current_user,
        lambda: User(id=999, email="other@example.com", is_active=True),
    )
    assert (await lifecycle_client.get(first.headers["Location"])).status_code == 404
    assert (
        await lifecycle_client.get("/api/v1/seckill/requests/99999")
    ).status_code == 404


async def test_seckill_worker_commits_once_and_reuses_ticket_lifecycle(
    lifecycle_client: httpx.AsyncClient,
    session: AsyncSession,
    ticket: Ticket,
    redis_clients,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.seckill_worker import process_seckill_request

    monkeypatch.setattr(settings, "SECKILL_ENABLED", True)
    ticket.ticket_type.is_seckill = True
    ticket.ticket_type.tickets_quantity = 2
    await session.commit()
    response = await lifecycle_client.post(
        "/api/v1/seckill/requests",
        json={"ticket_type_id": ticket.ticket_type_id},
        headers={"Idempotency-Key": "seckill-worker-001"},
    )
    assert response.status_code == 202, response.text
    business, _ = redis_clients
    ctx = {
        "session_maker": async_sessionmaker(session.bind, expire_on_commit=False),
        "business_redis": business,
        "redis": AsyncMock(),
    }
    await session.rollback()
    await process_seckill_request(ctx, response.json()["id"])
    await process_seckill_request(ctx, response.json()["id"])
    result = await lifecycle_client.get(response.headers["Location"])
    assert result.json()["status"] == "SUCCEEDED"
    ticket_id = result.json()["ticket_id"]
    assert await session.scalar(select(TicketType.tickets_sold)) == 2
    assert await session.scalar(select(func.count(Ticket.id))) == 2
    canceled = await lifecycle_client.delete(f"/api/v1/tickets/{ticket_id}")
    assert canceled.status_code == 204
    assert await session.scalar(select(TicketType.tickets_sold)) == 1


async def test_seckill_expired_request_does_not_take_inventory(
    lifecycle_client,
    session,
    ticket,
    redis_clients,
    monkeypatch,
) -> None:
    from src.models.seckill import SeckillRequest
    from src.seckill_worker import process_seckill_request

    monkeypatch.setattr(settings, "SECKILL_ENABLED", True)
    ticket.ticket_type.is_seckill = True
    await session.commit()
    response = await lifecycle_client.post(
        "/api/v1/seckill/requests",
        json={"ticket_type_id": ticket.ticket_type_id},
        headers={"Idempotency-Key": "seckill-expiry-001"},
    )
    application = await session.get(SeckillRequest, response.json()["id"])
    application.expires_at = datetime.now(UTC) - timedelta(seconds=1)
    await session.commit()
    business, _ = redis_clients
    await process_seckill_request(
        {
            "session_maker": async_sessionmaker(session.bind, expire_on_commit=False),
            "business_redis": business,
            "redis": AsyncMock(),
        },
        application.id,
    )
    await session.rollback()
    result = await lifecycle_client.get(response.headers["Location"])
    assert result.json()["status"] == "EXPIRED"
    assert await session.scalar(select(TicketType.tickets_sold)) == 1
    assert await session.scalar(select(func.count(Ticket.id))) == 1


async def test_seckill_requires_key_and_separates_booking_modes(
    lifecycle_client,
    session,
    ticket,
    monkeypatch,
) -> None:
    monkeypatch.setattr(settings, "SECKILL_ENABLED", True)
    payload = {"ticket_type_id": ticket.ticket_type_id}
    owner_id = ticket.owner_id
    monkeypatch.setitem(
        app.dependency_overrides,
        get_current_user,
        lambda: User(id=owner_id, email="seckill-mode@example.com", is_active=True),
    )
    assert (
        await lifecycle_client.post("/api/v1/seckill/requests", json=payload)
    ).status_code == 422
    headers = {"Idempotency-Key": "seckill-mode-001"}
    assert (
        await lifecycle_client.post(
            "/api/v1/seckill/requests", json=payload, headers=headers
        )
    ).status_code == 409
    await session.execute(
        update(TicketType)
        .where(TicketType.id == payload["ticket_type_id"])
        .values(is_seckill=True)
    )
    await session.commit()
    assert (
        await lifecycle_client.post("/api/v1/tickets", json=payload)
    ).status_code == 409
    # 没有成功进入队列或扣票，不产生业务副作用。
    assert await session.scalar(select(func.count(Ticket.id))) == 1
    assert await session.scalar(select(func.count(OutboxEvent.id))) == 0


@pytest.mark.parametrize("outcome", ["SOLD_OUT", "REJECTED"])
async def test_seckill_worker_rechecks_stock_and_booking_mode(
    lifecycle_client,
    session,
    ticket,
    redis_clients,
    outcome,
):
    from src.seckill_worker import process_seckill_request

    ticket.ticket_type.is_seckill = True
    ticket.ticket_type.tickets_quantity = 1
    await session.commit()
    response = await lifecycle_client.post(
        "/api/v1/seckill/requests",
        json={"ticket_type_id": ticket.ticket_type_id},
        headers={"Idempotency-Key": "seckill-recheck-001"},
    )
    if outcome == "REJECTED":
        ticket.ticket_type.is_seckill = False
        await session.commit()
    await process_seckill_request(
        {
            "session_maker": async_sessionmaker(session.bind, expire_on_commit=False),
            "business_redis": redis_clients[0],
            "redis": AsyncMock(),
        },
        response.json()["id"],
    )
    result = await lifecycle_client.get(response.headers["Location"])
    assert result.json()["status"] == outcome
    assert result.json()["ticket_id"] is None
    assert await session.scalar(select(TicketType.tickets_sold)) == 1
    assert await session.scalar(select(func.count(Ticket.id))) == 1


@pytest.mark.parametrize("failure", ["full", "offline"])
async def test_seckill_admission_rejects_without_creating_application(
    lifecycle_client,
    session,
    ticket,
    redis_clients,
    monkeypatch,
    failure,
):
    ticket.ticket_type.is_seckill = True
    await session.commit()
    payload = {"ticket_type_id": ticket.ticket_type_id}
    business = redis_clients[0]
    if failure == "full":
        monkeypatch.setattr(settings, "SECKILL_MAX_PENDING", 1)
        first = await lifecycle_client.post(
            "/api/v1/seckill/requests",
            json=payload,
            headers={"Idempotency-Key": "seckill-capacity-001"},
        )
        assert first.status_code == 202
        expected = 1
        rejected = await lifecycle_client.post(
            "/api/v1/seckill/requests",
            json=payload,
            headers={"Idempotency-Key": "seckill-capacity-002"},
        )
    else:
        expected = 0
        with patch.object(business, "eval", side_effect=ConnectionError("offline")):
            rejected = await lifecycle_client.post(
                "/api/v1/seckill/requests",
                json=payload,
                headers={"Idempotency-Key": "seckill-offline-001"},
            )
    assert rejected.status_code == 503
    assert await session.scalar(select(func.count(SeckillRequest.id))) == expected
    assert await session.scalar(select(TicketType.tickets_sold)) == 1


async def test_seckill_recovers_failed_delivery_and_lost_queue(
    lifecycle_client,
    session,
    ticket,
    redis_clients,
    monkeypatch,
):
    from src.db.redis import get_arq_pool
    from src.seckill_worker import reconcile_seckill_requests

    ticket.ticket_type.is_seckill = True
    await session.commit()
    broken = AsyncMock()
    broken.enqueue_job.side_effect = ConnectionError("queue offline")
    monkeypatch.setitem(app.dependency_overrides, get_arq_pool, lambda: broken)
    response = await lifecycle_client.post(
        "/api/v1/seckill/requests",
        json={"ticket_type_id": ticket.ticket_type_id},
        headers={"Idempotency-Key": "seckill-delivery-001"},
    )
    assert response.status_code == 202
    pending_id = await session.scalar(
        select(OutboxEvent.id).where(OutboxEvent.kind == SECKILL_DISPATCH)
    )
    assert pending_id is not None
    pool = AsyncMock()
    dispatcher = transaction_outbox(session, redis_clients[0], arq_pool=pool)
    assert await dispatcher.dispatch(pending_id)
    assert pool.enqueue_job.await_count == 1
    # 即使 Outbox 已确认，随后 Redis 丢失任务，仍能从申请记录补投。
    await session.execute(
        update(SeckillRequest).values(
            next_dispatch_at=datetime.now(UTC) - timedelta(seconds=1)
        )
    )
    await session.commit()
    pool.reset_mock()
    await reconcile_seckill_requests(
        {
            "session_maker": async_sessionmaker(session.bind, expire_on_commit=False),
            "business_redis": redis_clients[0],
            "redis": pool,
        }
    )
    pool.enqueue_job.assert_awaited_once()
    assert pool.enqueue_job.await_args.args == (
        "process_seckill_request",
        response.json()["id"],
    )
    assert await session.scalar(select(TicketType.tickets_sold)) == 1


async def test_seckill_transaction_failure_rolls_back_stock_and_retries(
    lifecycle_client,
    session,
    ticket,
    redis_clients,
):
    from src.seckill_worker import process_seckill_request
    from src.services.reservations import reserve_ticket

    ticket.ticket_type.is_seckill = True
    await session.commit()
    response = await lifecycle_client.post(
        "/api/v1/seckill/requests",
        json={"ticket_type_id": ticket.ticket_type_id},
        headers={"Idempotency-Key": "seckill-rollback-001"},
    )
    ctx = {
        "session_maker": async_sessionmaker(session.bind, expire_on_commit=False),
        "business_redis": redis_clients[0],
        "redis": AsyncMock(),
    }

    async def fail_after_booking(*args, **kwargs):
        await reserve_ticket(*args, **kwargs)
        raise SQLAlchemyError("simulate failure before transaction commit")

    with (
        patch("src.seckill_worker.reserve_ticket", fail_after_booking),
        pytest.raises(Retry),
    ):
        await process_seckill_request(ctx, response.json()["id"])
    assert await session.scalar(select(TicketType.tickets_sold)) == 1
    assert await session.scalar(select(func.count(Ticket.id))) == 1
    assert await session.scalar(select(SeckillRequest.status)) == "QUEUED"
    await session.rollback()
    await process_seckill_request(ctx, response.json()["id"])
    result = await lifecycle_client.get(response.headers["Location"])
    assert result.json()["status"] == "SUCCEEDED"
    assert await session.scalar(select(TicketType.tickets_sold)) == 2
