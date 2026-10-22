"""余票展示只做参考，真实扣票仍由数据库裁决。"""

from unittest.mock import AsyncMock, patch

import httpx
import pytest
from fakeredis.aioredis import FakeRedis
from redis.exceptions import ConnectionError
from sqlalchemy import event
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.cache_policy import CacheCircuitBreaker, DatabaseAdmission
from src.core.redis_keys import RedisKeys
from src.models import Ticket
from src.services import inventory_query
from src.services.inventory import PROJECT_INVENTORY

pytestmark = pytest.mark.asyncio


@pytest.fixture(autouse=True)
def isolated_circuit(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        inventory_query, "cache_circuit", CacheCircuitBreaker(2), raising=False
    )


async def test_inventory_endpoint_returns_remaining_and_not_found(
    lifecycle_client: httpx.AsyncClient, ticket: Ticket
) -> None:
    response = await lifecycle_client.get(
        f"/api/v1/ticket-types/{ticket.ticket_type_id}/inventory"
    )
    assert response.status_code == 200
    assert response.headers["Cache-Control"] == "no-store"
    assert response.json() == {
        "ticket_type_id": ticket.ticket_type_id,
        "remaining": 4,
    }
    missing = await lifecycle_client.get("/api/v1/ticket-types/999999/inventory")
    assert missing.status_code == 404
    invalid = await lifecycle_client.get("/api/v1/ticket-types/0/inventory")
    assert invalid.status_code == 422
    oversized = await lifecycle_client.get(f"/api/v1/ticket-types/{2**31}/inventory")
    assert oversized.status_code == 422


async def test_inventory_display_follows_booking_and_cancellation(
    lifecycle_client: httpx.AsyncClient, ticket: Ticket
) -> None:
    path = f"/api/v1/ticket-types/{ticket.ticket_type_id}/inventory"
    assert (await lifecycle_client.get(path)).json()["remaining"] == 4
    booked = await lifecycle_client.post(
        "/api/v1/tickets", json={"ticket_type_id": ticket.ticket_type_id}
    )
    assert booked.status_code == 201
    assert (await lifecycle_client.get(path)).json()["remaining"] == 3
    canceled = await lifecycle_client.delete(f"/api/v1/tickets/{booked.json()['id']}")
    assert canceled.status_code == 204
    assert (await lifecycle_client.get(path)).json()["remaining"] == 4


async def test_cached_zero_and_deleted_inventory_do_not_query_database(
    lifecycle_client: httpx.AsyncClient,
    session: AsyncSession,
    ticket: Ticket,
    redis_clients: tuple[FakeRedis, FakeRedis],
) -> None:
    business, _ = redis_clients
    type_id = ticket.ticket_type_id
    keys = [
        RedisKeys.ticket_type_inventory(type_id),
        RedisKeys.inventory_version(type_id),
    ]
    await business.eval(PROJECT_INVENTORY, 2, *keys, str(1).zfill(20), "0")
    with patch.object(
        session, "execute", AsyncMock(side_effect=AssertionError("不应查库"))
    ):
        response = await lifecycle_client.get(
            f"/api/v1/ticket-types/{type_id}/inventory"
        )
    assert response.status_code == 200
    assert response.json()["remaining"] == 0
    await business.eval(PROJECT_INVENTORY, 2, *keys, str(2**63).zfill(20), "deleted")
    with patch.object(
        session, "execute", AsyncMock(side_effect=AssertionError("不应查库"))
    ):
        response = await lifecycle_client.get(
            f"/api/v1/ticket-types/{type_id}/inventory"
        )
    assert response.status_code == 404


@pytest.mark.parametrize("corrupt", [False, True])
async def test_missing_or_invalid_projection_is_rebuilt_once(
    lifecycle_client: httpx.AsyncClient,
    session: AsyncSession,
    ticket: Ticket,
    redis_clients: tuple[FakeRedis, FakeRedis],
    corrupt: bool,
) -> None:
    business, _ = redis_clients
    type_id = ticket.ticket_type_id
    if corrupt:
        await business.set(RedisKeys.ticket_type_inventory(type_id), "-4")
        await business.set(RedisKeys.inventory_version(type_id), str(0).zfill(20))
    statements: list[str] = []

    def capture(conn, cursor, statement, parameters, context, executemany):
        if statement.lower().startswith("select") and "ticket_types" in statement:
            statements.append(statement)

    engine = session.bind.sync_engine
    event.listen(engine, "before_cursor_execute", capture)
    try:
        for _ in range(2):
            response = await lifecycle_client.get(
                f"/api/v1/ticket-types/{type_id}/inventory"
            )
            assert response.status_code == 200
            assert response.json()["remaining"] == 4
    finally:
        event.remove(engine, "before_cursor_execute", capture)
    assert len(statements) == 1
    assert await business.get(RedisKeys.ticket_type_inventory(type_id)) == "4"
    assert await business.get(RedisKeys.inventory_version(type_id)) == str(0).zfill(20)


async def test_redis_failure_uses_bounded_database_fallback(
    lifecycle_client: httpx.AsyncClient,
    ticket: Ticket,
    redis_clients: tuple[FakeRedis, FakeRedis],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    business, _ = redis_clients
    admission = DatabaseAdmission(1, 0, 0.05, 1.5)
    monkeypatch.setattr(inventory_query, "cache_admission", admission)
    path = f"/api/v1/ticket-types/{ticket.ticket_type_id}/inventory"
    with patch.object(
        business, "mget", AsyncMock(side_effect=ConnectionError("offline"))
    ):
        response = await lifecycle_client.get(path)
        assert response.status_code == 200
        assert response.json()["remaining"] == 4
        async with admission.slot():
            rejected = await lifecycle_client.get(path)
        assert rejected.status_code == 503


async def test_late_database_fill_cannot_replace_newer_inventory(
    lifecycle_client: httpx.AsyncClient,
    session: AsyncSession,
    ticket: Ticket,
    redis_clients: tuple[FakeRedis, FakeRedis],
) -> None:
    business, _ = redis_clients
    type_id = ticket.ticket_type_id
    keys = [
        RedisKeys.ticket_type_inventory(type_id),
        RedisKeys.inventory_version(type_id),
    ]
    execute = session.execute

    async def interleaved_query(*args, **kwargs):
        result = await execute(*args, **kwargs)
        await business.eval(PROJECT_INVENTORY, 2, *keys, str(1).zfill(20), "1")
        return result

    with patch.object(session, "execute", interleaved_query):
        response = await lifecycle_client.get(
            f"/api/v1/ticket-types/{type_id}/inventory"
        )
    assert response.status_code == 200
    assert response.json()["remaining"] == 4  # 与修改重叠的请求可以返回原快照。
    assert await business.get(keys[0]) == "1"
    subsequent = await lifecycle_client.get(f"/api/v1/ticket-types/{type_id}/inventory")
    assert subsequent.json()["remaining"] == 1
