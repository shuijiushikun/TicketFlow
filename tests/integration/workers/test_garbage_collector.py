"""真实测试 PostgreSQL 的 worker 业务检查；外部资源运行需根夹具隔离授权。"""

from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.core import settings
from src.core.redis_keys import RedisKeys
from src.db.redis import get_redis
from src.models import Ticket, TicketType, User
from src.models.ticket import TicketStatus
from src.worker import (
    reconcile_hung_tickets,
    reconcile_redis_orphans,
    release_unpaid_ticket,
)
from tests.factories import TicketFactory


def worker_context(session: AsyncSession, redis: AsyncMock) -> dict[str, Any]:
    return {
        "session_maker": async_sessionmaker(session.bind, expire_on_commit=False),
        "business_redis": redis,
    }


def assert_projection(
    redis: AsyncMock, ticket_type_id: int, version: int, available: int
) -> None:
    redis.eval.assert_awaited_once()
    assert redis.eval.await_args is not None
    assert redis.eval.await_args.args[1:] == (
        2,
        RedisKeys.ticket_type_inventory(ticket_type_id),
        RedisKeys.inventory_version(ticket_type_id),
        str(version).zfill(20),
        str(available),
    )


@pytest.mark.asyncio
class TestGarbageCollector:
    async def test_reconcile_hung_tickets_restores_inventory_in_db_and_redis(
        self,
        ticket_type_in_db: TicketType,
        ticket_in_db: Ticket,
        db_connection: AsyncSession,
    ) -> None:
        ticket_type_in_db.tickets_sold = 1
        ticket_type_in_db.inventory_version = 0
        ticket_in_db.status = TicketStatus.RESERVED
        ticket_in_db.created_at = datetime.now(UTC) - timedelta(
            seconds=settings.TICKET_RESERVATION_TIME_SECONDS + 1
        )
        await db_connection.commit()
        ticket_id, type_id = ticket_in_db.id, ticket_type_in_db.id
        mock_redis = AsyncMock()
        await reconcile_hung_tickets(worker_context(db_connection, mock_redis))

        updated_ticket = await db_connection.get(
            Ticket, ticket_id, populate_existing=True
        )
        assert updated_ticket is not None
        assert updated_ticket.status == TicketStatus.CANCELED
        updated_type = await db_connection.get(
            TicketType, type_id, populate_existing=True
        )
        assert updated_type is not None and updated_type.tickets_sold == 0
        assert updated_type.inventory_version == 1
        assert_projection(mock_redis, type_id, 1, updated_type.tickets_quantity)
        mock_redis.hdel.assert_awaited_once_with(
            RedisKeys.active_reservations_hash(), str(ticket_id)
        )

    async def test_ignores_unexpired_reservation(
        self,
        ticket_type_in_db: TicketType,
        ticket_in_db: Ticket,
        db_connection: AsyncSession,
    ) -> None:
        ticket_type_in_db.tickets_sold = 1
        ticket_in_db.status = TicketStatus.RESERVED
        ticket_in_db.created_at = datetime.now(UTC)
        await db_connection.commit()
        ticket_id, type_id = ticket_in_db.id, ticket_type_in_db.id
        mock_redis = AsyncMock()
        await reconcile_hung_tickets(worker_context(db_connection, mock_redis))
        updated = await db_connection.get(Ticket, ticket_id, populate_existing=True)
        assert updated is not None and updated.status == TicketStatus.RESERVED
        updated_type = await db_connection.get(
            TicketType, type_id, populate_existing=True
        )
        assert updated_type is not None and updated_type.tickets_sold == 1
        mock_redis.eval.assert_not_awaited()
        mock_redis.hdel.assert_not_awaited()

    async def test_status_lock_ignores_paid_tickets(
        self, ticket_in_db: Ticket, db_connection: AsyncSession
    ) -> None:
        ticket_in_db.status = TicketStatus.SOLD
        ticket_in_db.created_at = datetime.now(UTC) - timedelta(days=1)
        await db_connection.commit()
        ticket_id = ticket_in_db.id
        mock_redis = AsyncMock()
        await reconcile_hung_tickets(worker_context(db_connection, mock_redis))
        updated = await db_connection.get(Ticket, ticket_id, populate_existing=True)
        assert updated is not None and updated.status == TicketStatus.SOLD
        mock_redis.eval.assert_not_awaited()
        mock_redis.hdel.assert_not_awaited()

    async def test_bulk_processing_consistency(
        self,
        db_connection: AsyncSession,
        ticket_type_in_db: TicketType,
        user_in_db: User,
    ) -> None:
        ticket_type_in_db.tickets_sold = 5
        ticket_type_in_db.inventory_version = 0
        tickets = TicketFactory.batch(
            size=5, ticket_type=ticket_type_in_db, owner=user_in_db
        )
        for ticket in tickets:
            ticket.status = TicketStatus.RESERVED
            ticket.created_at = datetime.now(UTC) - timedelta(days=1)
        tickets[3].created_at = datetime.now(UTC)
        tickets[4].status = TicketStatus.SOLD
        db_connection.add_all(tickets)
        await db_connection.commit()
        ids = [ticket.id for ticket in tickets]
        type_id = ticket_type_in_db.id
        mock_redis = AsyncMock()
        await reconcile_hung_tickets(worker_context(db_connection, mock_redis))

        result = await db_connection.execute(
            select(Ticket.id, Ticket.status).where(Ticket.id.in_(ids))
        )
        status_map = {ticket_id: status for ticket_id, status in result.all()}
        assert [status_map[ticket_id] for ticket_id in ids] == [
            TicketStatus.CANCELED,
            TicketStatus.CANCELED,
            TicketStatus.CANCELED,
            TicketStatus.RESERVED,
            TicketStatus.SOLD,
        ]
        updated_type = await db_connection.get(
            TicketType, type_id, populate_existing=True
        )
        assert updated_type is not None and updated_type.tickets_sold == 2
        assert updated_type.inventory_version == 3
        assert mock_redis.eval.await_count == 3
        assert mock_redis.hdel.await_count == 3


@pytest.mark.asyncio
class TestReleaseUnpaidTicketWorker:
    @pytest.mark.parametrize("ticket_state", ["unexpired", "expired", "paid"])
    async def test_repeated_release_with_real_redis(
        self,
        user_in_db: User,
        db_connection: AsyncSession,
        ticket_type_in_db: TicketType,
        monkeypatch: pytest.MonkeyPatch,
        ticket_state: str,
    ) -> None:
        """真实 Redis 下，提前或重复回收不会错误释放库存。"""
        monkeypatch.setattr(settings, "TICKET_RESERVATION_TIME_SECONDS", 900)
        ticket_type_in_db.tickets_quantity = 5
        ticket_type_in_db.tickets_sold = 1
        ticket_type_in_db.inventory_version = 1
        ticket = TicketFactory.build(
            ticket_type=ticket_type_in_db,
            owner=user_in_db,
            status=TicketStatus.SOLD
            if ticket_state == "paid"
            else TicketStatus.RESERVED,
            created_at=datetime.now(UTC)
            - (timedelta(days=1) if ticket_state == "expired" else timedelta()),
        )
        db_connection.add(ticket)
        await db_connection.commit()
        ticket_id, type_id = ticket.id, ticket_type_in_db.id
        expired = ticket_state == "expired"
        expected_status = TicketStatus.CANCELED if expired else ticket.status
        expected_sold, expected_version = (0, 2) if expired else (1, 1)
        session_maker = async_sessionmaker(db_connection.bind, expire_on_commit=False)

        async with await get_redis() as redis:
            inventory_key = RedisKeys.ticket_type_inventory(type_id)
            version_key = RedisKeys.inventory_version(type_id)
            legacy_key = RedisKeys.active_reservations_hash()
            await redis.set(inventory_key, "4")
            await redis.set(version_key, str(1).zfill(20))
            await redis.hset(legacy_key, str(ticket_id), f"{type_id}:1")
            ctx = {"session_maker": session_maker, "business_redis": redis}

            for _ in range(2):
                await release_unpaid_ticket(ctx, ticket_id)
                async with session_maker() as session:
                    saved_ticket = await session.get(Ticket, ticket_id)
                    saved_type = await session.get(TicketType, type_id)
                    assert saved_ticket is not None and saved_type is not None
                    assert saved_ticket.status == expected_status
                    assert saved_type.tickets_sold == expected_sold
                    assert saved_type.inventory_version == expected_version
                assert await redis.get(inventory_key) == str(5 - expected_sold)
                assert await redis.get(version_key) == str(expected_version).zfill(20)
                assert bool(await redis.hexists(legacy_key, str(ticket_id))) == (
                    ticket_state == "unexpired"
                )

    async def test_release_unpaid_ticket_success(
        self,
        user_in_db: User,
        db_connection: AsyncSession,
        ticket_type_in_db: TicketType,
    ) -> None:
        ticket_type_in_db.tickets_sold = 1
        ticket_type_in_db.inventory_version = 0
        ticket = TicketFactory.build(
            ticket_type=ticket_type_in_db,
            owner=user_in_db,
            status=TicketStatus.RESERVED,
            created_at=datetime.now(UTC) - timedelta(days=1),
        )
        db_connection.add(ticket)
        await db_connection.commit()
        ticket_id, type_id = ticket.id, ticket_type_in_db.id
        mock_redis = AsyncMock()
        await release_unpaid_ticket(
            worker_context(db_connection, mock_redis), ticket_id
        )

        updated = await db_connection.get(Ticket, ticket_id, populate_existing=True)
        assert updated is not None and updated.status == TicketStatus.CANCELED
        updated_type = await db_connection.get(
            TicketType, type_id, populate_existing=True
        )
        assert updated_type is not None and updated_type.tickets_sold == 0
        assert_projection(mock_redis, type_id, 1, updated_type.tickets_quantity)
        mock_redis.hdel.assert_awaited_once_with(
            RedisKeys.active_reservations_hash(), str(ticket_id)
        )

    async def test_paid_ticket_only_cleans_legacy_marker(
        self,
        user_in_db: User,
        db_connection: AsyncSession,
        ticket_type_in_db: TicketType,
    ) -> None:
        ticket_type_in_db.tickets_sold = 1
        ticket = TicketFactory.build(
            ticket_type=ticket_type_in_db, owner=user_in_db, status=TicketStatus.SOLD
        )
        db_connection.add(ticket)
        await db_connection.commit()
        ticket_id, type_id = ticket.id, ticket_type_in_db.id
        mock_redis = AsyncMock()
        await release_unpaid_ticket(
            worker_context(db_connection, mock_redis), ticket_id
        )
        updated = await db_connection.get(Ticket, ticket_id, populate_existing=True)
        assert updated is not None and updated.status == TicketStatus.SOLD
        updated_type = await db_connection.get(
            TicketType, type_id, populate_existing=True
        )
        assert updated_type is not None and updated_type.tickets_sold == 1
        mock_redis.eval.assert_not_awaited()
        mock_redis.hdel.assert_awaited_once_with(
            RedisKeys.active_reservations_hash(), str(ticket_id)
        )

    async def test_ignores_non_existent_ticket(
        self, db_connection: AsyncSession
    ) -> None:
        mock_redis = AsyncMock()
        await release_unpaid_ticket(worker_context(db_connection, mock_redis), 999)
        mock_redis.eval.assert_not_awaited()
        mock_redis.hdel.assert_not_awaited()


@pytest.mark.asyncio
class TestReconcileRedisOrphansWorker:
    async def test_cleans_ghost_marker_without_inventing_inventory(
        self, db_connection: AsyncSession
    ) -> None:
        mock_redis = AsyncMock()
        mock_redis.hscan.return_value = (0, {"999": "5:1"})
        await reconcile_redis_orphans(worker_context(db_connection, mock_redis))
        mock_redis.hscan.assert_awaited_once_with(
            RedisKeys.active_reservations_hash(), cursor=0, count=100
        )
        mock_redis.eval.assert_not_awaited()
        mock_redis.hdel.assert_awaited_once_with(
            RedisKeys.active_reservations_hash(), "999"
        )

    async def test_canceled_ticket_projects_current_database_quantity(
        self,
        db_connection: AsyncSession,
        user_in_db: User,
        ticket_type_in_db: TicketType,
    ) -> None:
        ticket_type_in_db.tickets_sold = 0
        ticket_type_in_db.inventory_version = 0
        ticket = TicketFactory.build(
            ticket_type=ticket_type_in_db,
            owner=user_in_db,
            status=TicketStatus.CANCELED,
        )
        db_connection.add(ticket)
        await db_connection.commit()
        ticket_id, type_id = ticket.id, ticket_type_in_db.id
        mock_redis = AsyncMock()
        mock_redis.hscan.return_value = (0, {str(ticket_id): f"{type_id}:1"})
        await reconcile_redis_orphans(worker_context(db_connection, mock_redis))
        assert_projection(mock_redis, type_id, 0, ticket_type_in_db.tickets_quantity)
        mock_redis.hdel.assert_awaited_once_with(
            RedisKeys.active_reservations_hash(), str(ticket_id)
        )

    async def test_fresh_missing_marker_does_not_need_fixed_grace_period(
        self, db_connection: AsyncSession
    ) -> None:
        mock_redis = AsyncMock()
        mock_redis.hscan.return_value = (
            0,
            {"999": f"1:{int(datetime.now(UTC).timestamp())}"},
        )
        await reconcile_redis_orphans(worker_context(db_connection, mock_redis))
        mock_redis.eval.assert_not_awaited()
        mock_redis.hdel.assert_awaited_once_with(
            RedisKeys.active_reservations_hash(), "999"
        )

    async def test_preserves_marker_for_valid_reserved_ticket(
        self,
        db_connection: AsyncSession,
        user_in_db: User,
        ticket_type_in_db: TicketType,
    ) -> None:
        ticket_type_in_db.tickets_sold = 1
        ticket_type_in_db.inventory_version = 0
        ticket = TicketFactory.build(
            ticket_type=ticket_type_in_db,
            owner=user_in_db,
            status=TicketStatus.RESERVED,
        )
        db_connection.add(ticket)
        await db_connection.commit()
        ticket_id, type_id = ticket.id, ticket_type_in_db.id
        mock_redis = AsyncMock()
        mock_redis.hscan.return_value = (0, {str(ticket_id): f"{type_id}:1"})
        await reconcile_redis_orphans(worker_context(db_connection, mock_redis))
        assert_projection(
            mock_redis, type_id, 0, ticket_type_in_db.tickets_quantity - 1
        )
        mock_redis.hdel.assert_not_awaited()
        updated = await db_connection.get(Ticket, ticket_id, populate_existing=True)
        assert updated is not None and updated.status == TicketStatus.RESERVED

    async def test_mixed_state_bulk_processing(
        self,
        db_connection: AsyncSession,
        user_in_db: User,
        ticket_type_in_db: TicketType,
    ) -> None:
        ticket_type_in_db.tickets_sold = 2
        ticket_type_in_db.inventory_version = 0
        tickets = [
            TicketFactory.build(
                ticket_type=ticket_type_in_db, owner=user_in_db, status=s
            )
            for s in (TicketStatus.RESERVED, TicketStatus.CANCELED, TicketStatus.SOLD)
        ]
        db_connection.add_all(tickets)
        await db_connection.commit()
        reserved_id, canceled_id, sold_id = [ticket.id for ticket in tickets]
        type_id = ticket_type_in_db.id
        mock_redis = AsyncMock()
        mock_redis.hscan.return_value = (
            0,
            {
                str(reserved_id): f"{type_id}:1",
                str(canceled_id): f"{type_id}:1",
                str(sold_id): f"{type_id}:1",
                "999": f"{type_id}:1",
                "888": f"{type_id}:{int(datetime.now(UTC).timestamp())}",
                "bad": "invalid",
            },
        )
        await reconcile_redis_orphans(worker_context(db_connection, mock_redis))
        assert_projection(
            mock_redis, type_id, 0, ticket_type_in_db.tickets_quantity - 2
        )
        mock_redis.hdel.assert_awaited_once()
        assert mock_redis.hdel.await_args is not None
        assert set(mock_redis.hdel.await_args.args[1:]) == {
            str(canceled_id),
            str(sold_id),
            "999",
            "888",
            "bad",
        }
