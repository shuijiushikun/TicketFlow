import time
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from src.core import settings
from src.core.redis_keys import RedisKeys
from src.models import Ticket, TicketType, User
from src.models.ticket import TicketStatus
from src.worker import (
    reconcile_hung_tickets,
    reconcile_redis_orphans,
    release_unpaid_ticket,
)
from tests.factories import TicketFactory


@pytest.mark.asyncio
class TestGarbageCollector:
    async def test_reconcile_hung_tickets_restores_inventory_in_db_and_redis(
        self,
        ticket_type_in_db: TicketType,
        ticket_in_db: Ticket,
        db_connection: AsyncSession,
    ) -> None:
        ticket_type_in_db.tickets_sold = 1
        ticket_in_db.status = TicketStatus.RESERVED

        db_connection.add_all([ticket_in_db, ticket_type_in_db])
        await db_connection.commit()

        past_time = datetime.now(UTC) - timedelta(minutes=20)
        await db_connection.execute(
            update(Ticket)
            .where(Ticket.id == ticket_in_db.id)
            .values(created_at=past_time)
        )
        await db_connection.commit()

        @asynccontextmanager
        async def mock_session_maker() -> AsyncIterator[AsyncSession]:
            yield db_connection

        mock_redis = AsyncMock()

        ctx = {
            "session_maker": mock_session_maker,
            "business_redis": mock_redis,
        }

        await reconcile_hung_tickets(ctx)

        db_connection.expunge_all()

        updated_ticket = await db_connection.get(Ticket, ticket_in_db.id)
        assert updated_ticket is not None
        assert updated_ticket.status == TicketStatus.CANCELED

        updated_ticket_type = await db_connection.get(TicketType, ticket_type_in_db.id)
        assert updated_ticket_type is not None
        assert updated_ticket_type.tickets_sold == 0

        inventory_key = RedisKeys.ticket_type_inventory(ticket_type_in_db.id)
        canceled_set_key = RedisKeys.canceled_tickets_set()
        active_hash_key = RedisKeys.active_reservations_hash()

        assert mock_redis.eval.call_count == 1

        call_args = mock_redis.eval.call_args
        assert call_args is not None

        (
            passed_script,
            numkeys,
            passed_inv_key,
            passed_set_key,
            passed_active_hash,
            passed_ticket_id,
        ) = call_args.args

        assert numkeys == 3
        assert passed_inv_key == inventory_key
        assert passed_set_key == canceled_set_key
        assert passed_active_hash == active_hash_key
        assert int(passed_ticket_id) == ticket_in_db.id
        assert "SISMEMBER" in passed_script
        assert "INCR" in passed_script
        assert "HDEL" in passed_script

    async def test_grace_ignores_fresh_reservation(
        self,
        ticket_type_in_db: TicketType,
        ticket_in_db: Ticket,
        db_connection: AsyncSession,
    ) -> None:
        ticket_type_in_db.tickets_sold = 1
        ticket_in_db.status = TicketStatus.RESERVED

        db_connection.add_all([ticket_in_db, ticket_type_in_db])
        await db_connection.commit()

        recent_time = datetime.now(UTC) - timedelta(
            seconds=settings.TICKET_RESERVATION_TIME_SECONDS - 1
        )
        await db_connection.execute(
            update(Ticket)
            .where(Ticket.id == ticket_in_db.id)
            .values(created_at=recent_time)
        )
        await db_connection.commit()

        @asynccontextmanager
        async def mock_session_maker() -> AsyncIterator[AsyncSession]:
            yield db_connection

        ctx = {"session_maker": mock_session_maker, "business_redis": AsyncMock()}
        await reconcile_hung_tickets(ctx)

        db_connection.expunge_all()

        updated_ticket = await db_connection.get(Ticket, ticket_in_db.id)
        assert updated_ticket is not None
        assert updated_ticket.status == TicketStatus.RESERVED

        updated_ticket_type = await db_connection.get(TicketType, ticket_type_in_db.id)
        assert updated_ticket_type is not None
        assert updated_ticket_type.tickets_sold == 1

    async def test_status_lock_ignores_paid_tickets(
        self,
        ticket_in_db: Ticket,
        db_connection: AsyncSession,
    ) -> None:
        ticket_in_db.status = TicketStatus.SOLD
        db_connection.add(ticket_in_db)
        await db_connection.commit()

        very_past_time = datetime.now(UTC) - timedelta(hours=2)
        await db_connection.execute(
            update(Ticket)
            .where(Ticket.id == ticket_in_db.id)
            .values(created_at=very_past_time)
        )
        await db_connection.commit()

        @asynccontextmanager
        async def mock_session_maker() -> AsyncIterator[AsyncSession]:
            yield db_connection

        ctx = {"session_maker": mock_session_maker, "business_redis": AsyncMock()}
        await reconcile_hung_tickets(ctx)

        db_connection.expunge_all()

        update_ticket = await db_connection.get(Ticket, ticket_in_db.id)
        assert update_ticket is not None
        assert update_ticket.status == TicketStatus.SOLD

    async def test_bulk_processing_consistency(
        self,
        db_connection: AsyncSession,
        ticket_type_in_db: TicketType,
        user_in_db: User,
    ) -> None:
        ticket_type_in_db.tickets_sold = 5
        db_connection.add(ticket_type_in_db)
        await db_connection.flush()

        tickets = TicketFactory.batch(
            size=5, ticket_type=ticket_type_in_db, owner=user_in_db
        )
        for i in range(4):
            tickets[i].status = TicketStatus.RESERVED
        tickets[4].status = TicketStatus.SOLD

        db_connection.add_all(tickets)
        await db_connection.commit()

        time_past = datetime.now(UTC) - timedelta(days=1)
        time_recent = datetime.now(UTC) - timedelta(minutes=5)

        past_ids = [tickets[0].id, tickets[1].id, tickets[2].id, tickets[4].id]

        await db_connection.execute(
            update(Ticket).where(Ticket.id.in_(past_ids)).values(created_at=time_past)
        )

        await db_connection.execute(
            update(Ticket)
            .where(Ticket.id == tickets[3].id)
            .values(created_at=time_recent)
        )
        await db_connection.commit()

        @asynccontextmanager
        async def mock_session_maker() -> AsyncIterator[AsyncSession]:
            yield db_connection

        mock_redis = AsyncMock()
        ctx = {"session_maker": mock_session_maker, "business_redis": mock_redis}
        await reconcile_hung_tickets(ctx)

        db_connection.expunge_all()

        query = select(Ticket).where(Ticket.ticket_type_id == ticket_type_in_db.id)
        db_tickets: Sequence[Ticket] = (await db_connection.scalars(query)).all()

        status_map = {t.id: t.status for t in db_tickets}

        assert status_map[tickets[0].id] == TicketStatus.CANCELED
        assert status_map[tickets[1].id] == TicketStatus.CANCELED
        assert status_map[tickets[2].id] == TicketStatus.CANCELED
        assert status_map[tickets[3].id] == TicketStatus.RESERVED
        assert status_map[tickets[4].id] == TicketStatus.SOLD

        updated_ticket_type = await db_connection.get(TicketType, ticket_type_in_db.id)
        assert updated_ticket_type is not None
        assert updated_ticket_type.tickets_sold == 2

        assert mock_redis.eval.call_count == 3


@pytest.mark.asyncio
class TestReleaseUnpaidTicketWorker:
    async def test_release_unpaid_ticket_success(
        self,
        user_in_db: User,
        db_connection: AsyncSession,
        ticket_type_in_db: TicketType,
    ) -> None:
        ticket_type_in_db.tickets_sold = 1
        db_connection.add(ticket_type_in_db)
        await db_connection.flush()

        ticket = TicketFactory.build(
            ticket_type=ticket_type_in_db,
            owner=user_in_db,
            status=TicketStatus.RESERVED,
        )
        db_connection.add(ticket)
        await db_connection.commit()

        @asynccontextmanager
        async def mock_session_maker() -> AsyncIterator[AsyncSession]:
            yield db_connection

        mock_redis = AsyncMock()
        ctx = {"session_maker": mock_session_maker, "business_redis": mock_redis}

        await release_unpaid_ticket(ctx, ticket_id=ticket.id)

        db_connection.expunge_all()

        updated_ticket = await db_connection.get(Ticket, ticket.id)
        assert updated_ticket is not None
        assert updated_ticket.status == TicketStatus.CANCELED

        updated_ticket_type = await db_connection.get(TicketType, ticket_type_in_db.id)
        assert updated_ticket_type is not None
        assert updated_ticket_type.tickets_sold == 0

        assert mock_redis.eval.call_count == 1

    async def test_ignores_already_paid_ticket(
        self,
        user_in_db: User,
        db_connection: AsyncSession,
        ticket_type_in_db: TicketType,
    ) -> None:
        ticket_type_in_db.tickets_sold = 1
        db_connection.add(ticket_type_in_db)
        await db_connection.flush()

        ticket = TicketFactory.build(
            ticket_type=ticket_type_in_db,
            owner=user_in_db,
            status=TicketStatus.SOLD,
        )
        db_connection.add(ticket)
        await db_connection.commit()

        @asynccontextmanager
        async def mock_session_maker() -> AsyncIterator[AsyncSession]:
            yield db_connection

        mock_redis = AsyncMock()
        ctx = {"session_maker": mock_session_maker, "business_redis": mock_redis}
        await release_unpaid_ticket(ctx, ticket_id=ticket.id)

        db_connection.expunge_all()

        updated_ticket = await db_connection.get(Ticket, ticket.id)
        assert updated_ticket is not None
        assert updated_ticket.status == TicketStatus.SOLD

        updated_ticket_type = await db_connection.get(TicketType, ticket_type_in_db.id)
        assert updated_ticket_type is not None
        assert updated_ticket_type.tickets_sold == 1

        assert mock_redis.eval.call_count == 0

    async def test_ignores_non_existent_ticket(
        self,
        db_connection: AsyncSession,
    ) -> None:
        @asynccontextmanager
        async def mock_session_maker() -> AsyncIterator[AsyncSession]:
            yield db_connection

        mock_redis = AsyncMock()
        ctx = {"session_maker": mock_session_maker, "business_redis": mock_redis}

        await release_unpaid_ticket(ctx, ticket_id=999)

        assert mock_redis.eval.call_count == 0


@pytest.mark.asyncio
class TestReconcileRedisOrphansWorker:
    async def test_refunds_ghost_ticket_missing_in_db(
        self,
        db_connection: AsyncSession,
    ) -> None:
        @asynccontextmanager
        async def mock_session_maker() -> AsyncIterator[AsyncSession]:
            yield db_connection

        mock_redis = AsyncMock()

        past_time = int(time.time()) - 100
        mock_redis.hgetall.return_value = {b"999": f"5:{past_time}".encode()}

        ctx = {"session_maker": mock_session_maker, "business_redis": mock_redis}
        await reconcile_redis_orphans(ctx)

        assert mock_redis.eval.call_count == 1

        call_args = mock_redis.eval.call_args
        assert call_args is not None
        (
            passed_script,
            numkeys,
            passed_inv_key,
            passed_set_key,
            passed_active_hash,
            passed_ticket_id,
        ) = call_args.args

        assert passed_inv_key == RedisKeys.ticket_type_inventory(5)
        assert passed_active_hash == RedisKeys.active_reservations_hash()
        assert int(passed_ticket_id) == 999

    async def test_refunds_canceled_ticket_with_stuck_lock(
        self,
        db_connection: AsyncSession,
        user_in_db: User,
        ticket_type_in_db: TicketType,
    ) -> None:
        ticket = TicketFactory.build(
            ticket_type=ticket_type_in_db,
            owner=user_in_db,
            status=TicketStatus.CANCELED,
        )
        db_connection.add(ticket)
        await db_connection.commit()

        @asynccontextmanager
        async def mock_session_maker() -> AsyncIterator[AsyncSession]:
            yield db_connection

        mock_redis = AsyncMock()
        past_time = int(time.time()) - 100
        mock_redis.hgetall.return_value = {
            str(ticket.id).encode(
                "utf-8"
            ): f"{ticket_type_in_db.id}:{past_time}".encode()
        }

        ctx = {"session_maker": mock_session_maker, "business_redis": mock_redis}
        await reconcile_redis_orphans(ctx)

        assert mock_redis.eval.call_count == 1

    async def test_ignores_fresh_reservation_grace_period(
        self,
        db_connection: AsyncSession,
    ) -> None:
        @asynccontextmanager
        async def mock_session_maker() -> AsyncIterator[AsyncSession]:
            yield db_connection

        mock_redis = AsyncMock()
        fresh_time = int(time.time()) - 10
        mock_redis.hgetall.return_value = {b"999": f"1:{fresh_time}".encode()}

        ctx = {"session_maker": mock_session_maker, "business_redis": mock_redis}
        await reconcile_redis_orphans(ctx)

        assert mock_redis.eval.call_count == 0

    async def test_ignores_valid_db_ticket(
        self,
        db_connection: AsyncSession,
        user_in_db: User,
        ticket_type_in_db: TicketType,
    ) -> None:
        ticket = TicketFactory.build(
            ticket_type=ticket_type_in_db,
            owner=user_in_db,
            status=TicketStatus.RESERVED,
        )
        db_connection.add(ticket)
        await db_connection.commit()

        @asynccontextmanager
        async def mock_session_maker() -> AsyncIterator[AsyncSession]:
            yield db_connection

        mock_redis = AsyncMock()
        past_time = int(time.time()) - 100
        mock_redis.hgetall.return_value = {
            str(ticket.id).encode(
                "utf-8"
            ): f"{ticket_type_in_db.id}:{past_time}".encode()
        }

        ctx = {"session_maker": mock_session_maker, "business_redis": mock_redis}
        await reconcile_redis_orphans(ctx)

        assert mock_redis.eval.call_count == 0

    async def test_mixed_state_bulk_processing(
        self,
        db_connection: AsyncSession,
        user_in_db: User,
        ticket_type_in_db: TicketType,
    ) -> None:
        t_valid = TicketFactory.build(
            ticket_type=ticket_type_in_db,
            owner=user_in_db,
            status=TicketStatus.RESERVED,
        )
        t_canceled = TicketFactory.build(
            ticket_type=ticket_type_in_db,
            owner=user_in_db,
            status=TicketStatus.CANCELED,
        )

        db_connection.add_all([t_valid, t_canceled])
        await db_connection.commit()

        @asynccontextmanager
        async def mock_session_maker() -> AsyncIterator[AsyncSession]:
            yield db_connection

        mock_redis = AsyncMock()

        past = int(time.time()) - 100
        fresh = int(time.time()) - 5

        mock_redis.hgetall.return_value = {
            str(t_valid.id).encode("utf-8"): f"{ticket_type_in_db.id}:{past}".encode(),
            str(t_canceled.id).encode(
                "utf-8"
            ): f"{ticket_type_in_db.id}:{past}".encode(),
            b"999": f"{ticket_type_in_db.id}:{past}".encode(),
            b"888": f"{ticket_type_in_db.id}:{fresh}".encode(),
        }

        ctx = {"session_maker": mock_session_maker, "business_redis": mock_redis}
        await reconcile_redis_orphans(ctx)

        assert mock_redis.eval.call_count == 2
