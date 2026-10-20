from collections.abc import Awaitable, Callable
from typing import Any

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from src.models import Event, Ticket, TicketType, User
from tests.factories import (
    EventFactory,
    TicketFactory,
    TicketTypeFactory,
    UserFactory,
)


@pytest.fixture
async def normal_user(db_connection: AsyncSession) -> User:
    user = UserFactory.build()

    db_connection.add(user)
    await db_connection.commit()
    await db_connection.refresh(user)

    return user


@pytest.fixture
async def superuser(db_connection: AsyncSession) -> User:
    superuser = UserFactory.build(is_superuser=True)

    db_connection.add(superuser)
    await db_connection.commit()
    await db_connection.refresh(superuser)

    return superuser


@pytest.fixture
async def user_in_db(db_connection: AsyncSession) -> User:
    user = UserFactory.build()

    db_connection.add(user)
    await db_connection.commit()
    await db_connection.refresh(user)

    return user


@pytest.fixture
async def event_in_db(db_connection: AsyncSession) -> Event:
    existing_event = EventFactory.build()

    db_connection.add(existing_event)
    await db_connection.commit()
    await db_connection.refresh(existing_event)

    return existing_event


@pytest.fixture
async def ticket_type_in_db(
    db_connection: AsyncSession, event_in_db: Event
) -> TicketType:
    ticket_type = TicketTypeFactory.build(event=event_in_db)

    db_connection.add(ticket_type)
    await db_connection.commit()
    await db_connection.refresh(ticket_type)

    return ticket_type


@pytest.fixture
async def ticket_in_db(
    db_connection: AsyncSession, ticket_type_in_db: TicketType, user_in_db: User
) -> Ticket:
    ticket = TicketFactory.build(ticket_type=ticket_type_in_db, owner=user_in_db)

    db_connection.add(ticket)
    await db_connection.commit()
    await db_connection.refresh(ticket)

    return ticket


@pytest.fixture
async def event_factory(
    db_connection: AsyncSession,
) -> Callable[..., Awaitable[Event]]:
    async def _create(**kwargs: Any) -> Event:
        event = EventFactory.build(**kwargs)

        db_connection.add(event)
        await db_connection.commit()
        await db_connection.refresh(event)

        return event

    return _create


@pytest.fixture
async def ticket_factory(
    db_connection: AsyncSession,
) -> Callable[..., Awaitable[Ticket]]:
    async def _create(event_id: int, **kwargs: Any) -> Ticket:
        ticket = TicketFactory.build(event_id, **kwargs)

        db_connection.add(ticket)
        await db_connection.commit()
        await db_connection.refresh(ticket)

        return ticket

    return _create
