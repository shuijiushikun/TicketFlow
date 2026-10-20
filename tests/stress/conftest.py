from collections.abc import AsyncGenerator

import pytest
from httpx import AsyncClient
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.db.session import get_db
from src.main import app
from src.models import Event, Ticket, TicketType


@pytest.fixture
async def test_session_factory(
    db_connection: AsyncSession,
) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(bind=db_connection.bind, expire_on_commit=False)


@pytest.fixture
async def stress_client(
    client: AsyncClient,
    test_session_factory: async_sessionmaker[AsyncSession],
) -> AsyncGenerator[AsyncClient, None]:
    async def override_get_db_concurrent() -> AsyncGenerator[AsyncSession, None]:
        async with test_session_factory() as session:
            yield session

    original_override = app.dependency_overrides.get(get_db)
    app.dependency_overrides[get_db] = override_get_db_concurrent

    try:
        yield client
    finally:
        if original_override:
            app.dependency_overrides[get_db] = original_override
        else:
            app.dependency_overrides.pop(get_db, None)


@pytest.fixture
async def cleanup_physical_db(
    test_session_factory: async_sessionmaker[AsyncSession],
) -> AsyncGenerator[None, None]:
    yield
    async with test_session_factory() as session:
        await session.execute(delete(Ticket))
        await session.execute(delete(TicketType))
        await session.execute(delete(Event))
        await session.commit()
