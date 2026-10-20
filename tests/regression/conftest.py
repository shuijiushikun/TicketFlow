"""本地回归测试夹具，使用临时 SQLite 和内存 Redis，不连接实际业务数据。"""

import os
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import AsyncMock

import httpx
import pytest_asyncio
from dotenv import dotenv_values
from fakeredis import FakeServer
from fakeredis.aioredis import FakeRedis
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

for key, value in dotenv_values(Path(__file__).parents[2] / ".env.example").items():
    if value is not None:
        os.environ.setdefault(key, value)

from src.api.deps import get_current_user  # noqa: E402
from src.db.base import Base  # noqa: E402
from src.db.redis import get_arq_pool, get_cache_redis, get_redis  # noqa: E402
from src.db.session import get_db  # noqa: E402
from src.main import app  # noqa: E402
from src.models import Event, Ticket, TicketType, User  # noqa: E402
from src.models.ticket import TicketStatus  # noqa: E402


@pytest_asyncio.fixture
async def session(tmp_path: Path) -> AsyncIterator[AsyncSession]:
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'tickets.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    async with AsyncSession(engine, expire_on_commit=False) as db:
        yield db
    await engine.dispose()


@pytest_asyncio.fixture
async def ticket(session: AsyncSession) -> Ticket:
    owner = User(email="regression@example.com", hashed_password="not-used")
    event = Event(
        title="Campus event",
        date=datetime.now(UTC) + timedelta(days=1),
        country="China",
        city="Shanghai",
        street_address="Campus",
    )
    ticket_type = TicketType(
        name="Standard", price=100, tickets_quantity=5, tickets_sold=1, event=event
    )
    reservation = Ticket(
        owner=owner,
        ticket_type=ticket_type,
        status=TicketStatus.RESERVED,
        created_at=datetime.now(UTC),
    )
    session.add(reservation)
    await session.commit()
    return reservation


@pytest_asyncio.fixture
async def redis_clients(ticket: Ticket) -> AsyncIterator[tuple[FakeRedis, FakeRedis]]:
    server = FakeServer()
    business = FakeRedis(server=server, db=0, decode_responses=True)
    queue = FakeRedis(server=server, db=1, decode_responses=True)
    await business.set(f"ticketcore:inventory:ticket_type:{ticket.ticket_type_id}", 4)
    await business.hset(
        "system:active_reservations", str(ticket.id), f"{ticket.ticket_type_id}:1"
    )
    yield business, queue
    await business.aclose()
    await queue.aclose()


@pytest_asyncio.fixture
async def lifecycle_client(
    session: AsyncSession, ticket: Ticket, redis_clients: tuple[FakeRedis, FakeRedis]
) -> AsyncIterator[httpx.AsyncClient]:
    business, _ = redis_clients

    async def local_db() -> AsyncIterator[AsyncSession]:
        yield session

    original = app.dependency_overrides.copy()
    app.dependency_overrides.update(
        {
            get_db: local_db,
            get_current_user: lambda: ticket.owner,
            get_redis: lambda: business,
            get_cache_redis: lambda: business,
            get_arq_pool: lambda: AsyncMock(),
        }
    )
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            yield client
    finally:
        app.dependency_overrides.clear()
        app.dependency_overrides.update(original)
