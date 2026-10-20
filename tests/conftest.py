import os
import uuid
from collections.abc import AsyncGenerator, Generator
from pathlib import Path
from unittest.mock import AsyncMock

import asyncpg
import pytest
import pytest_asyncio
from dotenv import dotenv_values
from httpx import ASGITransport, AsyncClient
from redis.asyncio import Redis
from sqlalchemy import NullPool, text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine

from tests.resource_safety import RESOURCE_MARKER_KEY, TestResources

BASE_DIR = Path(__file__).resolve().parent.parent

if any(key.startswith("TICKETCORE_TEST_") for key in os.environ):
    try:
        TestResources.from_environment(os.environ).configure_environment(os.environ)
    except ValueError as error:
        raise pytest.UsageError(str(error)) from error

# 仅提供非敏感默认值；测试目标必须来自上方显式配置，不能从业务 .env 推导。
for key, value in dotenv_values(BASE_DIR / ".env.example").items():
    if value is not None:
        os.environ.setdefault(key, value)

from src.db.base import Base  # noqa: E402
from src.db.redis import (  # noqa: E402
    close_redis_pool,
    get_arq_pool,
    init_redis_pool,
)
from src.db.session import get_db  # noqa: E402
from src.main import app  # noqa: E402


@pytest.fixture(autouse=True)
def external_resources(request: pytest.FixtureRequest) -> None:
    """只有集成和并发测试启用外部资源，单元/本地回归不执行清库。"""
    relative = request.node.path.relative_to(BASE_DIR / "tests")
    if relative.parts[0] in {"integration", "stress"}:
        request.getfixturevalue("clean_tables")
        request.getfixturevalue("flush_redis_between_tests")


@pytest.fixture(scope="session")
def test_resources() -> TestResources:
    try:
        return TestResources.from_environment(os.environ)
    except ValueError as error:
        raise pytest.UsageError(str(error)) from error


@pytest.fixture(scope="session")
def test_engine(test_resources: TestResources) -> Generator[AsyncEngine, None, None]:
    yield create_async_engine(test_resources.sqlalchemy_url, poolclass=NullPool)


async def verify_redis_owner(resources: TestResources) -> None:
    """使用不被 FLUSHDB 清除的独立控制库验证测试实例所有权。"""
    async with Redis.from_url(
        resources.redis_control_url, decode_responses=True
    ) as control:
        resources.require_marker(await control.get(RESOURCE_MARKER_KEY))


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def setup_test_db(
    test_resources: TestResources,
    test_engine: AsyncEngine,
) -> AsyncGenerator[None, None]:
    await verify_redis_owner(test_resources)
    conn = await asyncpg.connect(test_resources.admin_dsn)
    database_name = test_resources.database_name
    try:
        marker = await conn.fetchval(
            "SELECT shobj_description(oid, 'pg_database') "
            "FROM pg_database WHERE datname = $1",
            database_name,
        )
        test_resources.require_marker(marker)
        await conn.execute(f'DROP DATABASE "{database_name}"')
        await conn.execute(f'CREATE DATABASE "{database_name}"')
        comment = await conn.fetchval(
            "SELECT format('COMMENT ON DATABASE %I IS %L', $1::text, $2::text)",
            database_name,
            test_resources.marker,
        )
        await conn.execute(comment)
    finally:
        await conn.close()
    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    try:
        yield
    finally:
        # 保留空的测试库及所有权标记，便于下次重置；不终止其他数据库连接。
        async with test_engine.begin() as conn:
            await conn.run_sync(Base.metadata.drop_all)
        await test_engine.dispose()


@pytest_asyncio.fixture
async def clean_tables(setup_test_db: None, test_engine: AsyncEngine) -> None:
    async with AsyncSession(test_engine) as session:
        await session.execute(
            text(
                "TRUNCATE TABLE outbox_events, idempotency_requests, "
                "tickets, events, users RESTART IDENTITY CASCADE;"
            )
        )
        await session.commit()


@pytest_asyncio.fixture
async def db_connection(
    setup_test_db: None,
    test_engine: AsyncEngine,
) -> AsyncGenerator[AsyncSession, None]:
    async with AsyncSession(test_engine, expire_on_commit=False) as session:
        yield session


@pytest_asyncio.fixture
async def setup_redis_for_tests(
    test_resources: TestResources,
) -> AsyncGenerator[None, None]:
    await verify_redis_owner(test_resources)
    await init_redis_pool()
    try:
        yield
    finally:
        await close_redis_pool()


@pytest_asyncio.fixture
async def flush_redis_between_tests(
    setup_redis_for_tests: None,
    test_resources: TestResources,
) -> None:
    await verify_redis_owner(test_resources)
    async with Redis.from_url(test_resources.redis_url) as redis_client:
        await redis_client.flushdb()


@pytest.fixture
def idempotency_header() -> dict[str, str]:
    return {"Idempotency-Key": str(uuid.uuid4())}


@pytest.fixture
async def client(db_connection: AsyncSession) -> AsyncGenerator[AsyncClient, None]:
    async def override_get_db() -> AsyncGenerator[AsyncSession, None]:
        yield db_connection

    mock_arq_pool = AsyncMock()

    async def override_get_arq_pool() -> AsyncMock:
        return mock_arq_pool

    app.dependency_overrides[get_db] = override_get_db
    app.dependency_overrides[get_arq_pool] = override_get_arq_pool

    try:
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="https://test"
        ) as c:
            yield c
    finally:
        app.dependency_overrides.pop(get_db, None)
        app.dependency_overrides.pop(get_arq_pool, None)
