import asyncio
from collections.abc import AsyncGenerator

from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from src.core.config import settings

connect_args = {
    "server_settings": {
        "jit": "off",
    }
}

engine: AsyncEngine = create_async_engine(
    str(settings.DATABASE_URL),
    # 保持单进程最多 20 个连接，并复用峰值连接，避免突发流量反复建连。
    pool_size=20,
    max_overflow=0,
    pool_timeout=10.0,
    pool_pre_ping=True,
    echo=settings.ENVIRONMENT == "dev",
    connect_args=connect_args,
)

AsyncSessionLocal: async_sessionmaker[AsyncSession] = async_sessionmaker(
    bind=engine,
    expire_on_commit=False,
    autoflush=False,
)


async def warmup_connections(count: int) -> None:
    """并行准备指定数量的池内连接；首次秒杀不再同时承担建连成本。

    所有连接通过屏障前均不归还，避免循环反复借到同一条连接；仍受池上限约束。
    仅由开启秒杀的 API 在启动时调用；失败或超过五秒则取消其他准备任务。
    """
    barrier = asyncio.Barrier(count)

    async def prepare() -> None:
        async with engine.connect() as connection:
            await connection.execute(text("SELECT 1"))
            await barrier.wait()

    async with asyncio.timeout(5):
        async with asyncio.TaskGroup() as group:
            for _ in range(count):
                group.create_task(prepare())


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """为每次接口请求提供独立的异步数据库会话，并在请求结束后关闭。"""
    async with AsyncSessionLocal() as session:
        try:
            yield session
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()
