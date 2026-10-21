from collections.abc import AsyncGenerator

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
