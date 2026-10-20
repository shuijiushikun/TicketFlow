from __future__ import annotations

from arq import ArqRedis
from arq.connections import RedisSettings, create_pool
from redis.asyncio import ConnectionPool, Redis

from src.core import settings

RedisPool = ConnectionPool
RedisClient = Redis

redis_pool: RedisPool | None = None
arq_pool: ArqRedis | None = None


async def init_redis_pool() -> None:
    """应用启动时初始化 Redis 连接池。

    业务连接使用 0 号库，保存缓存和库存；ARQ 连接使用 1 号库，保存后台任务队列。
    """
    global redis_pool, arq_pool

    redis_pool = ConnectionPool.from_url(
        settings.REDIS_URL,
        decode_responses=True,
        max_connections=1000,
    )

    arq_pool = await create_pool(
        RedisSettings(
            host=settings.REDIS_HOST,
            port=settings.REDIS_PORT,
            database=1,
        )
    )


async def close_redis_pool() -> None:
    """应用关闭时释放业务 Redis 连接池和 ARQ 队列连接。"""
    global redis_pool

    if redis_pool:
        await redis_pool.disconnect()

    if arq_pool:
        await arq_pool.aclose()


async def get_redis() -> RedisClient:
    """为接口依赖提供业务 Redis 客户端。"""
    if not redis_pool:
        raise RuntimeError("业务 Redis 连接池尚未初始化")
    return Redis(connection_pool=redis_pool)


async def get_arq_pool() -> ArqRedis:
    """为接口依赖提供 ARQ 后台任务队列连接。"""
    if not arq_pool:
        raise RuntimeError("ARQ 任务队列连接尚未初始化")
    return arq_pool
