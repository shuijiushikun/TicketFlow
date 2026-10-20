from __future__ import annotations

from typing import cast

from arq import ArqRedis
from arq.connections import RedisSettings, create_pool
from redis.asyncio import Connection, ConnectionPool, Redis

from src.core import settings
from src.core.redis_keys import RedisClient

RedisPool = ConnectionPool
redis_pool: RedisPool[Connection] | None = None
cache_pool: RedisPool[Connection] | None = None
arq_pool: ArqRedis | None = None


async def init_redis_pool() -> None:
    """应用启动时初始化 Redis 连接池。

    业务连接使用 0 号库，保存缓存和库存；ARQ 连接使用 1 号库，保存后台任务队列。
    """
    global redis_pool, cache_pool, arq_pool

    redis_pool = ConnectionPool.from_url(
        settings.REDIS_URL,
        decode_responses=True,
        max_connections=1000,
        socket_connect_timeout=settings.TRANSACTION_REDIS_TIMEOUT_SECONDS,
        socket_timeout=settings.TRANSACTION_REDIS_TIMEOUT_SECONDS,
        retry_on_timeout=False,
    )
    # 与库存共用 Redis 实例及 0 号库，但查询单独限制连接和命令时间。
    cache_pool = ConnectionPool.from_url(
        settings.REDIS_URL,
        decode_responses=True,
        max_connections=100,
        socket_connect_timeout=settings.CACHE_REDIS_TIMEOUT_SECONDS,
        socket_timeout=settings.CACHE_REDIS_TIMEOUT_SECONDS,
        retry_on_timeout=False,
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
    global redis_pool, cache_pool, arq_pool

    if redis_pool:
        await redis_pool.disconnect()
        redis_pool = None

    if cache_pool:
        await cache_pool.disconnect()
        cache_pool = None

    if arq_pool:
        await arq_pool.aclose()  # type: ignore[attr-defined]
        arq_pool = None


async def get_redis() -> RedisClient:
    """为接口依赖提供业务 Redis 客户端。"""
    if not redis_pool:
        raise RuntimeError("业务 Redis 连接池尚未初始化")
    return cast(RedisClient, Redis(connection_pool=redis_pool))


async def get_arq_pool() -> ArqRedis:
    """为接口依赖提供 ARQ 后台任务队列连接。"""
    if not arq_pool:
        raise RuntimeError("ARQ 任务队列连接尚未初始化")
    return arq_pool


async def get_cache_redis() -> RedisClient:
    """活动查询使用有超时的连接池；不改变库存和队列客户端的策略。"""
    if not cache_pool:
        raise RuntimeError("活动查询 Redis 连接池尚未初始化")
    return cast(RedisClient, Redis(connection_pool=cache_pool))
