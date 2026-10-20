from __future__ import annotations

from collections.abc import Awaitable
from typing import Any, cast
from uuid import uuid4

from src.core.redis_keys import RedisClient


class CacheLock:
    """用唯一持有者标记和有限租期协调缓存重建。

    调用方限制 Redis 操作时间；租期到期后的回填还必须检查版本和持有者。
    """

    RELEASE_SCRIPT = """
        if redis.call('GET', KEYS[1]) == ARGV[1] then
            return redis.call('DEL', KEYS[1])
        end
        return 0
    """

    def __init__(self, redis: RedisClient, key: str, lease_ms: int) -> None:
        self.redis = redis
        self.key = key
        self.lease_ms = lease_ms
        self.token = uuid4().hex

    async def acquire(self) -> bool:
        """尝试获取一次锁，不在锁封装内无限重试。"""
        return bool(
            await self.redis.set(self.key, self.token, nx=True, px=self.lease_ms)
        )

    async def release(self) -> None:
        """原子核对 token，仅删除自己的锁。"""
        # types-redis 的旧版存根没有为 eval 标注参数与异步返回类型。
        await cast(
            Awaitable[Any],
            self.redis.eval(  # type: ignore[no-untyped-call]
                self.RELEASE_SCRIPT, 1, self.key, self.token
            ),
        )
