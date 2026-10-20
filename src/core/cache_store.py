from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any, TypeVar, cast
from uuid import uuid4

from redis.exceptions import RedisError

from src.core import logger
from src.core.cache_policy import CacheCircuitBreaker, CacheUnavailableError
from src.core.config import settings
from src.core.redis_keys import RedisClient, RedisKeys

T = TypeVar("T")


class CacheOfflineError(Exception):
    """缓存命令失败或处于短暂熔断中，查询可进入本地回源准入。"""


class EventCacheStore:
    """原子读取版本和正文，回填时同时验证版本与重建锁持有者。

    哈希正文与版本键使用 v2 命名空间，避免把旧版 JSON 当成哈希读取。
    随机版本防止版本键过期或被淘汰后重新初始化造成 ABA。
    """

    READ_SCRIPT = """
        local generation = redis.call('GET', KEYS[1])
        if not generation then
            generation = ARGV[1]
            redis.call('SET', KEYS[1], generation, 'EX', ARGV[2])
        end
        if redis.call('HGET', KEYS[2], 'generation') == generation then
            return {generation, redis.call('HGET', KEYS[2], 'body')}
        end
        return {generation, false}
    """
    FILL_SCRIPT = """
        if redis.call('GET', KEYS[1]) ~= ARGV[1]
            or redis.call('GET', KEYS[3]) ~= ARGV[2] then
            return 0
        end
        redis.call('HSET', KEYS[2], 'generation', ARGV[1], 'body', ARGV[3])
        redis.call('EXPIRE', KEYS[2], ARGV[4])
        -- 版本键不得先于正文过期；不缩短其他分页已经延长的保留期。
        if redis.call('PTTL', KEYS[1]) < tonumber(ARGV[5]) then
            redis.call('PEXPIRE', KEYS[1], ARGV[5])
        end
        return 1
    """
    INVALIDATE_SCRIPT = """
        redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[3])
        redis.call('DEL', KEYS[2])
        redis.call('SET', KEYS[3], ARGV[2], 'EX', ARGV[4])
        return 1
    """

    def __init__(self, redis: RedisClient, circuit: CacheCircuitBreaker) -> None:
        self.redis = redis
        self.circuit = circuit

    async def call(
        self, operation: Callable[[], Awaitable[T]], *, force: bool = False
    ) -> T:
        """限制单次缓存命令时间；写后失效必须尝试，不能被读熔断跳过。"""
        if not force and not self.circuit.available():
            raise CacheOfflineError
        try:
            async with asyncio.timeout(settings.CACHE_REDIS_TIMEOUT_SECONDS):
                result = await operation()
        except (RedisError, TimeoutError) as exc:
            self.circuit.fail()
            logger.warning("活动查询缓存暂不可用，启用有界回源")
            raise CacheOfflineError from exc
        self.circuit.success()
        return result

    async def _eval(self, script: str, keys: list[str], *args: str | int) -> Any:
        return await cast(
            Awaitable[Any],
            self.redis.eval(script, len(keys), *keys, *args),  # type: ignore[no-untyped-call]
        )

    async def read(self, generation_key: str, cache_key: str) -> tuple[str, str | None]:
        """原子初始化随机版本并读取当前版本正文；空版本只短暂保留。"""
        bootstrap_ttl = (
            settings.CACHE_NEGATIVE_TTL_SECONDS
            + settings.CACHE_NEGATIVE_JITTER_SECONDS
            + (settings.CACHE_LOCK_LEASE_MS + 999) // 1000
        )
        result = await self.call(
            lambda: self._eval(
                self.READ_SCRIPT,
                [generation_key, cache_key],
                uuid4().hex,
                bootstrap_ttl,
            )
        )
        generation, body = result
        if isinstance(generation, bytes):
            generation = generation.decode()
        if isinstance(body, bytes):
            body = body.decode()
        return str(generation), body

    async def fill(
        self,
        generation_key: str,
        cache_key: str,
        generation: str,
        lock_key: str,
        token: str,
        body: str,
        ttl: int,
    ) -> bool:
        """只有版本和锁持有者均未改变，才允许回填并延长版本保留期。"""
        return bool(
            await self.call(
                lambda: self._eval(
                    self.FILL_SCRIPT,
                    [generation_key, cache_key, lock_key],
                    generation,
                    token,
                    body,
                    ttl,
                    ttl * 1000 + settings.CACHE_LOCK_LEASE_MS,
                )
            )
        )

    async def invalidate(self, event_id: int) -> None:
        """提交后原子切换详情及列表版本，阻止旧查询和旧空值晚回填。"""
        detail_retention = (
            settings.CACHE_DETAIL_TTL_SECONDS
            + settings.CACHE_DETAIL_JITTER_SECONDS
            + (settings.CACHE_LOCK_LEASE_MS + 999) // 1000
        )
        list_retention = (
            settings.CACHE_LIST_TTL_SECONDS
            + settings.CACHE_LIST_JITTER_SECONDS
            + (settings.CACHE_LOCK_LEASE_MS + 999) // 1000
        )
        try:
            await self.call(
                lambda: self._eval(
                    self.INVALIDATE_SCRIPT,
                    [
                        RedisKeys.event_generation(event_id),
                        RedisKeys.event_static(event_id),
                        RedisKeys.event_list_generation(),
                    ],
                    uuid4().hex,
                    uuid4().hex,
                    detail_retention,
                    list_retention,
                ),
                force=True,
            )
        except CacheOfflineError as exc:
            logger.error("数据库已提交，但活动 %s 的缓存失效失败，需补偿", event_id)
            raise CacheUnavailableError(
                "数据已提交，但缓存刷新暂时失败，请稍后查询确认"
            ) from exc
