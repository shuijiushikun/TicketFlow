"""秒杀入口的有限排队名额与独立进程预算。"""

import asyncio
from collections.abc import Awaitable
from datetime import UTC, datetime
from typing import Any, cast

from redis.exceptions import RedisError

from src.core.cache_policy import CacheUnavailableError, QueryRateLimiter
from src.core.config import settings
from src.core.logger import logger
from src.core.redis_keys import RedisClient

ADMISSIONS_KEY = "ticketflow:seckill:admissions"
SUBMIT_LIMITER = QueryRateLimiter(20, 40, 100, 200, 10000)
POLL_LIMITER = QueryRateLimiter(10, 20, 200, 400, 10000)

# 使用 Redis 服务器时间分配截止时间，同一个脚本内完成清理、计数和占位。
ACQUIRE_ADMISSION = """
local clock = redis.call('TIME')
local now = clock[1] * 1000 + math.floor(clock[2] / 1000)
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now)
if redis.call('ZCARD', KEYS[1]) >= tonumber(ARGV[1]) then return 0 end
local deadline = now + tonumber(ARGV[2]) * 1000
redis.call('ZADD', KEYS[1], deadline, ARGV[3])
redis.call('EXPIRE', KEYS[1], tonumber(ARGV[2]) + 10)
return deadline
"""


async def acquire_admission(redis: RedisClient, token: str) -> datetime:
    """为未过期申请占一个名额；依赖失败时拒绝新申请。"""
    try:
        async with asyncio.timeout(settings.TRANSACTION_REDIS_TIMEOUT_SECONDS):
            result = await cast(
                Awaitable[Any],
                redis.eval(
                    ACQUIRE_ADMISSION,
                    1,
                    ADMISSIONS_KEY,
                    settings.SECKILL_MAX_PENDING,
                    settings.SECKILL_WAIT_SECONDS,
                    token,
                ),
            )
    except (RedisError, OSError, TimeoutError) as error:
        # 命令可能已经执行；随机 token 的占位会自动到期，不盲目增加名额。
        raise CacheUnavailableError("秒杀排队服务暂不可用，请稍后重试") from error
    if not result:
        raise CacheUnavailableError("秒杀排队人数已满，请稍后重试")
    return datetime.fromtimestamp(int(result) / 1000, UTC)


async def release_admission(redis: RedisClient, token: str) -> None:
    """只清理本申请的随机名额；失败后由截止时间自动清理。"""
    try:
        async with asyncio.timeout(settings.TRANSACTION_REDIS_TIMEOUT_SECONDS):
            await cast(Awaitable[Any], redis.zrem(ADMISSIONS_KEY, token))
    except (RedisError, OSError, TimeoutError):
        logger.warning("秒杀排队名额清理失败，将自动到期 token=%s", token)
