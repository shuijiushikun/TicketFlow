"""秒杀入口的有限排队名额与独立进程预算。"""

import asyncio
from collections.abc import AsyncIterator, Awaitable, Iterator
from contextlib import asynccontextmanager, contextmanager
from datetime import UTC, datetime
from time import monotonic
from typing import Any, Literal, cast

from redis.exceptions import RedisError

from src.core.cache_policy import CacheUnavailableError, QueryRateLimiter
from src.core.config import settings
from src.core.logger import logger
from src.core.metrics import (
    SECKILL_ENTRY_ACTIVE,
    SECKILL_ENTRY_RESULTS,
    SECKILL_ENTRY_WAIT_SECONDS,
    SECKILL_ENTRY_WAITING,
    SECKILL_SUBMIT_SECONDS,
)
from src.core.redis_keys import RedisClient

ADMISSIONS_KEY = "ticketflow:seckill:admissions"
SUBMIT_LIMITER = QueryRateLimiter(20, 40, 100, 200, 10000)
POLL_LIMITER = QueryRateLimiter(10, 20, 200, 400, 10000)


class SubmissionAdmission:
    """在查库前限制秒杀接收并发与有限等待；仅供单进程同一事件循环使用。

    等待者尚未受理，没有排队编号或库存承诺；真正的排队申请仍持久化到数据库。
    入口名额覆盖身份查询、幂等检查、提交和快速入队，异常与取消均归还。
    """

    def __init__(self, concurrency: int, queue_size: int, wait_timeout: float) -> None:
        self._slots = asyncio.BoundedSemaphore(concurrency)
        self._capacity = concurrency + queue_size
        self._inside = 0
        self._wait_timeout = wait_timeout

    @asynccontextmanager
    async def slot(self) -> AsyncIterator[None]:
        """缓冲满时立即拒绝，等待限时；不取消已经开始的业务提交。"""
        if self._inside >= self._capacity:
            SECKILL_ENTRY_RESULTS.labels("full").inc()
            raise CacheUnavailableError("秒杀申请入口繁忙，请稍后重试")
        self._inside += 1
        acquired = False
        waiting = self._slots.locked()
        started = monotonic()
        if waiting:
            SECKILL_ENTRY_WAITING.inc()
        try:
            try:
                async with asyncio.timeout(self._wait_timeout):
                    await self._slots.acquire()
                acquired = True
            except TimeoutError as error:
                SECKILL_ENTRY_RESULTS.labels("wait_timeout").inc()
                raise CacheUnavailableError(
                    "秒杀申请入口等待超时，请稍后重试"
                ) from error
            except asyncio.CancelledError:
                SECKILL_ENTRY_RESULTS.labels("cancelled").inc()
                raise
            finally:
                if waiting:
                    SECKILL_ENTRY_WAITING.dec()
                SECKILL_ENTRY_WAIT_SECONDS.observe(monotonic() - started)
            SECKILL_ENTRY_RESULTS.labels("accepted").inc()
            SECKILL_ENTRY_ACTIVE.inc()
            try:
                yield
            finally:
                SECKILL_ENTRY_ACTIVE.dec()
        finally:
            self._inside -= 1
            if acquired:
                self._slots.release()


SUBMIT_ADMISSION = SubmissionAdmission(
    settings.SECKILL_SUBMIT_CONCURRENCY,
    settings.SECKILL_SUBMIT_QUEUE_SIZE,
    settings.SECKILL_SUBMIT_QUEUE_TIMEOUT_SECONDS,
)


@contextmanager
def submission_stage(
    stage: Literal[
        "acceptance", "ticket_type", "redis_admission", "persist", "dispatch"
    ],
) -> Iterator[None]:
    """记录固定阶段的耗时和结果，不使用用户 ID 或异常文本作为指标标签。"""
    started, result = monotonic(), "success"
    try:
        yield
    except BaseException as error:
        result = "cancelled" if isinstance(error, asyncio.CancelledError) else "error"
        raise
    finally:
        SECKILL_SUBMIT_SECONDS.labels(stage, result).observe(monotonic() - started)


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
    started = monotonic()
    try:
        with submission_stage("redis_admission"):
            async with asyncio.timeout(settings.TRANSACTION_REDIS_TIMEOUT_SECONDS):
                result = await cast(
                    Awaitable[Any],
                    redis.eval(  # type: ignore[no-untyped-call]
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
        logger.warning(
            "秒杀排队名额获取失败 error=%s elapsed_ms=%.1f",
            type(error).__name__,
            (monotonic() - started) * 1000,
        )
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
