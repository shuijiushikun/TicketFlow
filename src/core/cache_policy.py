"""活动查询的单进程保护，不依赖 Redis，也不适用于库存和支付决策。

实例应在 API 工作进程内共享，并由同一事件循环调用。多工作进程或多实例的
数据库并发和总限流预算会相加，部署时需按进程数分配。客户端身份由调用方传入
可信连接地址；经代理转发时可能共享同一地址，本模块不读取转发请求头。
"""

import asyncio
import math
from collections import OrderedDict, deque
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass
from time import monotonic


class CacheUnavailableError(Exception):
    """缓存查询降级期间没有可用的数据库回源预算。"""


class QueryRateLimitError(Exception):
    """活动查询超过本地请求预算，附带至少一秒的重试间隔。"""

    def __init__(self, retry_after: int) -> None:
        super().__init__("活动查询过于频繁，请稍后重试")
        self.retry_after = max(1, retry_after)


class DatabaseAdmission:
    """限制活动查询的并发、排队数量、等待时间和执行时间。

    构造参数由应用配置校验。空闲时不持有事件循环对象，因此同一实例可在
    前一个循环已清空查询与等待者后复用；不能跨运行中的事件循环共享实例。
    """

    def __init__(
        self,
        concurrency: int,
        queue_size: int,
        queue_timeout: float,
        query_timeout: float,
    ) -> None:
        self._concurrency = concurrency
        self._queue_size = queue_size
        self._queue_timeout = queue_timeout
        self._query_timeout = query_timeout
        self._active = 0
        self._waiters: deque[asyncio.Future[None]] = deque()

    @asynccontextmanager
    async def slot(self) -> AsyncIterator[None]:
        """获取回源名额，执行超时或取消时归还；普通数据库异常原样抛出。"""
        await self._acquire()
        try:
            deadline = asyncio.timeout(self._query_timeout)
            try:
                async with deadline:
                    yield
            except TimeoutError as exc:
                if not deadline.expired():
                    raise
                raise CacheUnavailableError("数据库查询超时，请稍后重试") from exc
        finally:
            self._release()

    async def _acquire(self) -> None:
        if self._active < self._concurrency:
            self._active += 1
            return
        if len(self._waiters) >= self._queue_size:
            raise CacheUnavailableError("数据库查询繁忙，请稍后重试")

        waiter: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        self._waiters.append(waiter)
        acquired = False
        try:
            async with asyncio.timeout(self._queue_timeout):
                await waiter
            acquired = True
        except TimeoutError as exc:
            raise CacheUnavailableError("等待数据库查询超时，请稍后重试") from exc
        finally:
            with suppress(ValueError):
                self._waiters.remove(waiter)
            # 名额已移交但任务尚未恢复时也可能被取消，此时必须继续转交名额。
            if not acquired and waiter.done() and not waiter.cancelled():
                self._release()

    def _release(self) -> None:
        while self._waiters:
            waiter = self._waiters.popleft()
            if not waiter.done():
                waiter.set_result(None)
                return
        self._active -= 1


@dataclass(slots=True)
class _TokenBucket:
    tokens: float
    updated_at: float


class QueryRateLimiter:
    """用客户端和进程总量两层令牌桶保护查询，状态由有界 LRU 保存。

    调用方只传入可信连接地址，不能直接信任 X-Forwarded-For。被 LRU 淘汰的
    客户端下次获得新桶，总量桶始终保留，因此地址轮换仍受进程总量限制。
    本地限流不提供跨进程统一预算，也不保证代理后不同用户的公平性。
    """

    def __init__(
        self,
        rate: float,
        burst: int,
        global_rate: float,
        global_burst: int,
        max_clients: int,
    ) -> None:
        self._rate = rate
        self._burst = burst
        self._global_rate = global_rate
        self._global_burst = global_burst
        self._max_clients = max_clients
        self._clients: OrderedDict[str, _TokenBucket] = OrderedDict()
        self._global = _TokenBucket(float(global_burst), monotonic())

    def check(self, client: str) -> None:
        """同步检查两个预算；任一不足时不扣减另一桶的令牌。"""
        now = monotonic()
        self._refill(self._global, self._global_rate, self._global_burst, now)
        bucket = self._clients.get(client)
        if bucket is None:
            if len(self._clients) >= self._max_clients:
                self._clients.popitem(last=False)
            bucket = _TokenBucket(float(self._burst), now)
            self._clients[client] = bucket
        else:
            self._clients.move_to_end(client)
            self._refill(bucket, self._rate, self._burst, now)

        retry = max(
            (1.0 - bucket.tokens) / self._rate,
            (1.0 - self._global.tokens) / self._global_rate,
        )
        if retry > 0:
            raise QueryRateLimitError(math.ceil(retry))
        bucket.tokens -= 1.0
        self._global.tokens -= 1.0

    @staticmethod
    def _refill(bucket: _TokenBucket, rate: float, burst: int, now: float) -> None:
        elapsed = max(0.0, now - bucket.updated_at)
        bucket.tokens = min(float(burst), bucket.tokens + elapsed * rate)
        bucket.updated_at = now


class CacheCircuitBreaker:
    """缓存操作失败后短暂跳过，冷却结束后允许调用方再次尝试。"""

    def __init__(self, cooldown: float) -> None:
        self._cooldown = cooldown
        self._retry_at = 0.0

    def available(self) -> bool:
        """返回是否可以访问缓存；冷却结束后允许恢复探测。"""
        return monotonic() >= self._retry_at

    def fail(self) -> None:
        """记录失败，从当前时刻起重新冷却。"""
        self._retry_at = monotonic() + self._cooldown

    def success(self) -> None:
        """记录成功，让后续请求继续访问缓存。"""
        self._retry_at = 0.0
