import asyncio
from time import perf_counter
from typing import Any
from uuid import uuid4

from src.core import logger, settings
from src.core.cache_lock import CacheLock
from src.core.cache_policy import CacheUnavailableError
from src.core.cache_store import CacheOfflineError, EventCacheStore
from src.core.exception import EventNotFoundError
from src.core.metrics import WARMUP_DURATION, WARMUP_ITEMS, WARMUP_RUNS
from src.core.request_context import request_id_context
from src.repositories.event import CachedEventRepository, EventRepository
from src.services.hotspot import HotspotTracker


async def warm_hot_events(ctx: dict[str, Any]) -> None:
    """有限预热热点详情及默认第一页，与在线查询共用缓存锁和版本保护。

    一轮全局批次锁避免重复调度；每项独立会话，释放连接后再处理下一项。
    进程内回源预算与在线 API 分开计算，缓存故障时禁止后台直连回源。
    """
    if not settings.CACHE_WARMUP_ENABLED:
        WARMUP_RUNS.labels("disabled").inc()
        return
    context_token = request_id_context.set(f"warmup:{uuid4().hex}")
    started = perf_counter()
    redis = ctx["cache_redis"]
    circuit = ctx["cache_circuit"]
    store = EventCacheStore(redis, circuit)
    batch = CacheLock(redis, "ticketcore:cache:v2:warmup:batch:lock", 30000)
    acquired = False
    try:
        acquired = await store.call(batch.acquire)
        if not acquired:
            WARMUP_RUNS.labels("duplicate").inc()
            return
        async with asyncio.timeout(settings.CACHE_WARMUP_TIMEOUT_SECONDS):
            report = await HotspotTracker(redis).report()
            event_ids = [
                item.event_id
                for item in report.hot_events[: settings.CACHE_WARMUP_MAX_EVENTS]
            ]
            # None 表示默认第一页；热点 ID 不进入 Prometheus 指标标签。
            for event_id in [None, *event_ids]:
                owner = await store.call(lambda: redis.get(batch.key))
                if isinstance(owner, bytes):
                    owner = owner.decode()
                if owner != batch.token:
                    raise CacheUnavailableError("预热批次租期已失效，停止剩余任务")
                async with ctx["session_maker"]() as session:
                    repo = CachedEventRepository(
                        EventRepository(session),
                        redis,
                        admission=ctx["warmup_admission"],
                        circuit=circuit,
                    )
                    try:
                        if event_id is None:
                            await repo.get_all(
                                0,
                                min(max(settings.DEFAULT_PAGE_LIMIT, 1), 100),
                                source="warmup",
                                refresh_before_seconds=settings.CACHE_WARMUP_REFRESH_SECONDS,
                            )
                        else:
                            await repo.get(
                                event_id,
                                source="warmup",
                                refresh_before_seconds=settings.CACHE_WARMUP_REFRESH_SECONDS,
                            )
                    except EventNotFoundError:
                        WARMUP_ITEMS.labels("not_found").inc()
                    else:
                        # 表示此项处理完成；实际回填数看 cache event=fill 指标。
                        WARMUP_ITEMS.labels("completed").inc()
        WARMUP_RUNS.labels("success").inc()
        logger.info(
            "规则预热完成",
            extra={
                "extra_data": {
                    "event": "cache_warmup_completed",
                    "candidate_count": len(event_ids),
                    "observed_buckets": report.observed_buckets,
                }
            },
        )
    except (CacheOfflineError, CacheUnavailableError):
        WARMUP_RUNS.labels("paused").inc()
        logger.warning("缓存故障或回源预算不足，本轮预热已暂停")
    except TimeoutError:
        WARMUP_RUNS.labels("timeout").inc()
        logger.warning("本轮预热已达到时间预算，停止剩余任务")
    except asyncio.CancelledError:
        WARMUP_RUNS.labels("cancelled").inc()
        raise
    except Exception:
        WARMUP_RUNS.labels("error").inc()
        logger.exception("规则预热执行失败")
        raise
    finally:
        try:
            if acquired:
                try:
                    await store.call(batch.release, force=True)
                except CacheOfflineError:
                    pass
        finally:
            WARMUP_DURATION.observe(perf_counter() - started)
            request_id_context.reset(context_token)
