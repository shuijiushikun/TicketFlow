from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable
from datetime import UTC, datetime
from typing import Any, Literal, cast

from redis.exceptions import RedisError

from src.core import logger, settings
from src.core.cache_policy import CacheCircuitBreaker, CacheUnavailableError
from src.core.metrics import HOTSPOT_CANDIDATES, HOTSPOT_RECORDS
from src.core.redis_keys import RedisClient
from src.schemas.hotspot import HotEvent, HotspotReport

record_circuit = CacheCircuitBreaker(settings.CACHE_CIRCUIT_COOLDOWN_SECONDS)


class HotspotTracker:
    """有界分钟访问桶与规则热点；记录失败不能拖垮活动查询。

    只接收成功的公开详情访问。先取最近两个完整分钟的有限候选，再精确读取
    候选在四个桶中的分数，避免把此前未入 Top K 的活动误当成零访问。
    """

    RECORD_SCRIPT = """
        if not redis.call('ZSCORE', KEYS[1], ARGV[1])
            and redis.call('ZCARD', KEYS[1]) >= tonumber(ARGV[2]) then
            return 0
        end
        redis.call('ZINCRBY', KEYS[1], 1, ARGV[1])
        redis.call('EXPIRE', KEYS[1], ARGV[3])
        return 1
    """

    def __init__(
        self, redis: RedisClient, *, circuit: CacheCircuitBreaker = record_circuit
    ) -> None:
        self.redis = redis
        self.circuit = circuit

    @staticmethod
    def bucket_key(minute: int) -> str:
        """以 UTC Unix 分钟编号生成访问桶键，无逐请求永久元数据。"""
        return f"ticketflow:analytics:v1:detail_views:{minute}"

    async def record(self, event_id: int, *, now: float | None = None) -> bool:
        """追加一次已完成访问，最多等待配置时长，故障时直接跳过。"""
        if not self.circuit.available():
            HOTSPOT_RECORDS.labels("skipped").inc()
            return False
        minute = int(time.time() if now is None else now) // 60
        try:
            async with asyncio.timeout(settings.HOTSPOT_RECORD_TIMEOUT_SECONDS):
                accepted = await cast(
                    Awaitable[Any],
                    self.redis.eval(  # type: ignore[no-untyped-call]
                        self.RECORD_SCRIPT,
                        1,
                        self.bucket_key(minute),
                        str(event_id),
                        settings.HOTSPOT_MAX_EVENTS_PER_MINUTE,
                        settings.HOTSPOT_BUCKET_TTL_SECONDS,
                    ),
                )
        except (RedisError, TimeoutError):
            self.circuit.fail()
            HOTSPOT_RECORDS.labels("error").inc()
            logger.warning("活动访问统计暂不可用，已跳过记录")
            return False
        self.circuit.success()
        HOTSPOT_RECORDS.labels("accepted" if accepted else "full").inc()
        return bool(accepted)

    async def report(self, *, now: float | None = None) -> HotspotReport:
        """识别访问量高或增长快的有限候选；缺失桶不解释为完整零流量。"""
        timestamp = time.time() if now is None else now
        minute = int(timestamp) // 60
        keys = [self.bucket_key(minute - i) for i in range(1, 5)]
        try:
            async with asyncio.timeout(settings.CACHE_REDIS_TIMEOUT_SECONDS):
                async with self.redis.pipeline(transaction=False) as pipe:
                    for key in keys[:2]:
                        pipe.zrevrange(
                            key, 0, settings.HOTSPOT_CANDIDATES_PER_BUCKET - 1
                        )
                    candidates_raw = await pipe.execute()
                candidates = sorted(
                    {int(value) for row in candidates_raw for value in row}
                )
                async with self.redis.pipeline(transaction=False) as pipe:
                    for key in keys:
                        pipe.exists(key)
                        if candidates:
                            pipe.zmscore(key, candidates)
                    rows = await pipe.execute()
        except (RedisError, TimeoutError) as exc:
            raise CacheUnavailableError("热点统计暂不可用，请稍后重试") from exc

        stride = 2 if candidates else 1
        present = [bool(rows[i * stride]) for i in range(4)]
        comparison_available = all(present)
        counts = [rows[i * stride + 1] for i in range(4)] if candidates else []
        hot_events: list[HotEvent] = []
        for index, event_id in enumerate(candidates):
            recent = sum(int(counts[i][index] or 0) for i in range(2))
            previous = sum(int(counts[i][index] or 0) for i in range(2, 4))
            growth = recent / max(previous, 1) if comparison_available else None
            reason: Literal["volume", "growth"]
            if recent >= settings.HOTSPOT_MIN_VIEWS:
                reason = "volume"
            elif (
                growth is not None
                and recent >= settings.HOTSPOT_GROWTH_MIN_VIEWS
                and growth >= settings.HOTSPOT_GROWTH_RATIO
            ):
                reason = "growth"
            else:
                continue
            hot_events.append(
                HotEvent(
                    event_id=event_id,
                    recent_views=recent,
                    previous_views=previous if comparison_available else None,
                    growth_ratio=round(growth, 3) if growth is not None else None,
                    reason=reason,
                )
            )
        hot_events.sort(key=lambda item: (-item.recent_views, item.event_id))
        hot_events = hot_events[: settings.HOTSPOT_TOP_K]
        HOTSPOT_CANDIDATES.set(len(hot_events))
        return HotspotReport(
            generated_at=datetime.fromtimestamp(timestamp, UTC),
            window_start=datetime.fromtimestamp((minute - 2) * 60, UTC),
            window_end=datetime.fromtimestamp(minute * 60, UTC),
            observed_buckets=sum(present),
            comparison_available=comparison_available,
            hot_events=hot_events,
        )
