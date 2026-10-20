from __future__ import annotations

import asyncio
import random
from collections.abc import Awaitable, Callable, Sequence
from typing import Any, Literal

from asyncpg import InterfaceError, PostgresError
from pydantic import TypeAdapter
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from src.core import logger
from src.core.cache_lock import CacheLock
from src.core.cache_policy import (
    CacheCircuitBreaker,
    CacheUnavailableError,
    DatabaseAdmission,
)
from src.core.cache_runtime import cache_admission, cache_circuit
from src.core.cache_store import CacheOfflineError, EventCacheStore
from src.core.config import settings
from src.core.exception import EventDeleteError, EventNotFoundError
from src.core.metrics import CACHE_EVENTS
from src.core.redis_keys import RedisClient, RedisKeys
from src.models import Event
from src.schemas import EventCreate, EventDetailResponse, EventResponse


class EventRepository:
    """活动数据库访问层，使用 SQLAlchemy 执行增删改查，不负责 Redis 缓存。"""

    def __init__(self, session: AsyncSession) -> None:
        self.db = session

    async def create(self, event_data: EventCreate) -> Event:
        """将 event_data 转为活动模型，写入并刷新会话，返回新活动；事务由调用层提交。"""
        new_event = Event(**event_data.model_dump())

        self.db.add(new_event)
        await self.db.flush()
        await self.db.refresh(new_event)

        return new_event

    async def get(self, event_id: int) -> Event | None:
        """按活动 ID 查询活动，并预先加载票种关系；不存在时返回 None。"""
        query = (
            select(Event)
            .options(selectinload(Event.ticket_types))
            .where(Event.id == event_id)
        )
        return await self.db.scalar(query)  # type: ignore[no-any-return]

    async def get_all(self, offset: int, limit: int) -> Sequence[Event]:
        """按 offset 和 limit 从数据库分页查询活动列表。"""
        query = select(Event).order_by(Event.id).offset(offset).limit(limit)
        result = await self.db.scalars(query)
        return result.all()

    async def delete(self, event: Event) -> None:
        """删除并刷新活动记录，由调用层处理事务。

        存在关联约束时抛出 EventDeleteError。
        """
        try:
            await self.db.delete(event)
            await self.db.flush()
        except IntegrityError as e:
            raise EventDeleteError("活动存在关联票种或票据，无法删除") from e

    async def update(self, event: Event, update_dict: dict[str, Any]) -> Event:
        """将 update_dict 中提供的字段写入活动。

        刷新记录及票种关系后，返回更新后的模型。
        """
        for key, value in update_dict.items():
            setattr(event, key, value)
        await self.db.flush()
        await self.db.refresh(event, attribute_names=["ticket_types"])
        return event


class CachedEventRepository:
    """活动查询缓存：空值、有限互斥重建、版本保护和本地回源预算。"""

    NOT_FOUND = "__NOT_FOUND__"

    def __init__(
        self,
        repository: EventRepository,
        redis: RedisClient,
        *,
        admission: DatabaseAdmission = cache_admission,
        circuit: CacheCircuitBreaker = cache_circuit,
    ) -> None:
        self.repo = repository
        self.redis = redis
        self.store = EventCacheStore(redis, circuit)
        self.admission = admission

    async def get(
        self,
        event_id: int,
        *,
        source: Literal["request", "warmup"] = "request",
        refresh_before_seconds: int = 0,
    ) -> EventDetailResponse:
        """查询详情；仅数据库确认不存在时写短期空值缓存。"""

        async def load() -> str:
            event = await self.repo.get(event_id)
            if event is None:
                return self.NOT_FOUND
            return EventDetailResponse.model_validate(event).model_dump_json()

        body = await self._query(
            RedisKeys.event_static(event_id),
            RedisKeys.event_generation(event_id),
            load,
            settings.CACHE_DETAIL_TTL_SECONDS,
            settings.CACHE_DETAIL_JITTER_SECONDS,
            kind="detail",
            source=source,
            refresh_before_seconds=refresh_before_seconds,
        )
        if body == self.NOT_FOUND:
            raise EventNotFoundError("活动不存在")
        return EventDetailResponse.model_validate_json(body)

    async def get_all(
        self,
        offset: int,
        limit: int,
        *,
        source: Literal["request", "warmup"] = "request",
        refresh_before_seconds: int = 0,
    ) -> Sequence[EventResponse]:
        """查询指定分页；所有分页共享失效版本，各分页单独互斥重建。"""
        adapter = TypeAdapter(list[EventResponse])

        async def load() -> str:
            events = await self.repo.get_all(offset, limit)
            return adapter.dump_json(
                [EventResponse.model_validate(e) for e in events]
            ).decode()

        body = await self._query(
            RedisKeys.event_list(offset, limit),
            RedisKeys.event_list_generation(),
            load,
            settings.CACHE_LIST_TTL_SECONDS,
            settings.CACHE_LIST_JITTER_SECONDS,
            kind="list",
            source=source,
            refresh_before_seconds=refresh_before_seconds,
        )
        return adapter.validate_json(body)

    async def _load(self, load: Callable[[], Awaitable[str]]) -> str:
        async with self.admission.slot():
            try:
                return await load()
            except (SQLAlchemyError, OSError, InterfaceError, PostgresError) as exc:
                logger.error("活动查询数据库操作失败", exc_info=True)
                raise CacheUnavailableError("活动数据暂不可用，请稍后重试") from exc

    async def _query(
        self,
        cache_key: str,
        generation_key: str,
        load: Callable[[], Awaitable[str]],
        base_ttl: int,
        jitter: int,
        *,
        kind: Literal["detail", "list"],
        source: Literal["request", "warmup"],
        refresh_before_seconds: int,
    ) -> str:
        lock = CacheLock(
            self.redis, RedisKeys.cache_lock(cache_key), settings.CACHE_LOCK_LEASE_MS
        )
        acquired = False
        try:
            generation, body = await self.store.read(
                generation_key, cache_key, refresh_before_seconds=refresh_before_seconds
            )
            if body is not None:
                CACHE_EVENTS.labels(
                    kind, source, "negative_hit" if body == self.NOT_FOUND else "hit"
                ).inc()
                return body
            CACHE_EVENTS.labels(kind, source, "miss").inc()
            deadline = (
                asyncio.get_running_loop().time() + settings.CACHE_LOCK_WAIT_SECONDS
            )
            while True:
                # 竞争只等待有限时间；超时返回 503，不绕过锁集中回源。
                try:
                    async with asyncio.timeout_at(deadline):
                        acquired = await self.store.call(lock.acquire)
                        CACHE_EVENTS.labels(
                            kind,
                            source,
                            "lock_acquired" if acquired else "lock_contended",
                        ).inc()
                        if not acquired:
                            await asyncio.sleep(random.uniform(0.015, 0.035))
                        # 获得锁后再次读取，包括版本，防止等待期间发生写入。
                        generation, body = await self.store.read(
                            generation_key,
                            cache_key,
                            refresh_before_seconds=refresh_before_seconds,
                        )
                except TimeoutError as exc:
                    CACHE_EVENTS.labels(kind, source, "lock_timeout").inc()
                    raise CacheUnavailableError("活动缓存正在重建，请稍后重试") from exc
                if body is not None:
                    return body
                if acquired:
                    break

            CACHE_EVENTS.labels(kind, source, "rebuild").inc()
            body = await self._load(load)
            ttl = base_ttl + random.randint(0, jitter)
            if body == self.NOT_FOUND:
                ttl = settings.CACHE_NEGATIVE_TTL_SECONDS + random.randint(
                    0, settings.CACHE_NEGATIVE_JITTER_SECONDS
                )
            try:
                filled = await self.store.fill(
                    generation_key,
                    cache_key,
                    generation,
                    lock.key,
                    lock.token,
                    body,
                    ttl,
                )
                CACHE_EVENTS.labels(
                    kind, source, "fill" if filled else "fill_rejected"
                ).inc()
            except CacheOfflineError:
                # 已获得数据库结果，回填失败不重复查询，也不丢失成功响应。
                pass
            return body
        except CacheOfflineError:
            if source == "warmup":
                CACHE_EVENTS.labels(kind, source, "warmup_paused").inc()
                raise CacheUnavailableError("缓存暂不可用，暂停预热") from None
            CACHE_EVENTS.labels(kind, source, "fallback").inc()
            return await self._load(load)
        finally:
            if acquired:
                try:
                    await self.store.call(lock.release)
                except CacheOfflineError:
                    # 释放失败由有限租期兜底，禁止无条件 DEL。
                    pass

    async def invalidate_event(self, event_id: int) -> None:
        """数据库提交后切换版本；可靠失效补偿由后续交易可靠性阶段完成。"""
        await self.store.invalidate(event_id)
