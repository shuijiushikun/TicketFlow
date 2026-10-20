from collections.abc import Sequence
from typing import Any

from pydantic import TypeAdapter
from redis.asyncio import Redis
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from src.core.exception import EventDeleteError, EventNotFoundError
from src.core.redis_keys import RedisKeys
from src.models import Event
from src.schemas import EventCreate, EventDetailResponse, EventResponse

RedisClient = Redis


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
        query = select(Event).offset(offset).limit(limit)
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
    """在活动数据库仓库外包装 Redis 缓存。

    查询时先读缓存，未命中再查询数据库并回写；提供缓存失效方法供写操作调用。
    """

    def __init__(self, repository: EventRepository, redis: RedisClient) -> None:
        self.repo = repository
        self.redis = redis

    async def get(self, event_id: int) -> EventDetailResponse:
        """先读活动详情缓存，未命中时查询数据库，转换为响应模型并缓存 24 小时。

        活动不存在时抛出 EventNotFoundError。
        """
        cached_key = RedisKeys.event_static(event_id)

        cached_data = await self.redis.get(cached_key)
        if cached_data:
            return EventDetailResponse.model_validate_json(cached_data)

        event = await self.repo.get(event_id)
        if not event:
            raise EventNotFoundError("活动不存在")

        event_dto = EventDetailResponse.model_validate(event)

        await self.redis.set(
            cached_key, event_dto.model_dump_json(), ex=RedisKeys.TTL_STATIC_24H
        )

        return event_dto

    async def get_all(self, offset: int, limit: int) -> Sequence[EventResponse]:
        """按版本化缓存键读取活动分页列表，未命中时查询数据库并缓存 1 小时。"""
        cached_key = await RedisKeys.event_list(self.redis, offset, limit)
        cached_data = await self.redis.get(cached_key)

        adapter = TypeAdapter(list[EventResponse])

        if cached_data:
            return adapter.validate_json(cached_data)

        events = await self.repo.get_all(offset, limit)
        events_dto = [EventResponse.model_validate(e) for e in events]

        await self.redis.set(
            cached_key, adapter.dump_json(events_dto), ex=RedisKeys.TTL_LISTS_1H
        )

        return events_dto

    async def invalidate_event(self, event_id: int) -> None:
        """删除指定活动的详情缓存，并提升列表缓存版本号，使旧列表失效。"""
        await self.redis.delete(RedisKeys.event_static(event_id))
        await RedisKeys.bump_event_list_version(self.redis)
