from collections.abc import Sequence

from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.core import logger
from src.core.exception import EventDeleteError, EventNotFoundError
from src.repositories.event import CachedEventRepository, EventRepository
from src.schemas import EventCreate, EventDetailResponse, EventResponse, EventUpdate
from src.services.outbox import (
    EVENT_CACHE_INVALIDATE,
    OutboxDispatcher,
    enqueue_outbox,
    event_cache_handler,
)
from src.services.transactions import commit_business


class EventService:
    """活动业务层，协调数据库访问、事务提交和缓存更新。"""

    def __init__(
        self,
        session: AsyncSession,
        base_repo: EventRepository,
        cached_repo: CachedEventRepository,
    ) -> None:
        self.db = session
        self.base_repo = base_repo
        self.cached_repo = cached_repo
        self.outbox = OutboxDispatcher(
            async_sessionmaker(session.bind, expire_on_commit=False),
            {EVENT_CACHE_INVALIDATE: event_cache_handler(cached_repo.store)},
        )

    async def create(self, event_data: EventCreate) -> EventResponse:
        """创建活动、提交事务并清理相关缓存，返回活动响应模型。"""
        try:
            new_event = await self.base_repo.create(event_data)
            pending = await enqueue_outbox(
                self.db, EVENT_CACHE_INVALIDATE, new_event.id
            )
            pending_id = pending.id
            response = EventResponse.model_validate(new_event, from_attributes=True)
            await commit_business(self.db, response)
        except SQLAlchemyError:
            await self.db.rollback()
            raise

        await self.outbox.dispatch_best_effort(pending_id)
        logger.info(f"活动创建成功，活动 ID：{response.id}")
        return response

    async def get(self, event_id: int) -> EventDetailResponse:
        """通过缓存仓库按活动 ID 查询活动详情。"""
        return await self.cached_repo.get(event_id)

    async def get_all(self, offset: int, limit: int) -> Sequence[EventResponse]:
        """通过缓存仓库按 offset 和 limit 查询活动分页列表。"""
        return await self.cached_repo.get_all(offset=offset, limit=limit)

    async def delete(self, event_id: int) -> None:
        """删除活动并提交事务，然后清理相关缓存；有关联数据时拒绝删除。"""
        event = await self.base_repo.get(event_id)

        if not event:
            raise EventNotFoundError("活动不存在")

        try:
            await self.base_repo.delete(event)
            pending = await enqueue_outbox(self.db, EVENT_CACHE_INVALIDATE, event_id)
            pending_id = pending.id
            await commit_business(self.db)
        except IntegrityError as e:
            await self.db.rollback()

            logger.warning(f"活动 {event_id} 删除失败：已存在关联票据")
            raise EventDeleteError("活动存在关联票种或票据，无法删除") from e
        except SQLAlchemyError:
            await self.db.rollback()
            raise

        await self.outbox.dispatch_best_effort(pending_id)
        logger.info(f"活动删除成功，活动 ID：{event_id}")

    async def update(self, event_id: int, update_data: EventUpdate) -> EventResponse:
        """更新指定活动的字段、提交事务，并刷新相关缓存。"""
        event = await self.base_repo.get(event_id)

        if not event:
            raise EventNotFoundError("活动不存在")

        update_dict = update_data.model_dump(exclude_unset=True)
        if not update_dict:
            return EventResponse.model_validate(event)

        try:
            await self.base_repo.update(event, update_dict)
            pending = await enqueue_outbox(self.db, EVENT_CACHE_INVALIDATE, event_id)
            pending_id = pending.id
            response = EventResponse.model_validate(event)
            await commit_business(self.db)
        except SQLAlchemyError:
            await self.db.rollback()
            raise

        await self.outbox.dispatch_best_effort(pending_id)
        logger.info(f"活动更新成功，活动 ID：{event_id}")
        return response
