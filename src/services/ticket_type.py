from __future__ import annotations

from collections.abc import Sequence

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from src.core import logger
from src.core.exception import (
    TicketTypeDeleteError,
    TicketTypeNotFoundError,
    TicketTypeQuantity,
)
from src.core.redis_keys import RedisClient
from src.models import SeckillRequest, Ticket, TicketType
from src.schemas.ticket_type import (
    TicketTypeCreate,
    TicketTypeDetailResponse,
    TicketTypeResponse,
    TicketTypeUpdate,
)
from src.services.inventory import INVENTORY_SYNC, transaction_outbox
from src.services.outbox import EVENT_CACHE_INVALIDATE, enqueue_outbox
from src.services.transactions import commit_business


class TicketTypeService:
    """数据库行锁协调管理员修改与购票，副作用通过事务待办恢复。"""

    def __init__(
        self,
        session: AsyncSession,
        redis: RedisClient,
        *,
        cache_redis: RedisClient | None = None,
    ) -> None:
        self.db = session
        self.redis = redis
        self.outbox = transaction_outbox(session, redis, cache_redis)

    async def _pending(self, ticket_type_id: int, event_id: int) -> list[int]:
        sync = await enqueue_outbox(self.db, INVENTORY_SYNC, ticket_type_id)
        invalidate = await enqueue_outbox(self.db, EVENT_CACHE_INVALIDATE, event_id)
        return [sync.id, invalidate.id]

    async def _deliver(self, pending_ids: list[int]) -> None:
        for pending_id in pending_ids:
            await self.outbox.dispatch_best_effort(pending_id)

    async def _locked(self, ticket_type_id: int) -> TicketType:
        ticket_type = await self.db.scalar(
            select(TicketType)
            .where(TicketType.id == ticket_type_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        if ticket_type is None:
            raise TicketTypeNotFoundError("票种不存在")
        return ticket_type

    async def create(
        self, event_id: int, type_data: TicketTypeCreate
    ) -> TicketTypeResponse:
        """创建票种，库存同步和活动缓存失效与业务同事务登记。"""
        type_dict = type_data.model_dump()

        type_dict["event_id"] = event_id

        new_ticket_type = TicketType(**type_dict)
        self.db.add(new_ticket_type)

        try:
            await self.db.flush()
            pending_ids = await self._pending(new_ticket_type.id, event_id)
            response = TicketTypeResponse.model_validate(new_ticket_type)
            await commit_business(self.db, response)
        except SQLAlchemyError:
            await self.db.rollback()
            raise

        await self._deliver(pending_ids)
        logger.info(f"票种创建成功，票种 ID：{response.id}")
        return response

    async def get(self, ticket_type_id: int) -> TicketTypeDetailResponse:
        """按票种 ID 查询详情及已占用数量；不存在时抛出 TicketTypeNotFoundError。"""
        ticket_type = await self.db.get(TicketType, ticket_type_id)
        if not ticket_type:
            raise TicketTypeNotFoundError("票种不存在")
        return TicketTypeDetailResponse.model_validate(
            ticket_type, from_attributes=True
        )

    async def get_all_for_event(
        self, event_id: int, offset: int, limit: int
    ) -> Sequence[TicketTypeResponse]:
        """按活动 ID 查询票种，通过 offset 和 limit 分页返回。"""
        query = (
            select(TicketType)
            .where(TicketType.event_id == event_id)
            .offset(offset)
            .limit(limit)
        )
        result = await self.db.scalars(query)
        return [
            TicketTypeResponse.model_validate(tt, from_attributes=True)
            for tt in result.all()
        ]

    async def delete(self, ticket_type_id: int) -> None:
        """删除票种并清理缓存；保留票据或抢票申请所引用的票种。"""
        ticket_type = await self._locked(ticket_type_id)

        try:
            event_id = ticket_type.event_id

            # 只读核对关联；避免 ORM 先修改 Ticket 外键，反转取消链路的锁顺序。
            associated = await self.db.scalar(
                select(Ticket.id)
                .where(Ticket.ticket_type_id == ticket_type_id)
                .limit(1)
            )
            if associated is not None:
                raise TicketTypeDeleteError("票种已存在关联票据，无法删除")
            application = await self.db.scalar(
                select(SeckillRequest.id)
                .where(SeckillRequest.ticket_type_id == ticket_type_id)
                .limit(1)
            )
            if application is not None:
                raise TicketTypeDeleteError(
                    "票种已存在抢票申请，无法删除；可关闭秒杀模式"
                )

            await self.db.delete(ticket_type)
            await self.db.flush()
            pending_ids = await self._pending(ticket_type_id, event_id)
            await commit_business(self.db)
        except IntegrityError as e:
            await self.db.rollback()
            logger.warning(f"票种 {ticket_type_id} 已存在关联票据或抢票申请，无法删除")
            raise TicketTypeDeleteError("票种已存在关联票据或抢票申请，无法删除") from e
        except SQLAlchemyError:
            await self.db.rollback()
            raise

        await self._deliver(pending_ids)
        logger.info(f"票种删除成功，票种 ID：{ticket_type_id}")

    async def update(
        self, ticket_type_id: int, update_data: TicketTypeUpdate
    ) -> TicketTypeResponse:
        """更新票种指定字段，并在总票数变化时同步 Redis 剩余库存。

        update_data 只包含本次需要修改的字段。总票数不能小于预留与已售出的占用数量，
        否则抛出 TicketTypeQuantity；数据库异常时回滚。
        """
        ticket_type = await self._locked(ticket_type_id)

        update_dict = update_data.model_dump(exclude_unset=True, exclude_none=True)
        if not update_dict:
            return TicketTypeResponse.model_validate(ticket_type, from_attributes=True)

        new_quantity = update_dict.get("tickets_quantity")
        if new_quantity is not None and new_quantity < ticket_type.tickets_sold:
            raise TicketTypeQuantity(
                f"总票数 tickets_quantity（{new_quantity}）"
                f"不能小于已占用数量 tickets_sold（{ticket_type.tickets_sold}）"
            )

        for key, value in update_dict.items():
            setattr(ticket_type, key, value)
        if new_quantity is not None:
            ticket_type.inventory_version += 1

        try:
            pending_ids = await self._pending(ticket_type_id, ticket_type.event_id)
            response = TicketTypeResponse.model_validate(ticket_type)
            await commit_business(self.db, response)
        except SQLAlchemyError:
            await self.db.rollback()
            raise

        await self._deliver(pending_ids)
        logger.info(f"票种更新成功，票种 ID：{ticket_type_id}")
        return response
