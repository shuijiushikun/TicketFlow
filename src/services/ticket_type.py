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
from src.core.redis_keys import RedisClient, RedisKeys
from src.models import TicketType
from src.schemas.ticket_type import (
    TicketTypeCreate,
    TicketTypeDetailResponse,
    TicketTypeResponse,
    TicketTypeUpdate,
)


class TicketTypeService:
    """票种业务层，处理定价、容量限制和 Redis 库存初始化及更新。"""

    def __init__(self, session: AsyncSession, redis: RedisClient) -> None:
        self.db = session
        self.redis = redis

    async def create(
        self, event_id: int, type_data: TicketTypeCreate
    ) -> TicketTypeResponse:
        """为 event_id 对应活动创建票种，并按总票数初始化 Redis 可用库存。

        type_data 提供票种参数；提交后清理活动详情和列表缓存，返回票种响应。
        """
        type_dict = type_data.model_dump()

        type_dict["event_id"] = event_id

        new_ticket_type = TicketType(**type_dict)
        self.db.add(new_ticket_type)

        try:
            await self.db.commit()
            await self.db.refresh(new_ticket_type)

            inventory_key = RedisKeys.ticket_type_inventory(new_ticket_type.id)
            await self.redis.set(inventory_key, new_ticket_type.tickets_quantity)

            await self.redis.delete(RedisKeys.event_static(event_id))
            await RedisKeys.bump_event_list_version(self.redis)

            logger.info(f"票种创建成功，票种 ID：{new_ticket_type.id}")
        except SQLAlchemyError:
            await self.db.rollback()
            raise

        return TicketTypeResponse.model_validate(new_ticket_type, from_attributes=True)

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
        """删除票种并清理库存及活动缓存；已有关联票据时拒绝删除。"""
        ticket_type = await self.db.get(TicketType, ticket_type_id)

        if not ticket_type:
            raise TicketTypeNotFoundError("票种不存在")

        try:
            event_id = ticket_type.event_id

            await self.db.delete(ticket_type)
            await self.db.commit()

            await self.redis.delete(RedisKeys.ticket_type_inventory(ticket_type_id))
            await self.redis.delete(RedisKeys.event_static(event_id))
            await RedisKeys.bump_event_list_version(self.redis)

            logger.info(f"票种删除成功，票种 ID：{ticket_type_id}")
        except IntegrityError as e:
            await self.db.rollback()
            logger.warning(f"票种 {ticket_type_id} 已存在关联票据，无法删除")
            raise TicketTypeDeleteError("票种已存在关联票据，无法删除") from e
        except SQLAlchemyError:
            await self.db.rollback()
            raise

    async def update(
        self, ticket_type_id: int, update_data: TicketTypeUpdate
    ) -> TicketTypeResponse:
        """更新票种指定字段，并在总票数变化时同步 Redis 剩余库存。

        update_data 只包含本次需要修改的字段。总票数不能小于预留与已售出的占用数量，
        否则抛出 TicketTypeQuantity；数据库异常时回滚。
        """
        ticket_type = await self.db.get(TicketType, ticket_type_id)

        if not ticket_type:
            raise TicketTypeNotFoundError("票种不存在")

        update_dict = update_data.model_dump(exclude_unset=True)
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

        try:
            await self.db.commit()
            await self.db.refresh(ticket_type)

            if new_quantity is not None:
                inventory_key = RedisKeys.ticket_type_inventory(ticket_type.id)
                available_tickets = (
                    ticket_type.tickets_quantity - ticket_type.tickets_sold
                )
                await self.redis.set(inventory_key, available_tickets)

            await self.redis.delete(RedisKeys.event_static(ticket_type.event_id))
            await RedisKeys.bump_event_list_version(self.redis)

            logger.info(f"票种更新成功，票种 ID：{ticket_type_id}")
        except SQLAlchemyError:
            await self.db.rollback()
            raise

        return TicketTypeResponse.model_validate(ticket_type, from_attributes=True)
