import time
from collections.abc import Awaitable, Sequence
from typing import cast

from arq import ArqRedis
from redis.asyncio import Redis
from sqlalchemy import select, update
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from src.core import logger, settings
from src.core.exception import (
    TicketAlreadyPaidError,
    TicketNotFoundError,
    TicketsSoldOutError,
    TicketTypeNotFoundError,
)
from src.core.redis_keys import RedisKeys
from src.models import Ticket, TicketType
from src.models.ticket import TicketStatus
from src.schemas import TicketCreate, TicketResponse
from src.schemas.ticket import TicketDetailResponse

RedisClient = Redis


class TicketService:
    """票据预订业务层，协调 Redis 库存扣减、数据库票据记录和后台回收任务。"""

    def __init__(
        self, session: AsyncSession, arq_pool: ArqRedis, redis: RedisClient
    ) -> None:
        self.db = session
        self.arq_pool = arq_pool
        self.redis = redis

    async def create(self, user_id: int, ticket_data: TicketCreate) -> TicketResponse:
        """为 user_id 对应的用户预订 ticket_data 指定票种的一张票。

        先通过 Lua 原子扣减 Redis 可用库存；库存键缺失时从数据库读取剩余数量并初始化。
        随后增加数据库占用数量、创建 RESERVED 票据并提交事务，最后安排延迟回收任务。
        数据库写入失败时回滚并补偿库存；提交成功后的刷新或入队失败不能释放已存在的票据库存。
        票种不存在时抛出 TicketTypeNotFoundError，库存不足时抛出 TicketsSoldOutError。
        """
        inventory_key = RedisKeys.ticket_type_inventory(ticket_data.ticket_type_id)

        reserve_script = """
        local current = redis.call('GET', KEYS[1])
        if current == false then
            return -1
        end
        if tonumber(current) > 0 then
            redis.call('DECR', KEYS[1])
            return 1
        else
            return 0
        end
        """

        result = await self.redis.eval(reserve_script, 1, inventory_key)  # type: ignore[misc]

        if result == -1:
            ticket_type = await self.db.get(TicketType, ticket_data.ticket_type_id)
            if not ticket_type:
                raise TicketTypeNotFoundError("票种不存在")

            available_tickets = ticket_type.tickets_quantity - ticket_type.tickets_sold
            await self.redis.set(inventory_key, available_tickets)

            result = await self.redis.eval(reserve_script, 1, inventory_key)  # type: ignore[misc]

        if result == 0:
            raise TicketsSoldOutError("该票种已售罄，暂无可用库存")

        reservation_id: int | None = None
        try:
            update_query = (
                update(TicketType)
                .where(TicketType.id == ticket_data.ticket_type_id)
                .values(tickets_sold=TicketType.tickets_sold + 1)
            )
            await self.db.execute(update_query)

            new_ticket = Ticket(owner_id=user_id, **ticket_data.model_dump())
            self.db.add(new_ticket)
            await self.db.flush()
            reservation_id = new_ticket.id

            redis_val = f"{new_ticket.ticket_type_id}:{int(time.time())}"
            await cast(
                Awaitable[int],
                self.redis.hset(
                    RedisKeys.active_reservations_hash(),
                    str(new_ticket.id),
                    redis_val,
                ),
            )

            await self.db.commit()
        except SQLAlchemyError as e:
            await self.db.rollback()
            if reservation_id is not None:
                await RedisKeys.refund_inventory_idempotent(
                    self.redis, ticket_data.ticket_type_id, reservation_id
                )
            else:
                safe_incr_script = """
                if redis.call('EXISTS', KEYS[1]) == 1 then
                    redis.call('INCR', KEYS[1])
                end
                """
                await self.redis.eval(safe_incr_script, 1, inventory_key)  # type: ignore[misc]

            error_msg = str(e).lower()
            if isinstance(e, IntegrityError) and (
                "ck_ticket_types_check_sold_limit" in error_msg
                or "checkviolation" in error_msg
            ):
                raise TicketsSoldOutError("该票种已售罄，暂无可用库存") from e
            raise

        # 数据库提交成功后，刷新或任务入队失败不能释放已存在票据的库存。
        await self.db.refresh(new_ticket)
        logger.info(f"票据预订成功，票据 ID：{new_ticket.id}")
        await self.arq_pool.enqueue_job(
            "release_unpaid_ticket",
            new_ticket.id,
            _defer_by=settings.TICKET_RESERVATION_TIME_SECONDS,
        )

        return TicketResponse.model_validate(new_ticket, from_attributes=True)

    async def get(self, owner_id: int, ticket_id: int) -> TicketDetailResponse:
        """按 owner_id 和 ticket_id 查询用户自己的票据，预加载票种及活动关系。

        返回票据详情；不存在或不属于该用户时抛出 TicketNotFoundError。
        """
        query = (
            select(Ticket)
            .options(selectinload(Ticket.ticket_type).joinedload(TicketType.event))
            .where(Ticket.id == ticket_id)
            .where(Ticket.owner_id == owner_id)
        )
        ticket = await self.db.scalar(query)
        if not ticket:
            raise TicketNotFoundError("票据不存在或不属于当前用户")

        return TicketDetailResponse.model_validate(ticket, from_attributes=True)

    async def get_all_for_user(
        self, owner_id: int, offset: int, limit: int
    ) -> Sequence[TicketResponse]:
        """按用户 ID 筛选票据，再通过 offset 和 limit 分页返回票据响应列表。"""
        query = (
            select(Ticket)
            .where(Ticket.owner_id == owner_id)
            .offset(offset)
            .limit(limit)
        )
        result = await self.db.scalars(query)
        return [
            TicketResponse.model_validate(t, from_attributes=True) for t in result.all()
        ]

    async def delete(self, owner_id: int, ticket_id: int) -> None:
        """取消用户自己的未付款预订，并只回补一次库存。

        加锁查询票据，RESERVED 改为 CANCELED 并减少数据库占用数量；重复取消不再减少。
        先提交数据库，再幂等回补 Redis，保留取消记录供重试和定时补偿使用。
        票据不存在时抛出 TicketNotFoundError，已付款时抛出 TicketAlreadyPaidError。
        """
        query = (
            select(Ticket)
            .where(Ticket.id == ticket_id)
            .where(Ticket.owner_id == owner_id)
            .with_for_update()
        )
        ticket = await self.db.scalar(query)

        if not ticket:
            raise TicketNotFoundError("票据不存在或不属于当前用户")

        if ticket.status == TicketStatus.SOLD:
            raise TicketAlreadyPaidError("已付款票据不能直接取消，需要退款流程")

        ticket_type_id = ticket.ticket_type_id
        try:
            if ticket.status == TicketStatus.RESERVED:
                ticket.status = TicketStatus.CANCELED
                update_query = (
                    update(TicketType)
                    .where(TicketType.id == ticket_type_id)
                    .values(tickets_sold=TicketType.tickets_sold - 1)
                )
                await self.db.execute(update_query)
            await self.db.commit()
        except SQLAlchemyError:
            await self.db.rollback()
            raise

        await RedisKeys.refund_inventory_idempotent(
            redis=self.redis, ticket_type_id=ticket_type_id, ticket_id=ticket_id
        )
        logger.info(f"票据取消成功，票据 ID：{ticket_id}")
