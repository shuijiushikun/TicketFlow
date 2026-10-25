import asyncio
from collections.abc import Sequence

from arq import ArqRedis
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from src.core import logger, settings
from src.core.exception import (
    TicketAlreadyPaidError,
    TicketNotFoundError,
)
from src.core.redis_keys import RedisClient
from src.models import Ticket, TicketType
from src.models.ticket import TicketStatus
from src.schemas import TicketCreate, TicketResponse
from src.schemas.ticket import TicketDetailResponse
from src.services.inventory import sync_reserved_inventory, transaction_outbox
from src.services.reservations import cancel_reservation, reserve_ticket
from src.services.transactions import commit_business


class TicketService:
    """数据库条件扣票，票据与库存同步待办在同一事务中持久化。"""

    def __init__(
        self, session: AsyncSession, arq_pool: ArqRedis, redis: RedisClient
    ) -> None:
        self.db = session
        self.arq_pool = arq_pool
        self.redis = redis
        self.outbox = transaction_outbox(session, redis)

    async def create(self, user_id: int, ticket_data: TicketCreate) -> TicketResponse:
        """原子判断剩余量并扣一张票；回滚时无需跨系统加回库存。"""
        try:
            reserved = await reserve_ticket(
                self.db, user_id, ticket_data.ticket_type_id
            )
            response = TicketResponse.model_validate(reserved.ticket)
            await commit_business(self.db, response)
        except SQLAlchemyError:
            await self.db.rollback()
            raise

        await sync_reserved_inventory(
            self.db,
            self.redis,
            outbox_id=reserved.outbox_id,
            ticket_type_id=response.ticket_type_id,
            version=reserved.version,
            available=reserved.remaining,
        )
        try:
            async with asyncio.timeout(settings.TRANSACTION_REDIS_TIMEOUT_SECONDS):
                await self.arq_pool.enqueue_job(
                    "release_unpaid_ticket",
                    response.id,
                    _job_id=f"ticket-expire:{response.id}",
                    _defer_by=settings.TICKET_RESERVATION_TIME_SECONDS,
                )
        except Exception:
            # 数据库期限扫描是兜底，漏入队不改变已提交的预订结果。
            logger.exception(
                "延迟回收任务入队失败，留待数据库扫描 ticket=%s", response.id
            )
        logger.info(f"票据预订成功，票据 ID：{response.id}")
        return response

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
        """锁定票据后取消预订，库存释放与同步待办同事务提交。"""
        query = (
            select(Ticket)
            .where(Ticket.id == ticket_id)
            .where(Ticket.owner_id == owner_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        ticket = await self.db.scalar(query)

        if not ticket:
            raise TicketNotFoundError("票据不存在或不属于当前用户")

        if ticket.status == TicketStatus.SOLD:
            raise TicketAlreadyPaidError("已付款票据不能直接取消，需要退款流程")

        try:
            pending_ids = await cancel_reservation(self.db, ticket)
            await commit_business(self.db)
        except SQLAlchemyError:
            await self.db.rollback()
            raise

        for pending_id in pending_ids:
            await self.outbox.dispatch_best_effort(pending_id)
        logger.info(f"票据取消成功，票据 ID：{ticket_id}")
