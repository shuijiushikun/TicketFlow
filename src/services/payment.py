from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from src.core import logger
from src.core.exception import (
    TicketAlreadyPaidError,
    TicketNotFoundError,
    TicketReservationExpireError,
)
from src.core.redis_keys import RedisClient
from src.models import Ticket
from src.models.ticket import TicketStatus
from src.schemas.payment import TicketPaymentSchema
from src.services.inventory import RESERVATION_CLEANUP, transaction_outbox
from src.services.outbox import enqueue_outbox
from src.services.reservations import reservation_expired
from src.services.transactions import commit_business


class PaymentService:
    """模拟支付业务层，校验支付条件并处理票据状态流转。"""

    def __init__(self, session: AsyncSession, redis: RedisClient) -> None:
        self.db = session
        self.redis = redis
        self.outbox = transaction_outbox(session, redis)

    async def pay_for_ticket(
        self, ticket_id: int, owner_id: int
    ) -> TicketPaymentSchema:
        """将当前用户未过期的预留票据改为 SOLD（已售出）。

        根据 ticket_id 和 owner_id 查询并加行锁，避免并发操作同一张票。
        不存在或不属于用户时抛出 TicketNotFoundError。
        已付款时抛出 TicketAlreadyPaidError。
        已取消或超期时抛出 TicketReservationExpireError。
        数据库写入失败时回滚。
        支付和清理待办同事务提交，提交后尝试清理旧 Redis 标记；不对接真实支付渠道。
        """
        ticket_query = (
            select(Ticket)
            .where(Ticket.id == ticket_id)
            .where(Ticket.owner_id == owner_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        ticket = await self.db.scalar(ticket_query)

        if not ticket:
            raise TicketNotFoundError("票据不存在或不属于当前用户")

        if ticket.status == TicketStatus.SOLD:
            logger.warning(f"票据 {ticket_id} 已售出（SOLD），拒绝重复支付")
            raise TicketAlreadyPaidError("该票据已经付款，请勿重复支付")

        if ticket.status == TicketStatus.CANCELED:
            logger.warning(f"票据 {ticket_id} 已取消（CANCELED），拒绝支付")
            raise TicketReservationExpireError("预订已取消或超过支付期限，请重新预订")

        if reservation_expired(ticket, datetime.now(UTC)):
            raise TicketReservationExpireError("预订已取消或超过支付期限，请重新预订")

        ticket.status = TicketStatus.SOLD

        try:
            pending = await enqueue_outbox(self.db, RESERVATION_CLEANUP, ticket_id)
            pending_id = pending.id
            response = TicketPaymentSchema.model_validate(ticket)
            await commit_business(self.db, response)
        except SQLAlchemyError:
            await self.db.rollback()
            raise

        await self.outbox.dispatch_best_effort(pending_id)
        logger.info(f"票据 {ticket_id} 模拟支付成功，已改为 SOLD（已售出）")
        return response
