"""秒杀申请接收与结果读取，不在 HTTP 提交链路中扣票。"""

from datetime import UTC, datetime, timedelta
from uuid import uuid4

from arq import ArqRedis
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.exception import (
    BookingModeError,
    SeckillNotFoundError,
    TicketTypeNotFoundError,
)
from src.core.logger import logger
from src.core.redis_keys import RedisClient
from src.core.seckill_policy import acquire_admission, submission_stage
from src.models.seckill import SeckillRequest
from src.models.ticket_type import TicketType
from src.schemas.seckill import SeckillCreate, SeckillResponse
from src.services.outbox import enqueue_outbox
from src.services.seckill_queue import SECKILL_DISPATCH, enqueue_seckill
from src.services.transactions import commit_business


class SeckillService:
    """提交时原子保存申请、幂等响应和入队记录，查询只允许本人。"""

    def __init__(
        self, session: AsyncSession, redis: RedisClient, pool: ArqRedis
    ) -> None:
        self.db, self.redis, self.pool = session, redis, pool

    async def submit(self, owner_id: int, data: SeckillCreate) -> SeckillResponse:
        """接收申请并返回凭证；失败回滚，成功后的投递故障交给后台恢复。"""
        with submission_stage("ticket_type"):
            mode = await self.db.scalar(
                select(TicketType.is_seckill).where(
                    TicketType.id == data.ticket_type_id
                )
            )
        if mode is None:
            raise TicketTypeNotFoundError("票种不存在")
        if not mode:
            raise BookingModeError("该票种请通过普通预约入口购票")
        token = uuid4().hex
        expires_at = await acquire_admission(self.redis, token)
        # 提交结果未知时保留名额至截止，不能在数据库可能已提交后提前让出名额。
        try:
            with submission_stage("persist"):
                request = SeckillRequest(
                    owner_id=owner_id,
                    ticket_type_id=data.ticket_type_id,
                    admission_token=token,
                    expires_at=expires_at,
                    next_dispatch_at=datetime.now(UTC) + timedelta(seconds=5),
                )
                self.db.add(request)
                await self.db.flush()
                await enqueue_outbox(self.db, SECKILL_DISPATCH, request.id)
                response = SeckillResponse.model_validate(request)
                await commit_business(self.db, response)
        except BaseException:
            await self.db.rollback()
            # 不立即释放：commit 的响应丢失不能证明数据库没有保存申请。
            raise
        try:
            # HTTP 只做一次有界快速入队，不为确认投递额外争抢数据库连接。
            # 持久化 Outbox 留给原 Worker 确认；重复任务由申请终态去重。
            with submission_stage("dispatch"):
                await enqueue_seckill(self.pool, response.id)
        except Exception:
            logger.warning(
                "秒杀申请已保存，快速入队失败，留待后台补投 request=%s", response.id
            )
        return response

    async def get(self, owner_id: int, request_id: int) -> SeckillResponse:
        """读取最新申请状态，未知编号与他人申请统一返回不存在。"""
        request = await self.db.scalar(
            select(SeckillRequest)
            .where(SeckillRequest.id == request_id, SeckillRequest.owner_id == owner_id)
            .execution_options(populate_existing=True)
        )
        if request is None:
            raise SeckillNotFoundError("抢票申请不存在或不属于当前用户")
        return SeckillResponse.model_validate(request)
