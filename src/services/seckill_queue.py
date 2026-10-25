"""持久化申请到独立 Arq 队列的投递协议。"""

import asyncio

from arq import ArqRedis
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.config import settings
from src.models.outbox import OutboxEvent
from src.services.outbox import OutboxHandler

SECKILL_DISPATCH = "seckill_dispatch"


async def enqueue_seckill(pool: ArqRedis, request_id: int) -> None:
    """使用稳定任务编号投递；重复任务最终仍由数据库申请状态去重。"""
    async with asyncio.timeout(settings.TRANSACTION_REDIS_TIMEOUT_SECONDS):
        await pool.enqueue_job(
            "process_seckill_request",
            request_id,
            _queue_name=settings.SECKILL_QUEUE_NAME,
            _job_id=f"seckill-request:{request_id}",
            _expires=settings.SECKILL_WAIT_SECONDS + 30,
        )


def seckill_dispatch_handler(pool: ArqRedis) -> OutboxHandler:
    """生成可重放的入队处理器，投递成功后由 Outbox 确认本条记录。"""

    async def handle(session: AsyncSession, event: OutboxEvent) -> None:
        await enqueue_seckill(pool, event.aggregate_id)

    return handle
