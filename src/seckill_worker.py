"""独立秒杀消费进程：限制数据库扣票并发，恢复未完成申请。"""

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any

from arq import Retry, cron
from arq.connections import RedisSettings
from arq.constants import default_queue_name
from sqlalchemy import select, text
from sqlalchemy.exc import SQLAlchemyError

from src.core.config import settings
from src.core.exception import (
    BookingModeError,
    TicketsSoldOutError,
    TicketTypeNotFoundError,
)
from src.core.logger import logger
from src.core.seckill_policy import release_admission
from src.models.seckill import SeckillRequest, SeckillStatus
from src.services.inventory import sync_reserved_inventory
from src.services.reservations import ReservedTicket, reserve_ticket
from src.services.seckill_queue import enqueue_seckill
from src.services.transactions import commit_business
from src.worker import shutdown, startup


def _utc(value: datetime) -> datetime:
    return value if value.tzinfo else value.replace(tzinfo=UTC)


async def process_seckill_request(ctx: dict[str, Any], request_id: int) -> None:
    """锁定申请后原子完成扣票、建票、结果和库存待办，重复消费不会再扣票。"""
    reserved: ReservedTicket | None = None
    async with ctx["session_maker"]() as session:
        try:
            # 只约束新消费事务，不改普通预约及其他 SQL 的预算。
            if session.bind.dialect.name == "postgresql":
                await session.execute(text("SET LOCAL lock_timeout = '1000ms'"))
                await session.execute(text("SET LOCAL statement_timeout = '3000ms'"))
            application = await session.scalar(
                select(SeckillRequest)
                .where(SeckillRequest.id == request_id)
                .with_for_update(skip_locked=True)
                .execution_options(populate_existing=True)
            )
            if application is None:
                return
            token = application.admission_token
            if application.status == SeckillStatus.QUEUED.value:
                now = datetime.now(UTC)
                if _utc(application.expires_at) <= now:
                    application.status = SeckillStatus.EXPIRED.value
                else:
                    try:
                        reserved = await reserve_ticket(
                            session,
                            application.owner_id,
                            application.ticket_type_id,
                            seckill=True,
                        )
                    except TicketsSoldOutError:
                        application.status = SeckillStatus.SOLD_OUT.value
                    except (BookingModeError, TicketTypeNotFoundError):
                        application.status = SeckillStatus.REJECTED.value
                    else:
                        # 扣票等待跨过排队期限时回滚整笔事务，下次只标记超时。
                        if _utc(application.expires_at) <= datetime.now(UTC):
                            await session.rollback()
                            raise Retry(defer=0)
                        application.status = SeckillStatus.SUCCEEDED.value
                        application.ticket_id = reserved.ticket.id
                application.completed_at = datetime.now(UTC)
            await commit_business(session)
        except (SQLAlchemyError, TimeoutError) as error:
            await session.rollback()
            logger.warning(
                "秒杀事务暂未完成 request=%s error=%s", request_id, type(error).__name__
            )
            raise Retry(defer=1) from error

        await release_admission(ctx["business_redis"], token)
        if reserved is not None:
            await sync_reserved_inventory(
                session,
                ctx["business_redis"],
                outbox_id=reserved.outbox_id,
                ticket_type_id=reserved.ticket.ticket_type_id,
                version=reserved.version,
                available=reserved.remaining,
            )
            try:
                async with asyncio.timeout(settings.TRANSACTION_REDIS_TIMEOUT_SECONDS):
                    deadline = _utc(reserved.ticket.created_at) + timedelta(
                        seconds=settings.TICKET_RESERVATION_TIME_SECONDS
                    )
                    await ctx["redis"].enqueue_job(
                        "release_unpaid_ticket",
                        reserved.ticket.id,
                        _queue_name=default_queue_name,
                        _job_id=f"ticket-expire:{reserved.ticket.id}",
                        _defer_by=max(
                            0, (deadline - datetime.now(UTC)).total_seconds()
                        ),
                    )
            except Exception:
                logger.exception(
                    "秒杀票据回收任务入队失败，留待数据库扫描 ticket=%s",
                    reserved.ticket.id,
                )


async def reconcile_seckill_requests(ctx: dict[str, Any]) -> None:
    """每轮有限认领到期申请或待补投申请，Redis 丢队列后可恢复。"""
    try:
        async with asyncio.timeout(2):
            tokens: list[str] = []
            request_ids: list[int] = []
            async with ctx["session_maker"]() as session, session.begin():
                now = datetime.now(UTC)
                applications = await session.scalars(
                    select(SeckillRequest)
                    .where(
                        SeckillRequest.status == SeckillStatus.QUEUED.value,
                        SeckillRequest.next_dispatch_at <= now,
                    )
                    .order_by(SeckillRequest.next_dispatch_at, SeckillRequest.id)
                    .limit(settings.SECKILL_RECOVERY_BATCH)
                    .with_for_update(skip_locked=True)
                )
                for application in applications:
                    application.next_dispatch_at = now + timedelta(seconds=5)
                    if _utc(application.expires_at) <= now:
                        application.status = SeckillStatus.EXPIRED.value
                        application.completed_at = now
                        tokens.append(application.admission_token)
                    else:
                        request_ids.append(application.id)
            for token in tokens:
                await release_admission(ctx["business_redis"], token)
            for request_id in request_ids:
                try:
                    await enqueue_seckill(ctx["redis"], request_id)
                except Exception:
                    logger.warning(
                        "秒杀申请补投未完成 request=%s", request_id, exc_info=True
                    )
                    break
    except TimeoutError:
        logger.warning("秒杀恢复达到本轮预算，剩余申请留待下轮")


async def seckill_startup(ctx: dict[str, Any]) -> None:
    """复用资源初始化，使用独立指标端口以兼容普通 Worker 同时运行。"""
    ctx["metrics_port"] = settings.SECKILL_WORKER_METRICS_PORT
    await startup(ctx)


class WorkerSettings:
    """只消费秒杀队列，维护任务仍由原 Worker 处理。"""

    redis_settings = RedisSettings(
        host=settings.REDIS_HOST, port=settings.REDIS_PORT, database=1
    )
    queue_name = settings.SECKILL_QUEUE_NAME
    functions = [process_seckill_request]
    max_jobs = settings.SECKILL_WORKER_CONCURRENCY
    job_timeout = settings.SECKILL_JOB_TIMEOUT_SECONDS
    max_tries = 5
    keep_result = 0
    on_startup = seckill_startup
    on_shutdown = shutdown
    cron_jobs = [
        cron(reconcile_seckill_requests, second=set(range(0, 60, 5)), timeout=3)
    ]
