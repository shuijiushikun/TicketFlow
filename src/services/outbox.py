import asyncio
from collections.abc import Awaitable, Callable, Mapping
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.core import logger
from src.core.cache_store import EventCacheStore
from src.models.outbox import OutboxEvent

OutboxHandler = Callable[[AsyncSession, OutboxEvent], Awaitable[None]]
EVENT_CACHE_INVALIDATE = "event_cache_invalidate"


async def enqueue_outbox(
    session: AsyncSession,
    kind: str,
    aggregate_id: int,
    payload: dict[str, Any] | None = None,
) -> OutboxEvent:
    """在调用方业务事务中写待办并取得 ID，不提交事务。"""
    event = OutboxEvent(kind=kind, aggregate_id=aggregate_id, payload=payload or {})
    session.add(event)
    await session.flush()
    return event


def event_cache_handler(store: EventCacheStore) -> OutboxHandler:
    """复用有超时且强制尝试的原子缓存失效；重复切换版本是安全的。"""

    async def invalidate(session: AsyncSession, event: OutboxEvent) -> None:
        await store.invalidate(event.aggregate_id)

    return invalidate


class OutboxDispatcher:
    """用独立事务锁定并投递待办；处理器不得自行提交事务。

    外部副作用可能成功后进程退出，因此处理器必须支持重复执行。
    普通投递异常保留待办并退避，取消直接传播且由事务回滚保留待办。
    """

    def __init__(
        self,
        session_maker: async_sessionmaker[AsyncSession],
        handlers: Mapping[str, OutboxHandler],
    ) -> None:
        self.session_maker = session_maker
        self.handlers = handlers

    async def dispatch(self, outbox_id: int) -> bool:
        """按 ID 锁定到期待办；成功投递并提交删除才返回 True。"""
        delivered = False
        async with self.session_maker() as session, session.begin():
            event = await session.scalar(
                select(OutboxEvent)
                .where(
                    OutboxEvent.id == outbox_id,
                    OutboxEvent.next_attempt_at <= datetime.now(UTC),
                )
                .with_for_update(skip_locked=True)
            )
            if event is None:
                return False
            try:
                # 处理器的数据库操作失败时回滚保存点，外层仍能记录重试。
                async with session.begin_nested():
                    handler = self.handlers[event.kind]
                    await handler(session, event)
            except Exception:
                event.attempts += 1
                delay = min(2 ** min(event.attempts - 1, 9), 300)
                event.next_attempt_at = datetime.now(UTC) + timedelta(seconds=delay)
                logger.warning(
                    "Outbox 投递失败，保留待办 id=%s kind=%s attempts=%s",
                    event.id,
                    event.kind,
                    event.attempts,
                    exc_info=True,
                )
            else:
                await session.delete(event)
                delivered = True
        return delivered

    async def dispatch_best_effort(self, outbox_id: int) -> None:
        """仅用于业务提交后的快速尝试；投递故障不改写已提交业务结果。"""
        try:
            async with asyncio.timeout(0.5):
                await self.dispatch(outbox_id)
        except Exception:
            logger.exception("Outbox 快速投递未完成，待后台恢复 id=%s", outbox_id)

    async def dispatch_batch(
        self, *, batch_size: int = 100, time_budget_seconds: float = 5
    ) -> int:
        """供 cron 调用的有限到期扫描，每项独立事务，返回成功投递数。

        单批最多 100 项、30 秒。未知 kind 同样保留并退避，禁止丢弃待办。
        """
        batch_size = min(max(batch_size, 1), 100)
        time_budget_seconds = min(max(time_budget_seconds, 0.01), 30)
        delivered = 0
        try:
            async with asyncio.timeout(time_budget_seconds):
                async with self.session_maker() as session:
                    ids = list(
                        await session.scalars(
                            select(OutboxEvent.id)
                            .where(OutboxEvent.next_attempt_at <= datetime.now(UTC))
                            .order_by(OutboxEvent.next_attempt_at, OutboxEvent.id)
                            .limit(batch_size)
                        )
                    )
                for outbox_id in ids:
                    delivered += await self.dispatch(outbox_id)
        except TimeoutError:
            logger.warning("Outbox 批次达到时间预算，剩余待办留待下轮")
        return delivered
