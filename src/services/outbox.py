import asyncio
from collections.abc import Awaitable, Callable, Mapping, Sequence
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import delete, select
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
        *,
        coalesce_kinds: frozenset[str] = frozenset(),
    ) -> None:
        self.session_maker = session_maker
        self.handlers = handlers
        # 仅允许显式声明为“读取当前状态”的处理器合并，逐笔副作用不能合并。
        self.coalesce_kinds = coalesce_kinds

    async def dispatch(self, outbox_id: int) -> bool:
        """按 ID 锁定到期待办；成功投递并提交删除才返回 True。"""
        return bool(await self._dispatch_group((outbox_id,)))

    async def _dispatch_group(self, outbox_ids: Sequence[int]) -> int:
        """锁定已选 ID；成功后仅确认这批记录，外部新待办留待下一轮。"""
        delivered = 0
        async with self.session_maker() as session, session.begin():
            events = list(
                await session.scalars(
                    select(OutboxEvent)
                    .where(
                        OutboxEvent.id.in_(outbox_ids),
                        OutboxEvent.next_attempt_at <= datetime.now(UTC),
                    )
                    .order_by(OutboxEvent.id)
                    .with_for_update(skip_locked=True)
                )
            )
            if not events:
                return 0
            event = events[0]
            try:
                # 处理器的数据库操作失败时回滚保存点，外层仍能记录重试。
                async with session.begin_nested():
                    if len(events) > 1 and (
                        event.kind not in self.coalesce_kinds
                        or any(
                            (item.kind, item.aggregate_id)
                            != (event.kind, event.aggregate_id)
                            for item in events
                        )
                    ):
                        raise ValueError("拒绝合并不同业务或不支持合并的待办")
                    handler = self.handlers[event.kind]
                    await handler(session, event)
            except Exception:
                for item in events:
                    item.attempts += 1
                    delay = min(2 ** min(item.attempts - 1, 9), 300)
                    item.next_attempt_at = datetime.now(UTC) + timedelta(seconds=delay)
                logger.warning(
                    "Outbox 投递失败，保留待办 id=%s kind=%s attempts=%s count=%s",
                    event.id,
                    event.kind,
                    event.attempts,
                    len(events),
                    exc_info=True,
                )
            else:
                await session.execute(
                    delete(OutboxEvent).where(
                        OutboxEvent.id.in_([item.id for item in events])
                    )
                )
                delivered = len(events)
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
        """有限扫描，按显式规则合并同一对象，返回成功确认的待办条数。

        默认 100 条，最多 1000 条、30 秒；每组独立事务，未知 kind 保留并退避。
        """
        batch_size = min(max(batch_size, 1), 1000)
        time_budget_seconds = min(max(time_budget_seconds, 0.01), 30)
        delivered = 0
        try:
            async with asyncio.timeout(time_budget_seconds):
                async with self.session_maker() as session:
                    rows = (
                        await session.execute(
                            select(
                                OutboxEvent.id,
                                OutboxEvent.kind,
                                OutboxEvent.aggregate_id,
                            )
                            .where(OutboxEvent.next_attempt_at <= datetime.now(UTC))
                            .order_by(OutboxEvent.next_attempt_at, OutboxEvent.id)
                            .limit(batch_size)
                        )
                    ).all()
                groups: dict[tuple[bool, str, int], list[int]] = {}
                for row in rows:
                    key = (
                        (True, row.kind, row.aggregate_id)
                        if row.kind in self.coalesce_kinds
                        else (False, row.kind, row.id)
                    )
                    groups.setdefault(key, []).append(row.id)
                for ids in groups.values():
                    delivered += await self._dispatch_group(ids)
        except TimeoutError:
            logger.warning("Outbox 批次达到时间预算，剩余待办留待下轮")
        return delivered
