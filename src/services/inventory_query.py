"""余票展示查询，与数据库扣票及结算判断分开。"""

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.cache_runtime import cache_admission, cache_circuit
from src.core.cache_store import CacheOfflineError, EventCacheStore
from src.core.exception import TicketTypeNotFoundError
from src.core.metrics import INVENTORY_READS
from src.core.redis_keys import RedisClient, RedisKeys
from src.models import TicketType
from src.schemas.ticket_type import TicketTypeInventoryResponse
from src.services.inventory import write_inventory_snapshot


class InventoryQueryService:
    """提供非承诺性的余票展示；数据库回源复用进程共享预算。"""

    def __init__(self, session: AsyncSession, redis: RedisClient) -> None:
        self.db = session
        self.redis = redis
        self.store = EventCacheStore(redis, cache_circuit)

    async def get(self, ticket_type_id: int) -> TicketTypeInventoryResponse:
        """Redis 命中不查库；缺失或故障有界回源，不承诺实时库存。"""
        try:
            remaining = await self._read_projection(ticket_type_id)
        except CacheOfflineError:
            remaining = None
        if remaining is not None:
            INVENTORY_READS.labels("redis").inc()
            return TicketTypeInventoryResponse(
                ticket_type_id=ticket_type_id, remaining=remaining
            )

        async with cache_admission.slot():
            row = (
                await self.db.execute(
                    select(
                        TicketType.tickets_quantity - TicketType.tickets_sold,
                        TicketType.inventory_version,
                    ).where(TicketType.id == ticket_type_id)
                )
            ).first()
        INVENTORY_READS.labels("database").inc()
        if row is None:
            raise TicketTypeNotFoundError("票种不存在")
        # 沿用交易同步的版本校验；并发扣票/删除后，旧查询不能覆盖新快照。
        if self.store.circuit.available():
            try:
                await self.store.call(
                    lambda: write_inventory_snapshot(
                        self.redis, ticket_type_id, row[1], row[0]
                    )
                )
            except CacheOfflineError:
                pass  # 展示查询不能因缓存写入失败丢掉已经取得的数据库结果。
        return TicketTypeInventoryResponse(
            ticket_type_id=ticket_type_id, remaining=row[0]
        )

    async def _read_projection(self, ticket_type_id: int) -> int | None:
        """一次 MGET 原子读取余票与版本；旧格式或损坏数据不直接展示。"""
        remaining, version = await self.store.call(
            lambda: self.redis.mget(
                [
                    RedisKeys.ticket_type_inventory(ticket_type_id),
                    RedisKeys.inventory_version(ticket_type_id),
                ]
            )
        )
        if (
            not isinstance(version, str)
            or len(version) != 20
            or not version.isascii()
            or not version.isdecimal()
        ):
            return None
        if version == str(2**63).zfill(20):
            INVENTORY_READS.labels("redis").inc()
            raise TicketTypeNotFoundError("票种不存在")
        if (
            version > str(2**63 - 1).zfill(20)
            or not isinstance(remaining, str)
            or not 1 <= len(remaining) <= 10
            or not remaining.isascii()
            or not remaining.isdecimal()
            or int(remaining) > 2**31 - 1
        ):
            return None
        return int(remaining)
