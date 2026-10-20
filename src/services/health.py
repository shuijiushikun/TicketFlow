from redis.asyncio import Redis
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from src.core import logger
from src.core.exception import HealthError


class HealthService:
    """依赖检查业务层，检查 PostgreSQL 和 Redis 的连接可用性。"""

    def __init__(self, session: AsyncSession, redis: Redis) -> None:
        self.db = session
        self.redis = redis

    async def check_readiness(self) -> bool:
        """执行轻量数据库查询和 Redis 探测；任一失败时抛出 HealthError。"""
        try:
            await self.db.execute(text("SELECT 1"))
            await self.redis.ping()
            return True
        except Exception as e:
            logger.info(f"依赖就绪检查失败：{e}")
            raise HealthError("数据库或 Redis 暂不可用") from e
