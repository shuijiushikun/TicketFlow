from typing import TYPE_CHECKING

from redis.asyncio import Redis

# types-redis 声明了泛型，但运行时 Redis 类不支持下标；FastAPI 会求值注解。
if TYPE_CHECKING:
    RedisClient = Redis[str]
else:
    RedisClient = Redis


class RedisKeys:
    """集中管理 Redis 键名，统一缓存、库存和预订记录的命名规则。

    活动查询缓存使用独立命名空间及随机版本，库存仅为数据库的可恢复副本。
    """

    @staticmethod
    def event_static(event_id: int) -> str:
        """生成活动详情缓存哈希键，保存版本与响应正文。"""
        return f"ticketcore:cache:v2:events:detail:{event_id}"

    @staticmethod
    def event_generation(event_id: int) -> str:
        """生成活动详情版本键，包括确认不存在的活动。"""
        return f"ticketcore:cache:v2:events:detail:{event_id}:generation"

    @staticmethod
    def event_list_generation() -> str:
        """生成分页列表共享的版本键。"""
        return "ticketcore:cache:v2:events:list:generation"

    @staticmethod
    def event_list(offset: int, limit: int) -> str:
        """生成分页缓存键，读取时原子检查其版本是否仍有效。"""
        return f"ticketcore:cache:v2:events:list:o:{offset}:l:{limit}"

    @staticmethod
    def cache_lock(cache_key: str) -> str:
        """生成单个详情或分页缓存的重建锁键。"""
        return f"{cache_key}:lock"

    @staticmethod
    def ticket_type_inventory(ticket_type_id: int) -> str:
        """生成数据库剩余库存的 Redis 投影键，不用于扣票决策。"""
        return f"ticketcore:inventory:ticket_type:{ticket_type_id}"

    @staticmethod
    def inventory_version(ticket_type_id: int) -> str:
        """生成库存投影版本键，禁止旧同步命令覆盖新状态。"""
        return f"ticketcore:inventory:v2:ticket_type:{ticket_type_id}:version"

    @staticmethod
    def active_reservations_hash() -> str:
        """旧版本预订哈希，仅用于核对数据库最终状态后清理。"""
        return "system:active_reservations"
