from collections.abc import Awaitable
from typing import TYPE_CHECKING, cast

from redis.asyncio import Redis

# types-redis 声明了泛型，但运行时 Redis 类不支持下标；FastAPI 会求值注解。
if TYPE_CHECKING:
    RedisClient = Redis[str]
else:
    RedisClient = Redis


class RedisKeys:
    """集中管理 Redis 键名，统一缓存、库存和预订记录的命名规则。

    活动查询缓存使用独立命名空间及随机版本，库存键保持原有语义。
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
        """根据票种 ID 生成可用库存计数器的键，供 Lua 脚本原子扣减。"""
        return f"ticketcore:inventory:ticket_type:{ticket_type_id}"

    @staticmethod
    def canceled_tickets_set() -> str:
        """生成已完成库存回补的票据集合键。

        集合中保存处理过的票据 ID，避免重试时重复回补库存。
        """
        return "system:canceled_tickets"

    @staticmethod
    def active_reservations_hash() -> str:
        """生成有效预订记录的哈希表键。

        字段为票据 ID，值为票种 ID 与时间戳，以冒号分隔；后台据此排查异常预订。
        """
        return "system:active_reservations"

    @classmethod
    async def refund_inventory_idempotent(
        cls, redis: Redis, ticket_type_id: int, ticket_id: int
    ) -> None:
        """幂等回补已取消票据的库存。

        Lua 脚本原子检查票据是否已回补。
        未处理时记录票据 ID、回补已存在的库存键并移除有效预订记录。
        redis 必须连接业务库，ticket_type_id 是票种 ID，ticket_id 是被取消的票据 ID。
        """
        lua_script = """
            -- KEYS[1]：票种可用库存键
            -- KEYS[2]：已回补票据 ID 的集合键
            -- KEYS[3]：有效预订记录的哈希表键
            -- ARGV[1]：本次取消的票据 ID

            if redis.call('SISMEMBER', KEYS[2], ARGV[1]) == 0 then
                redis.call('SADD', KEYS[2], ARGV[1])
                if redis.call('EXISTS', KEYS[1]) == 1 then
                    redis.call('INCR', KEYS[1])
                end
                redis.call('HDEL', KEYS[3], ARGV[1])
                return 1
            end
            return 0
            """
        inventory_key = cls.ticket_type_inventory(ticket_type_id)
        processed_set_key = cls.canceled_tickets_set()
        active_hash_key = cls.active_reservations_hash()

        await redis.eval(
            lua_script,
            3,
            inventory_key,
            processed_set_key,
            active_hash_key,
            str(ticket_id),
        )  # type: ignore[misc]

    @classmethod
    async def get_all_active_reservations(cls, redis: Redis) -> list[dict[str, int]]:
        """读取 Redis 中的有效预订记录。

        返回字典列表，每项包含 ticket_id、ticket_type_id 和 timestamp。
        后台任务据此排查异常记录。
        """
        hash_key = cls.active_reservations_hash()
        raw_data = await cast(Awaitable[dict[bytes, bytes]], redis.hgetall(hash_key))

        reservations = []
        for ticket_id_bytes, val_bytes in raw_data.items():
            val_str = (
                val_bytes.decode("utf-8") if isinstance(val_bytes, bytes) else val_bytes
            )
            ticket_type_id_str, ts_str = val_str.split(":")

            reservations.append(
                {
                    "ticket_id": int(ticket_id_bytes),
                    "ticket_type_id": int(ticket_type_id_str),
                    "timestamp": int(ts_str),
                }
            )

        return reservations
