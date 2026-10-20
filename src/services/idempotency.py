from __future__ import annotations

import hashlib
import json
from typing import Any

from pydantic import ValidationError
from redis.asyncio import Redis

from src.core import settings
from src.core.exception import IdempotencyConflictError, IdempotencyStateError
from src.schemas import IdempotencyRecord

RedisClient = Redis


class IdempotencyService:
    """使用 Redis 临时锁和响应缓存处理重复请求，效果受记录有效期限制。"""

    def __init__(self, redis_client: RedisClient):
        self.redis = redis_client
        self.ttl_seconds = settings.REDIS_TTL_SECONDS

    def _generate_key(self, user_id: int, action: str, idempotency_key: str) -> str:
        return f"idem:u{user_id}:{action}:{idempotency_key}"

    def _hash_payload(self, payload: dict[str, Any] | None) -> str:
        if not payload:
            return hashlib.sha256(b"").hexdigest()

        payload_bytes = json.dumps(payload, sort_keys=True).encode("utf-8")
        return hashlib.sha256(payload_bytes).hexdigest()

    async def check_and_lock(
        self,
        user_id: int,
        action: str,
        idempotency_key: str,
        payload: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        """检查幂等记录，并为新请求获取临时锁。

        user_id、action 和 idempotency_key 共同确定记录。
        payload 用于校验请求内容是否一致。
        新请求返回 None；有效期内已完成的相同请求返回缓存响应。
        内容不同或仍在处理时抛出 IdempotencyConflictError。
        临时状态异常时抛出 IdempotencyStateError。
        """
        redis_key = self._generate_key(user_id, action, idempotency_key)
        current_hash = self._hash_payload(payload)

        initial_record = IdempotencyRecord(
            status="IN_PROGRESS", payload_hash=current_hash
        )
        lock_acquired = await self.redis.set(
            name=redis_key,
            value=initial_record.model_dump_json(),
            ex=self.ttl_seconds,
            nx=True,
        )

        if lock_acquired:
            return None

        raw_key = await self.redis.get(redis_key)

        if not raw_key:
            raise IdempotencyStateError("请求发生并发冲突，请重试")

        try:
            data = IdempotencyRecord.model_validate_json(raw_key)
        except ValidationError as e:
            raise IdempotencyStateError("幂等缓存记录异常，请重试") from e

        if data.payload_hash != current_hash:
            raise IdempotencyConflictError(
                "该幂等键已用于其他请求内容，请为新操作更换幂等键"
            )

        if data.status == "IN_PROGRESS":
            raise IdempotencyConflictError("相同请求正在处理中，请稍后重试")

        return data.response

    async def save_response(
        self,
        user_id: int,
        action: str,
        idempotency_key: str,
        response_data: dict[str, Any],
        payload: dict[str, Any] | None = None,
    ) -> None:
        """将请求标记为 COMPLETED（已完成），保存响应及请求内容摘要，并设置有效期。

        response_data 必须可序列化为 JSON；记录过期后，相同幂等键可能再次执行业务。
        """
        redis_key = self._generate_key(user_id, action, idempotency_key)
        current_hash = self._hash_payload(payload)

        record = IdempotencyRecord(
            status="COMPLETED", response=response_data, payload_hash=current_hash
        )

        await self.redis.set(redis_key, record.model_dump_json(), ex=self.ttl_seconds)

    async def unlock(self, user_id: int, action: str, idempotency_key: str) -> None:
        """业务执行失败时删除临时幂等记录，允许客户端使用相同键重试。"""
        redis_key = self._generate_key(user_id, action, idempotency_key)
        await self.redis.delete(redis_key)
