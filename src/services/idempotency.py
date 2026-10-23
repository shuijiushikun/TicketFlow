from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import Awaitable
from typing import Any, cast
from uuid import uuid4

from redis.exceptions import RedisError
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.config import settings
from src.core.exception import IdempotencyConflictError, IdempotencyStateError
from src.core.redis_keys import RedisClient
from src.models.idempotency import IdempotencyRequest
from src.services.transactions import IDEMPOTENCY_CONTEXT_KEY, IdempotencyContext


class IdempotencyService:
    """以数据库唯一约束保证去重，Redis 短租期锁只用于快速拒绝竞争。"""

    REDIS_TIMEOUT_SECONDS = settings.TRANSACTION_REDIS_TIMEOUT_SECONDS
    CLAIM_TIMEOUT_SECONDS = 0.5
    LOCK_LEASE_MS = 5000
    RELEASE_SCRIPT = """
        if redis.call('GET', KEYS[1]) == ARGV[1] then
            return redis.call('DEL', KEYS[1])
        end
        return 0
    """

    def __init__(self, redis_client: RedisClient, session: AsyncSession) -> None:
        self.redis = redis_client
        self.session = session
        self._lock_key: str | None = None
        self._lock_token: str | None = None
        self._context: IdempotencyContext | None = None

    def _generate_key(self, user_id: int, action: str, idempotency_key: str) -> str:
        scope = json.dumps([user_id, action, idempotency_key]).encode("utf-8")
        return f"ticketflow:idempotency:v2:lock:{hashlib.sha256(scope).hexdigest()}"

    def _hash_payload(self, payload: dict[str, Any] | None) -> str:
        payload_bytes = json.dumps(
            payload, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
        return hashlib.sha256(payload_bytes).hexdigest()

    async def _find(
        self, user_id: int, action: str, idempotency_key: str
    ) -> IdempotencyRequest | None:
        query = select(IdempotencyRequest).where(
            IdempotencyRequest.user_id == user_id,
            IdempotencyRequest.action == action,
            IdempotencyRequest.idempotency_key == idempotency_key,
        )
        result = await self.session.scalars(query)
        return result.one_or_none()

    def _response(
        self, record: IdempotencyRequest, payload_hash: str
    ) -> dict[str, Any]:
        if record.payload_hash != payload_hash:
            raise IdempotencyConflictError(
                "该幂等键已用于其他请求内容，请为新操作更换幂等键"
            )
        if record.response is None:
            raise IdempotencyStateError("幂等记录缺少已提交的响应快照，请联系管理员")
        return record.response

    async def _acquire(self, key: str, token: str) -> bool:
        # SET 可能已执行后响应超时；仍保留 token，以便 finally 尝试安全释放。
        self._lock_key, self._lock_token = key, token
        try:
            async with asyncio.timeout(self.REDIS_TIMEOUT_SECONDS):
                return bool(
                    await self.redis.set(key, token, nx=True, px=self.LOCK_LEASE_MS)
                )
        except (RedisError, OSError, TimeoutError):
            return True

    async def check_and_lock(
        self,
        user_id: int,
        action: str,
        idempotency_key: str,
        payload: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        """复用已提交响应，或在业务事务内插入唯一请求占用记录。"""
        if IDEMPOTENCY_CONTEXT_KEY in self.session.info:
            raise IdempotencyStateError("同一数据库会话不能同时处理多个幂等请求")
        payload_hash = self._hash_payload(payload)
        record = await self._find(user_id, action, idempotency_key)
        if record is not None:
            return self._response(record, payload_hash)

        token = uuid4().hex
        if not await self._acquire(
            self._generate_key(user_id, action, idempotency_key), token
        ):
            record = await self._find(user_id, action, idempotency_key)
            if record is not None:
                return self._response(record, payload_hash)
            raise IdempotencyConflictError("相同请求正在处理中，请稍后重试")

        record = IdempotencyRequest(
            user_id=user_id,
            action=action,
            idempotency_key=idempotency_key,
            payload_hash=payload_hash,
            owner_token=token,
        )
        self.session.add(record)
        deadline = asyncio.timeout(self.CLAIM_TIMEOUT_SECONDS)
        try:
            async with deadline:
                await self.session.flush()
        except TimeoutError as error:
            await self.session.rollback()
            if not deadline.expired():
                raise
            raise IdempotencyConflictError("相同请求正在处理中，请稍后重试") from error
        except IntegrityError:
            await self.session.rollback()
            existing = await self._find(user_id, action, idempotency_key)
            if existing is None:
                raise
            return self._response(existing, payload_hash)

        self._context = IdempotencyContext(record)
        self.session.info[IDEMPOTENCY_CONTEXT_KEY] = self._context
        return None

    @property
    def committed(self) -> bool:
        """本次业务和响应是否已通过统一提交入口持久化。"""
        return self._context is not None and self._context.committed

    async def release(self) -> None:
        """在有限时间内原子核对 token，旧请求不能删除接替者的锁。"""
        if self._lock_key is None or self._lock_token is None:
            return
        key, token = self._lock_key, self._lock_token
        self._lock_key = self._lock_token = None
        try:
            async with asyncio.timeout(self.REDIS_TIMEOUT_SECONDS):
                await cast(
                    Awaitable[Any],
                    self.redis.eval(  # type: ignore[no-untyped-call]
                        self.RELEASE_SCRIPT, 1, key, token
                    ),
                )
        except (RedisError, OSError, TimeoutError):
            pass

    def clear_context(self) -> None:
        """仅清理当前请求所拥有的会话上下文。"""
        if self.session.info.get(IDEMPOTENCY_CONTEXT_KEY) is self._context:
            self.session.info.pop(IDEMPOTENCY_CONTEXT_KEY, None)
        self._context = None
