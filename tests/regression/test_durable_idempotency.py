"""持久化幂等的必要快速检查；不连接外部数据库或 Redis。"""

from typing import Any
from unittest.mock import AsyncMock

import pytest
from fakeredis.aioredis import FakeRedis
from pydantic import BaseModel
from redis.exceptions import ConnectionError
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.api.decorators import idempotent
from src.core.exception import IdempotencyConflictError
from src.models import User
from src.services.idempotency import IdempotencyService


class Creation(BaseModel):
    email: str


class Created(Creation):
    id: int


async def test_database_snapshot_survives_redis_loss_and_rejects_new_payload(
    session: AsyncSession,
) -> None:
    calls = 0

    @idempotent("local_create")
    async def create(
        owner: User,
        data: Creation,
        idempotency_service: IdempotencyService,
        idempotency_key: str,
    ) -> Created:
        from src.services.transactions import commit_business

        nonlocal calls
        calls += 1
        user = User(email=data.email, hashed_password="unused")
        session.add(user)
        await session.flush()
        response = Created(id=user.id, email=user.email)
        await commit_business(session, response)
        return response

    async with FakeRedis(decode_responses=True) as redis:
        kwargs: dict[str, Any] = {
            "owner": User(id=71, email="owner@example.com", hashed_password="unused"),
            "data": Creation(email="created@example.com"),
            "idempotency_service": IdempotencyService(redis, session),
            "idempotency_key": "retry-same-operation",
        }
        first = await create(**kwargs)
        await redis.flushdb()
        retry = await create(**kwargs)
        assert retry == first.model_dump(mode="json")
        assert calls == 1
        assert await session.scalar(select(func.count()).select_from(User)) == 1
        kwargs["data"] = Creation(email="different@example.com")
        with pytest.raises(IdempotencyConflictError, match="其他请求内容"):
            await create(**kwargs)
        assert calls == 1


async def test_business_failure_rolls_back_claim_even_when_redis_is_unavailable(
    session: AsyncSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    from src.models.idempotency import IdempotencyRequest
    from src.services.transactions import IDEMPOTENCY_CONTEXT_KEY, commit_business

    fail = True

    @idempotent("local_rollback")
    async def create(
        owner: User,
        idempotency_service: IdempotencyService,
        idempotency_key: str,
    ) -> Created:
        user = User(email="rollback@example.com", hashed_password="unused")
        session.add(user)
        await session.flush()
        if fail:
            raise RuntimeError("business failed")
        result = Created(id=user.id, email=user.email)
        await commit_business(session, result)
        return result

    async with FakeRedis(decode_responses=True) as redis:
        monkeypatch.setattr(redis, "set", AsyncMock(side_effect=ConnectionError()))
        kwargs: dict[str, Any] = {
            "owner": User(id=72, email="owner@example.com", hashed_password="unused"),
            "idempotency_service": IdempotencyService(redis, session),
            "idempotency_key": "retry-after-failure",
        }
        with pytest.raises(RuntimeError, match="business failed"):
            await create(**kwargs)
        assert IDEMPOTENCY_CONTEXT_KEY not in session.info
        assert await session.scalar(select(func.count()).select_from(User)) == 0
        assert (
            await session.scalar(select(func.count()).select_from(IdempotencyRequest))
            == 0
        )
        fail = False
        await create(**kwargs)
        assert (
            await session.scalar(select(func.count()).select_from(IdempotencyRequest))
            == 1
        )


async def test_expired_holder_cannot_release_replacement_lock(
    session: AsyncSession,
) -> None:
    async with FakeRedis(decode_responses=True) as redis:
        service = IdempotencyService(redis, session)
        assert await service.check_and_lock(73, "local_lock", "holder-change") is None
        key = service._generate_key(73, "local_lock", "holder-change")
        await redis.set(key, "replacement-owner", px=5000)
        await service.release()
        assert await redis.get(key) == "replacement-owner"
        await session.rollback()
        service.clear_context()
