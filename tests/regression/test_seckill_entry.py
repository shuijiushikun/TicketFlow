"""只验证秒杀入口资源预算，不访问外部数据库或 Redis。"""

import asyncio
from contextlib import asynccontextmanager, suppress
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from src.core.cache_policy import CacheUnavailableError
from src.core.seckill_policy import SubmissionAdmission

pytestmark = pytest.mark.asyncio


async def test_buffer_bounds_waiters_and_recovers_after_cancellation():
    gate = SubmissionAdmission(1, 1, 1)

    async def enter():
        async with gate.slot():
            return "allowed"

    async with gate.slot():
        waiting = asyncio.create_task(enter())
        await asyncio.sleep(0)
        try:
            with pytest.raises(CacheUnavailableError, match="入口繁忙"):
                await enter()
        finally:
            waiting.cancel()
            with suppress(asyncio.CancelledError):
                await waiting
        replacement = asyncio.create_task(enter())
        await asyncio.sleep(0)
    assert await replacement == "allowed"
    assert await enter() == "allowed"


async def test_wait_timeout_and_body_failure_return_capacity():
    gate = SubmissionAdmission(1, 1, 0.01)
    async with gate.slot():
        with pytest.raises(CacheUnavailableError, match="入口等待超时"):
            async with gate.slot():
                pytest.fail("持有者还未释放，不应开始接收")
    with pytest.raises(ValueError, match="business failed"):
        async with gate.slot():
            raise ValueError("business failed")
    async with gate.slot():
        pass


async def test_cancel_after_handoff_does_not_leak_a_permit():
    gate = SubmissionAdmission(1, 1, 1)

    async def enter():
        async with gate.slot():
            pytest.fail("交接后尚未恢复的等待者应被取消")

    async with gate.slot():
        waiting = asyncio.create_task(enter())
        await asyncio.sleep(0)
    # 释放已经唤醒等待者，但此时还没有把执行权交回它。
    waiting.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiting
    async with gate.slot():
        pass


@pytest.mark.parametrize("broken", [False, True])
async def test_pool_warmup_holds_distinct_connections_and_cleans_failure(
    monkeypatch, broken
):
    from src.db.session import warmup_connections

    active = peak = created = 0

    @asynccontextmanager
    async def connect():
        nonlocal active, peak, created
        created += 1
        active += 1
        peak = max(peak, active)
        try:
            connection = AsyncMock()
            if broken and created == 1:
                connection.execute.side_effect = RuntimeError("unavailable")
            yield connection
        finally:
            active -= 1

    monkeypatch.setattr("src.db.session.engine", SimpleNamespace(connect=connect))
    if broken:
        with pytest.raises(ExceptionGroup, match="TaskGroup"):
            await warmup_connections(3)
    else:
        await warmup_connections(3)
        assert peak == 3
    assert active == 0
