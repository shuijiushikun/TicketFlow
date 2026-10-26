"""客户端重试只使用内存 HTTP 响应，不连接数据库或外部服务。"""

from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock

import httpx
import pytest

from src.clients import seckill
from src.clients.seckill import (
    RetryExhaustedError,
    SeckillAttempt,
    SeckillClient,
    SeckillWaitTimeout,
)

pytestmark = pytest.mark.asyncio


def receipt(status="QUEUED", request_id=12):
    return {
        "id": request_id,
        "ticket_type_id": 3,
        "status": status,
        "expires_at": (datetime.now(UTC) + timedelta(seconds=60)).isoformat(),
        "ticket_id": 25 if status == "SUCCEEDED" else None,
    }


@pytest.fixture
def waits(monkeypatch):
    sleeping = AsyncMock()
    monkeypatch.setattr(seckill, "sleep", sleeping)
    monkeypatch.setattr(seckill, "uniform", lambda low, high: high / 2)
    return sleeping


async def test_busy_and_lost_reply_reuse_key_and_payload_then_only_poll(waits):
    requests = []
    statuses = iter(["QUEUED", "SUCCEEDED"])

    def respond(request):
        requests.append(request)
        posts = sum(item.method == "POST" for item in requests)
        if request.method == "POST":
            if posts == 1:
                return httpx.Response(503, headers={"Retry-After": "3"})
            if posts == 2:
                raise httpx.ReadTimeout("提交回复丢失", request=request)
            return httpx.Response(202, json=receipt())
        return httpx.Response(200, json=receipt(next(statuses)))

    attempt = SeckillAttempt(ticket_type_id=3)
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(respond), base_url="http://test"
    ) as http:
        client = SeckillClient(http)
        accepted = await client.submit(attempt)
        result = await client.wait_result(accepted.id)

    assert result.status == "SUCCEEDED"
    assert result.ticket_id == 25
    assert [item.method for item in requests] == ["POST"] * 3 + ["GET"] * 2
    assert {item.headers["Idempotency-Key"] for item in requests[:3]} == {
        attempt.idempotency_key
    }
    assert len({item.content for item in requests[:3]}) == 1
    delays = [call.args[0] for call in waits.await_args_list]
    assert delays == [3.5, 3.0, 1.25]


async def test_retries_stop_after_three_extra_attempts(waits):
    requests = []

    def respond(request):
        requests.append(request)
        return httpx.Response(503, headers={"Retry-After": "1"})

    attempt = SeckillAttempt(3)
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(respond), base_url="http://test"
    ) as http:
        with pytest.raises(RetryExhaustedError):
            await SeckillClient(http).submit(attempt)
    assert len(requests) == 4
    assert waits.await_count == 3
    assert [call.args[0] for call in waits.await_args_list] == [1.5, 3.0, 6.0]


@pytest.mark.parametrize("code", [400, 401, 403, 404, 409, 422, 500])
async def test_permanent_error_does_not_retry(code, waits):
    requests = []

    def respond(request):
        requests.append(request)
        return httpx.Response(code, json={"detail": "请求内容冲突或不可用"})

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(respond), base_url="http://test"
    ) as http:
        with pytest.raises(httpx.HTTPStatusError):
            await SeckillClient(http).submit(SeckillAttempt(3))
    assert len(requests) == 1
    waits.assert_not_awaited()


@pytest.mark.parametrize(
    ("code", "detail"),
    [(429, "请求过于频繁"), (409, "相同请求正在处理中，请稍后重试")],
)
async def test_rate_limit_or_in_progress_conflict_can_retry(code, detail, waits):
    requests = []

    def respond(request):
        requests.append(request)
        if len(requests) == 1:
            return httpx.Response(code, json={"detail": detail})
        return httpx.Response(202, json=receipt())

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(respond), base_url="http://test"
    ) as http:
        assert (await SeckillClient(http).submit(SeckillAttempt(3))).id == 12
    assert len(requests) == 2
    assert waits.await_count == 1


@pytest.mark.parametrize("hint", ["86400", "Wed, 21 Oct 2099 07:28:00 GMT"])
async def test_retry_after_beyond_budget_does_not_retry_early(hint, waits):
    requests = []

    def respond(request):
        requests.append(request)
        return httpx.Response(503, headers={"Retry-After": hint})

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(respond), base_url="http://test"
    ) as http:
        with pytest.raises(RetryExhaustedError):
            await SeckillClient(http).submit(SeckillAttempt(3))
    assert len(requests) == 1
    waits.assert_not_awaited()


async def test_wait_timeout_retains_application_and_never_posts():
    requests = []

    def respond(request):
        requests.append(request)
        return httpx.Response(200, json=receipt())

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(respond), base_url="http://test"
    ) as http:
        with pytest.raises(SeckillWaitTimeout, match="12"):
            await SeckillClient(http).wait_result(12, timeout=0.01)
    assert len(requests) == 1
    assert requests[0].method == "GET"


async def test_cancellation_is_not_retried(waits):
    import asyncio

    def respond(request):
        raise asyncio.CancelledError()

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(respond), base_url="http://test"
    ) as http:
        with pytest.raises(asyncio.CancelledError):
            await SeckillClient(http).submit(SeckillAttempt(3))
    waits.assert_not_awaited()
