"""仅用内存 ASGI 与准入实例验证指标，不访问外部数据库或 Redis。"""

import asyncio
import json
import logging
from uuid import UUID

import httpx
import pytest

from src.api.observability import router
from src.core.cache_policy import CacheUnavailableError, DatabaseAdmission
from src.core.logger import JsonFormatter
from src.core.metrics import registry
from src.core.request_context import request_id_context
from src.main import ObservedFastAPI


def sample(name: str, **labels: str) -> float:
    return registry.get_sample_value(name, labels) or 0.0


async def test_http_context_templates_errors_and_metrics_exclusion() -> None:
    app = ObservedFastAPI()
    app.include_router(router)

    @app.get("/observability-test/{item_id}")
    async def item(item_id: int) -> dict[str, str | None]:
        return {"request_id": request_id_context.get()}

    @app.get("/observability-failure")
    async def failure() -> None:
        raise RuntimeError("用于验证 500 外层中间件")

    before = sample(
        "ticketcore_http_requests_total",
        method="GET",
        route="/observability-test/{item_id}",
        status="200",
    )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app, raise_app_exceptions=False),
        base_url="http://test",
    ) as client:
        responses = [
            await client.get(
                f"/observability-test/{i}", headers={"X-Request-ID": "client-supplied"}
            )
            for i in (1, 2)
        ]
        for response in responses:
            request_id = response.headers["X-Request-ID"]
            assert str(UUID(request_id)) == request_id
            assert response.json()["request_id"] == request_id
        first_id, second_id = [r.headers["X-Request-ID"] for r in responses]
        assert first_id != second_id
        failed = await client.get("/observability-failure")
        assert failed.status_code == 500
        assert UUID(failed.headers["X-Request-ID"])
        assert (await client.get("/unknown-observability-path")).status_code == 404
        exported = await client.get("/metrics")
        assert exported.status_code == 200
        assert "ticketcore_http_requests_total" in exported.text
        assert 'route="/observability-test/1"' not in exported.text
        assert 'route="/unknown-observability-path"' not in exported.text
        assert 'route="/metrics"' not in exported.text
    assert request_id_context.get() is None
    assert (
        sample(
            "ticketcore_http_requests_total",
            method="GET",
            route="/observability-test/{item_id}",
            status="200",
        )
        == before + 2
    )
    assert (
        sample(
            "ticketcore_http_requests_total",
            method="GET",
            route="__unmatched__",
            status="404",
        )
        >= 1
    )


def test_json_log_uses_context_request_id() -> None:
    token = request_id_context.set("服务端请求标识")
    try:
        record = logging.LogRecord("test", logging.INFO, __file__, 1, "测试", (), None)
        record.extra_data = {"request_id": "不能覆盖上下文"}
        formatted = json.loads(JsonFormatter().format(record))
        assert formatted["request_id"] == "服务端请求标识"
    finally:
        request_id_context.reset(token)


async def test_admission_metrics_release_after_rejection_and_cancellation() -> None:
    admission = DatabaseAdmission(1, 1, 1, 2, budget="warmup")
    active = sample("ticketcore_db_active", budget="warmup")
    queued = sample("ticketcore_db_queued", budget="warmup")
    started, release = asyncio.Event(), asyncio.Event()

    async def hold() -> None:
        async with admission.slot():
            started.set()
            await release.wait()

    first = asyncio.create_task(hold())
    await asyncio.wait_for(started.wait(), 1)
    second = asyncio.create_task(hold())
    await asyncio.sleep(0)
    try:
        assert sample("ticketcore_db_active", budget="warmup") == active + 1
        assert sample("ticketcore_db_queued", budget="warmup") == queued + 1
        with pytest.raises(CacheUnavailableError):
            async with admission.slot():
                pytest.fail("已满的预算不能继续接纳请求")
        second.cancel()
        with pytest.raises(asyncio.CancelledError):
            await second
    finally:
        second.cancel()
        release.set()
        await asyncio.gather(first, second, return_exceptions=True)
    assert sample("ticketcore_db_active", budget="warmup") == active
    assert sample("ticketcore_db_queued", budget="warmup") == queued
    for result in ("accepted", "rejected", "cancelled"):
        count = sample("ticketcore_db_admission_total", budget="warmup", result=result)
        assert count >= 1


async def test_query_timeout_is_counted_and_returns_its_slot() -> None:
    admission = DatabaseAdmission(1, 0, 0.1, 0.01)
    active = sample("ticketcore_db_active", budget="query")
    timed_out = sample(
        "ticketcore_db_query_seconds_count", budget="query", result="timeout"
    )
    with pytest.raises(CacheUnavailableError, match="数据库查询超时"):
        async with admission.slot():
            await asyncio.sleep(1)
    assert sample("ticketcore_db_active", budget="query") == active
    assert (
        sample("ticketcore_db_query_seconds_count", budget="query", result="timeout")
        == timed_out + 1
    )
