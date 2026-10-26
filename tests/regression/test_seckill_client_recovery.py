"""验证客户端在提交回复丢失后复用真实的持久幂等记录。"""

from unittest.mock import AsyncMock

import httpx
import pytest
from sqlalchemy import func, select

from src.clients import seckill
from src.clients.seckill import SeckillAttempt, SeckillClient
from src.core.config import settings
from src.models.idempotency import IdempotencyRequest
from src.models.seckill import SeckillRequest

pytestmark = pytest.mark.asyncio


async def test_lost_committed_receipt_reuses_one_application(
    lifecycle_client, session, ticket, monkeypatch
):
    monkeypatch.setattr(settings, "SECKILL_ENABLED", True)
    monkeypatch.setattr(seckill, "sleep", AsyncMock())
    ticket.ticket_type.is_seckill = True
    await session.commit()
    attempts = []

    async def relay(request):
        attempts.append(request)
        response = await lifecycle_client.request(
            request.method,
            request.url.path,
            content=request.content,
            headers=request.headers,
        )
        if len(attempts) == 1:
            assert response.status_code == 202, response.text
            # 数据库已保存申请，但调用方在收到回复前断线。
            raise httpx.ReadTimeout("受理回复丢失", request=request)
        return response

    attempt = SeckillAttempt(ticket.ticket_type_id)
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(relay), base_url="http://test"
    ) as http:
        receipt = await SeckillClient(http).submit(attempt)

    assert receipt.id > 0
    assert len(attempts) == 2
    assert {request.headers["Idempotency-Key"] for request in attempts} == {
        attempt.idempotency_key
    }
    assert await session.scalar(select(func.count(SeckillRequest.id))) == 1
    assert await session.scalar(select(func.count(IdempotencyRequest.id))) == 1
    assert (await session.get(SeckillRequest, receipt.id)).owner_id == ticket.owner_id
