"""秒杀客户端：同键有限重试，受理后查询结果，等待结束不取消申请。"""

import asyncio
from asyncio import sleep
from dataclasses import dataclass, field
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from math import isfinite
from random import uniform
from time import monotonic
from uuid import uuid4

import httpx

from src.models.seckill import SeckillStatus
from src.schemas.seckill import SeckillCreate, SeckillResponse

MAX_RETRIES = 3
REQUEST_TIMEOUT = 5.0
RETRY_BUDGET = 30.0
RETRYABLE_STATUS = {429, 502, 503, 504}
IN_PROGRESS_DETAIL = "相同请求正在处理中，请稍后重试"


@dataclass(frozen=True)
class SeckillAttempt:
    """一次购票意图；重复调用 submit 时复用本对象或原来的幂等键。"""

    ticket_type_id: int
    idempotency_key: str = field(default_factory=lambda: uuid4().hex)

    def __post_init__(self) -> None:
        """在首次发送前校验票种与幂等键。"""
        SeckillCreate(ticket_type_id=self.ticket_type_id)
        if not 10 <= len(self.idempotency_key) <= 100:
            raise ValueError("幂等键长度必须为 10～100 个字符")


class RetryExhaustedError(RuntimeError):
    """暂时故障仍未恢复；保留原幂等键或申请编号供后续继续操作。"""


class SeckillWaitTimeout(TimeoutError):
    """客户端停止等待，后台申请仍然有效，可通过原编号继续查询。"""


def _retry_after(response: httpx.Response | None) -> float:
    """解析秒数或 HTTP 日期；无效提示按本地退避策略处理。"""
    if response is None or not (value := response.headers.get("Retry-After")):
        return 0.0
    try:
        if value.strip().isdigit():
            return float(value)
        date: datetime = parsedate_to_datetime(value)
        if date.tzinfo is None:
            date = date.replace(tzinfo=UTC)
        return max(0.0, (date - datetime.now(UTC)).total_seconds())
    except (ValueError, TypeError, OverflowError):
        return 0.0


def _retryable(response: httpx.Response) -> bool:
    """仅对暂时故障重试，幂等内容冲突和身份错误交给调用方处理。"""
    if response.status_code in RETRYABLE_STATUS:
        return True
    if response.status_code == 409:
        try:
            body = response.json()
        except ValueError:
            return False
        return isinstance(body, dict) and body.get("detail") == IN_PROGRESS_DETAIL
    return False


class SeckillClient:
    """复用调用方的 HTTPX 会话；认证与会话关闭仍由调用方负责。

    初次请求加最多 3 次重试，每次最多 5 秒，总重试预算 30 秒。
    服务端等待提示超过剩余预算时停止；不会缩短提示时间提前重试。
    """

    def __init__(self, http: httpx.AsyncClient, api_prefix: str = "/api/v1") -> None:
        self.http = http
        self.path = f"{api_prefix.rstrip('/')}/seckill/requests"

    async def _request(
        self,
        method: str,
        path: str,
        expected: int,
        *,
        json: dict[str, int] | None = None,
        headers: dict[str, str] | None = None,
    ) -> httpx.Response:
        deadline = monotonic() + RETRY_BUDGET
        last_error: Exception | None = None
        for retry in range(MAX_RETRIES + 1):
            response = None
            remaining = deadline - monotonic()
            if remaining <= 0:
                break
            try:
                async with asyncio.timeout(min(REQUEST_TIMEOUT, remaining)):
                    response = await self.http.request(
                        method, path, json=json, headers=headers
                    )
            except (
                httpx.TimeoutException,
                httpx.NetworkError,
                httpx.RemoteProtocolError,
                TimeoutError,
            ) as error:
                last_error = error
            else:
                if response.status_code == expected:
                    return response
                last_error = httpx.HTTPStatusError(
                    f"秒杀接口返回 HTTP {response.status_code}",
                    request=response.request,
                    response=response,
                )
                if not _retryable(response):
                    raise last_error
            if retry == MAX_RETRIES:
                break
            base = float(2**retry)
            delay = max(base, _retry_after(response)) + uniform(0, base)
            if monotonic() + delay >= deadline:
                break
            await sleep(delay)
        raise RetryExhaustedError(
            "重试次数或等待预算已用尽，请保留原幂等键或申请编号后继续操作"
        ) from last_error

    async def submit(self, attempt: SeckillAttempt) -> SeckillResponse:
        """同一申请的所有提交沿用相同键和请求体；返回原始受理凭证。"""
        response = await self._request(
            "POST",
            self.path,
            202,
            json={"ticket_type_id": attempt.ticket_type_id},
            headers={"Idempotency-Key": attempt.idempotency_key},
        )
        receipt = SeckillResponse.model_validate(response.json())
        if receipt.ticket_type_id != attempt.ticket_type_id:
            raise ValueError("返回凭证的票种与申请不一致，请保留原幂等键核对")
        return receipt

    async def wait_result(
        self, request_id: int, *, timeout: float = 70.0
    ) -> SeckillResponse:
        """只查询原编号，默认最多等待 70 秒；查询故障同样有限重试。"""
        if request_id <= 0 or not isfinite(timeout) or timeout <= 0:
            raise ValueError("申请编号和等待时间必须为正数")
        try:
            async with asyncio.timeout(timeout):
                while True:
                    response = await self._request(
                        "GET", f"{self.path}/{request_id}", 200
                    )
                    result = SeckillResponse.model_validate(response.json())
                    if result.id != request_id:
                        raise ValueError("查询结果编号与原申请不一致")
                    if result.status != SeckillStatus.QUEUED:
                        return result
                    await sleep(max(1.0, _retry_after(response)) + uniform(0, 0.5))
        except TimeoutError as error:
            raise SeckillWaitTimeout(
                f"客户端等待结束，申请 {request_id} 仍可通过原编号继续查询"
            ) from error
