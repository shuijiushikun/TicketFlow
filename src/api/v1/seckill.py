"""提交异步抢票申请及查询本人结果。"""

from collections.abc import AsyncIterator
from typing import Annotated

from fastapi import APIRouter, Depends, Header, Path, Request, Response, status

from src.api.decorators import idempotent
from src.api.deps import IdempotencyServiceDep, SeckillServiceDep, get_current_user
from src.core.cache_policy import CacheUnavailableError
from src.core.config import settings
from src.core.seckill_policy import (
    POLL_LIMITER,
    SUBMIT_ADMISSION,
    SUBMIT_LIMITER,
    submission_stage,
)
from src.models import User
from src.schemas.seckill import SeckillCreate, SeckillResponse

router = APIRouter()
RequiredKey = Annotated[
    str, Header(alias="Idempotency-Key", min_length=10, max_length=100)
]


async def limit_submission(request: Request) -> AsyncIterator[None]:
    """先限制接收并发再查库；总开关只影响接收，不隐藏已有结果。"""
    if not settings.SECKILL_ENABLED:
        raise CacheUnavailableError("秒杀入口尚未开启")
    SUBMIT_LIMITER.check(request.client.host if request.client else "unknown")
    async with SUBMIT_ADMISSION.slot():
        with submission_stage("acceptance"):
            yield


async def limit_polling(request: Request) -> None:
    """限制查询频率，客户端建议每秒查询一次。"""
    POLL_LIMITER.check(request.client.host if request.client else "unknown")


@idempotent(action="seckill_apply")
async def _submit(
    *,
    owner: User,
    ticket: SeckillCreate,
    seckill_service: SeckillServiceDep,
    idempotency_service: IdempotencyServiceDep,
    idempotency_key: str,
) -> SeckillResponse:
    return await seckill_service.submit(owner.id, ticket)


@router.post(
    "/requests",
    response_model=SeckillResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="提交秒杀申请",
    dependencies=[Depends(limit_submission)],
)
async def submit_request(
    ticket: SeckillCreate,
    owner: Annotated[User, Depends(get_current_user)],
    seckill_service: SeckillServiceDep,
    idempotency_service: IdempotencyServiceDep,
    idempotency_key: RequiredKey,
    response: Response,
) -> SeckillResponse:
    """保存申请后返回 202；同次重试沿用幂等键，成功与否通过查询接口获取。"""
    result = await _submit(
        owner=owner,
        ticket=ticket,
        seckill_service=seckill_service,
        idempotency_service=idempotency_service,
        idempotency_key=idempotency_key,
    )
    # 幂等重放也统一设置响应头；缓存的提交凭证不代表最新处理结果。
    result = SeckillResponse.model_validate(result)
    response.headers["Location"] = f"{settings.API_V1_STR}/seckill/requests/{result.id}"
    response.headers["Cache-Control"] = "no-store"
    response.headers["Retry-After"] = "1"
    return result


@router.get(
    "/requests/{request_id}",
    response_model=SeckillResponse,
    summary="查询本人秒杀结果",
    dependencies=[Depends(limit_polling)],
)
async def get_request(
    request_id: Annotated[int, Path(gt=0)],
    owner: Annotated[User, Depends(get_current_user)],
    seckill_service: SeckillServiceDep,
    response: Response,
) -> SeckillResponse:
    """返回最新状态；SUCCEEDED 时携带票据 ID，沿用原有支付和取消接口。"""
    response.headers["Cache-Control"] = "no-store"
    response.headers["Retry-After"] = "1"
    return await seckill_service.get(owner.id, request_id)
