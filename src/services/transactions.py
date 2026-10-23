from dataclasses import dataclass

from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.exception import IdempotencyStateError
from src.models.idempotency import IdempotencyRequest

IDEMPOTENCY_CONTEXT_KEY = "ticketflow.idempotency_context"


@dataclass
class IdempotencyContext:
    """当前请求在同一数据库会话中的占用记录与提交结果。"""

    record: IdempotencyRequest
    committed: bool = False


async def commit_business(
    session: AsyncSession, response: BaseModel | None = None
) -> None:
    """原子提交业务变更与本次幂等请求的 JSON 响应快照。"""
    context = session.info.get(IDEMPOTENCY_CONTEXT_KEY)
    if isinstance(context, IdempotencyContext):
        if response is None:
            raise IdempotencyStateError("幂等业务提交时必须提供响应快照")
        context.record.response = response.model_dump(mode="json")
    await session.commit()
    if isinstance(context, IdempotencyContext):
        context.committed = True
