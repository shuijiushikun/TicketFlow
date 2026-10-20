from typing import Any

from pydantic import BaseModel


class IdempotencyRecord(BaseModel):
    """Redis 中的幂等记录，包含处理状态、请求内容摘要和已完成的接口响应。"""

    status: str
    response: dict[str, Any] | None = None
    payload_hash: str | None = None
