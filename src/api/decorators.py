from collections.abc import Callable
from functools import wraps
from typing import Any, TypeVar, cast

from pydantic import BaseModel

from src.core.exception import IdempotencyStateError
from src.models import User
from src.services.idempotency import IdempotencyService

F = TypeVar("F", bound=Callable[..., Any])


def _extract_business_payload(
    kwargs: dict[str, Any], key_name: str
) -> dict[str, Any] | None:
    """提取业务参数，用于计算请求内容的摘要。

    只保留请求体模型及路径、查询参数中的基本类型，忽略数据库会话、服务实例和用户对象。
    kwargs 是接口收到的参数；key_name 指定需要排除的幂等键参数名。
    返回业务参数字典；没有业务参数时返回 None。
    """
    payload: dict[str, Any] = {}
    for key, value in kwargs.items():
        if key == key_name:
            continue

        if isinstance(value, BaseModel):
            payload[key] = value.model_dump(mode="json")
        elif isinstance(value, (int, str, float, bool)):
            payload[key] = value

    return payload if payload else None


def idempotent(
    action: str, key_param_name: str = "idempotency_key"
) -> Callable[[F], F]:
    """在同一数据库事务中保存业务结果，并复用同一次请求的原响应。

    action 区分业务操作，key_param_name 指定接口中的幂等键参数名。
    相同用户、操作和幂等键永久复用已提交响应；内容不同或仍在处理中时拒绝请求。
    没有提供幂等键时直接执行业务。
    """

    def decorator(func: F) -> F:
        @wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            idempotency_key = kwargs.get(key_param_name)

            user = next((v for v in kwargs.values() if isinstance(v, User)), None)
            service = next(
                (v for v in kwargs.values() if isinstance(v, IdempotencyService)), None
            )

            if not idempotency_key:
                return await func(*args, **kwargs)
            if user is None or service is None:
                raise IdempotencyStateError("幂等接口缺少用户或数据库事务服务")

            payload = _extract_business_payload(kwargs, key_param_name)

            try:
                cached_response = await service.check_and_lock(
                    user_id=user.id,
                    action=action,
                    idempotency_key=idempotency_key,
                    payload=payload,
                )
                if cached_response is not None:
                    return cached_response
                response = await func(*args, **kwargs)
                if not service.committed:
                    raise IdempotencyStateError("幂等业务尚未提交响应快照")
                return response
            except BaseException:
                # 包含取消；提交后的 rollback 不会撤销已提交业务或删除持久化记录。
                await service.session.rollback()
                raise
            finally:
                try:
                    await service.release()
                finally:
                    service.clear_context()

        return cast(F, wrapper)

    return decorator
