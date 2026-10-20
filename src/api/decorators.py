from collections.abc import Callable
from functools import wraps
from typing import Any, TypeVar, cast

from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel

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
    """通过 Redis 临时锁和请求内容摘要，处理短时间内的重复请求。

    action 区分业务操作，key_param_name 指定接口中的幂等键参数名。
    相同用户、操作和幂等键在记录有效期内复用已完成的响应；内容不同或仍在处理中时拒绝请求。
    没有提供幂等键时直接执行业务。记录过期后不再提供去重，不能替代持久化业务约束。
    """

    def decorator(func: F) -> F:
        @wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            idempotency_key = kwargs.get(key_param_name)

            user = next((v for v in kwargs.values() if isinstance(v, User)), None)
            service = next(
                (v for v in kwargs.values() if isinstance(v, IdempotencyService)), None
            )

            if not idempotency_key or not user or not service:
                return await func(*args, **kwargs)

            payload = _extract_business_payload(kwargs, key_param_name)

            cached_response = await service.check_and_lock(
                user_id=user.id,
                action=action,
                idempotency_key=idempotency_key,
                payload=payload,
            )

            if cached_response is not None:
                return cached_response

            try:
                response = await func(*args, **kwargs)
                response_data = jsonable_encoder(response)

                await service.save_response(
                    user_id=user.id,
                    action=action,
                    idempotency_key=idempotency_key,
                    response_data=response_data,
                    payload=payload,
                )

                return response
            except Exception:
                await service.unlock(
                    user_id=user.id, action=action, idempotency_key=idempotency_key
                )
                raise

        return cast(F, wrapper)

    return decorator
