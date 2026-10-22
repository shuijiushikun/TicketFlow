from __future__ import annotations

from typing import Annotated

import jwt
from arq import ArqRedis
from fastapi import Depends, Header, HTTPException, Request, status
from fastapi.security import OAuth2PasswordBearer
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncSession

from src.core import settings
from src.core.cache_runtime import event_query_limiter
from src.core.logger import logger
from src.core.redis_keys import RedisClient
from src.db.redis import get_arq_pool, get_cache_redis, get_redis
from src.db.session import get_db
from src.models import User
from src.repositories.event import CachedEventRepository, EventRepository
from src.schemas.token import TokenPayload
from src.services.auth import AuthService
from src.services.event import EventService
from src.services.health import HealthService
from src.services.hotspot import HotspotTracker
from src.services.idempotency import IdempotencyService
from src.services.inventory_query import InventoryQueryService
from src.services.payment import PaymentService
from src.services.ticket import TicketService
from src.services.ticket_type import TicketTypeService

reusable_oauth2 = OAuth2PasswordBearer(tokenUrl=f"{settings.API_V1_STR}/auth/login")

DBDep = Annotated[AsyncSession, Depends(get_db)]
IdempotencyHeader = Annotated[
    str | None,
    Header(
        alias="Idempotency-Key",
        min_length=10,
        max_length=100,
        description=(
            "可选的请求幂等键。同一次操作重试时沿用原值，新操作使用新值。"
            "相同用户、操作和键复用数据库持久化的原始响应；内容不同返回 409。"
            "未提供时不保证请求重试去重；响应快照不代表票据的最新状态。"
        ),
    ),
]


async def get_current_user(
    token: Annotated[str, Depends(reusable_oauth2)], session: DBDep
) -> User:
    """验证 JWT 登录凭证并查询当前用户。

    从 Bearer token 中读取用户 ID（sub），校验签名和有效期，再从数据库获取 User。
    凭证无效、过期或用户不存在时返回 401。
    """
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="登录凭证无效或已过期，请重新登录",
        headers={"WWW-Authenticate": "Bearer"},
    )

    try:
        jwt_payload = jwt.decode(
            token, settings.SECRET_KEY, algorithms=[settings.ALGORITHM]
        )

        token_data = TokenPayload(**jwt_payload)
        if not token_data.sub:
            logger.info('身份验证失败：令牌缺少 "sub" 字段')
            raise credentials_exception

    except jwt.ExpiredSignatureError as e:
        logger.info("身份验证失败：令牌已过期")
        raise credentials_exception from e
    except (jwt.PyJWTError, ValidationError) as e:
        logger.warning(f"身份验证失败：令牌解析异常：{e}")
        raise credentials_exception from e

    try:
        user_id = int(token_data.sub)
    except ValueError as e:
        logger.warning(f"身份验证失败：用户 ID 不是整数：{token_data.sub}")
        raise credentials_exception from e

    user = await session.get(User, user_id)

    if not user:
        logger.warning(f"身份验证失败：数据库中不存在用户 {user_id}")
        raise credentials_exception

    return user


async def get_current_superuser(current_user: User = Depends(get_current_user)) -> User:
    """检查当前用户是否拥有管理员权限。

    返回管理员用户对象；普通用户访问管理接口时返回 403。
    """
    if not current_user.is_superuser:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, detail="当前用户没有管理员权限"
        )
    return current_user


async def get_ticket_service(
    session: DBDep,
    arq_pool: ArqRedis = Depends(get_arq_pool),
    redis: RedisClient = Depends(get_redis),
) -> TicketService:
    """为接口准备票据服务，并注入数据库会话、任务队列和业务 Redis 连接。"""
    return TicketService(session=session, arq_pool=arq_pool, redis=redis)


async def get_event_service(
    session: DBDep, redis: RedisClient = Depends(get_cache_redis)
) -> EventService:
    """为接口准备活动服务，组合数据库仓库与 Redis 缓存仓库。"""
    base_repo = EventRepository(session)
    cached_repo = CachedEventRepository(repository=base_repo, redis=redis)
    return EventService(session=session, base_repo=base_repo, cached_repo=cached_repo)


async def limit_event_queries(request: Request) -> None:
    """限制公开活动查询；使用连接地址，不自行信任客户端转发头。"""
    client = request.client.host if request.client else "unknown"
    event_query_limiter.check(client)


async def get_hotspot_tracker(
    redis: RedisClient = Depends(get_cache_redis),
) -> HotspotTracker:
    """访问统计复用有限查询连接池，使用独立故障冷却保护查询链路。"""
    return HotspotTracker(redis)


async def get_auth_service(session: DBDep) -> AuthService:
    """为接口准备身份认证服务，并注入数据库会话。"""
    return AuthService(session)


async def get_ticket_type_service(
    session: DBDep,
    redis: RedisClient = Depends(get_redis),
    cache_redis: RedisClient = Depends(get_cache_redis),
) -> TicketTypeService:
    """为接口准备票种服务，并注入数据库会话和 Redis 连接。"""
    return TicketTypeService(session=session, redis=redis, cache_redis=cache_redis)


async def get_idempotency_service(
    session: DBDep,
    redis: RedisClient = Depends(get_redis),
) -> IdempotencyService:
    """复用业务数据库会话；Redis 只承担快速互斥。"""
    return IdempotencyService(redis, session)


async def get_inventory_query_service(
    session: DBDep, redis: RedisClient = Depends(get_cache_redis)
) -> InventoryQueryService:
    """余票展示使用独立读服务，不参与交易判断。"""
    return InventoryQueryService(session, redis)


async def get_payment_service(
    session: DBDep, redis: RedisClient = Depends(get_redis)
) -> PaymentService:
    """为接口准备模拟支付服务，并注入数据库会话和 Redis 连接。"""
    return PaymentService(session=session, redis=redis)


async def get_health_service(
    session: DBDep, redis: RedisClient = Depends(get_redis)
) -> HealthService:
    """为接口准备依赖检查服务，检查数据库和 Redis 是否可用。"""
    return HealthService(session=session, redis=redis)


TicketServiceDep = Annotated[TicketService, Depends(get_ticket_service)]
EventServiceDep = Annotated[EventService, Depends(get_event_service)]
AuthServiceDep = Annotated[AuthService, Depends(get_auth_service)]
TicketTypeServiceDep = Annotated[TicketTypeService, Depends(get_ticket_type_service)]
InventoryQueryServiceDep = Annotated[
    InventoryQueryService, Depends(get_inventory_query_service)
]
IdempotencyServiceDep = Annotated[IdempotencyService, Depends(get_idempotency_service)]
PaymentServiceDep = Annotated[PaymentService, Depends(get_payment_service)]
HealthServiceDep = Annotated[HealthService, Depends(get_health_service)]
