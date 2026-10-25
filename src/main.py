from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, status
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.exc import TimeoutError as DatabasePoolTimeout
from starlette.responses import JSONResponse
from starlette.types import ASGIApp

from src.api import health, observability
from src.api.v1 import analytics, auth, events, payment, seckill, ticket_type, tickets
from src.core import logger
from src.core.cache_policy import CacheUnavailableError, QueryRateLimitError
from src.core.config import settings
from src.core.exception import (
    BookingModeError,
    EmptyUpdateDataError,
    EventDeleteError,
    EventNotFoundError,
    HealthError,
    IdempotencyConflictError,
    IdempotencyStateError,
    InactiveUserError,
    InvalidCredentialsError,
    SeckillNotFoundError,
    TicketAlreadyPaidError,
    TicketNotFoundError,
    TicketReservationExpireError,
    TicketsSoldOutError,
    TicketTypeDeleteError,
    TicketTypeNotFoundError,
    TicketTypeQuantity,
    UserAlreadyExistsError,
)
from src.core.request_context import RequestContextMiddleware
from src.db.redis import close_redis_pool, init_redis_pool


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """管理应用启动和关闭过程。

    启动时初始化 Redis 连接，关闭时释放连接资源。
    """
    logger.info("正在初始化 Redis 连接池……")
    await init_redis_pool()

    yield

    logger.info("正在关闭 Redis 连接池……")
    await close_redis_pool()


class ObservedFastAPI(FastAPI):
    """将请求上下文放在错误响应外层，让未处理异常也带追踪 ID。"""

    def build_middleware_stack(self) -> ASGIApp:
        """保留 FastAPI 原异常处理顺序，再包装原生 ASGI 观测层。"""
        return RequestContextMiddleware(super().build_middleware_stack())


app = ObservedFastAPI(
    title=f"{settings.PROJECT_NAME} 活动票务预约后端",
    version=settings.PROJECT_VERSION,
    description=(
        "活动票务后端，支持注册登录、活动与票种管理、预订、模拟支付和超时回收。\n\n"
        "先注册账号，再点击 Authorize（授权），使用邮箱作为 username 登录。"
        "创建活动和票种需要管理员权限；预订、支付和取消操作使用当前登录用户。\n\n"
        "票据状态：RESERVED（已预留）、SOLD（已售出）、CANCELED（已取消）。"
        "票种价格以分为单位；tickets_sold 表示预留与已售出共同占用的数量。"
    ),
    openapi_tags=[
        {
            "name": "系统检查",
            "description": "查看应用状态，以及 PostgreSQL 和 Redis 是否就绪。",
        },
        {"name": "用户认证", "description": "注册普通用户、登录获取令牌和修改密码。"},
        {"name": "活动管理", "description": "浏览活动；管理员可创建、修改和删除活动。"},
        {
            "name": "票种管理",
            "description": "查询票种与库存占用；管理员可维护价格和总票数。",
        },
        {
            "name": "票据管理",
            "description": "预订一张票、查询自己的票据和取消未付款预订。",
        },
        {
            "name": "异步秒杀",
            "description": "提交秒杀申请并查询结果；抢到后沿用原有支付和取消。",
        },
        {
            "name": "模拟支付",
            "description": "将有效预订标记为已售出，不调用真实支付渠道。",
        },
    ],
    openapi_url=f"{settings.API_V1_STR}/openapi.json",
    lifespan=lifespan,
)

app.include_router(observability.router)
app.include_router(health.router, tags=["系统检查"])
app.include_router(
    analytics.router, prefix=f"{settings.API_V1_STR}/analytics", tags=["热点分析"]
)

app.include_router(auth.router, prefix=f"{settings.API_V1_STR}/auth", tags=["用户认证"])
app.include_router(
    tickets.router, prefix=f"{settings.API_V1_STR}/tickets", tags=["票据管理"]
)
app.include_router(
    payment.router, prefix=f"{settings.API_V1_STR}/tickets", tags=["模拟支付"]
)
app.include_router(
    ticket_type.router,
    prefix=f"{settings.API_V1_STR}/ticket-types",
    tags=["票种管理"],
)
app.include_router(
    events.router, prefix=f"{settings.API_V1_STR}/events", tags=["活动管理"]
)
app.include_router(
    seckill.router, prefix=f"{settings.API_V1_STR}/seckill", tags=["异步秒杀"]
)


@app.exception_handler(InvalidCredentialsError)
@app.exception_handler(InactiveUserError)
async def auth_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """将身份认证失败统一转换为 401，并保留 Bearer 认证响应头。"""
    return JSONResponse(
        status_code=status.HTTP_401_UNAUTHORIZED,
        content={"detail": str(exc)},
        headers={"WWW-Authenticate": "Bearer"},
    )


@app.exception_handler(TicketTypeNotFoundError)
@app.exception_handler(TicketNotFoundError)
@app.exception_handler(EventNotFoundError)
@app.exception_handler(SeckillNotFoundError)
async def not_found_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """将活动、票种或票据不存在等异常统一转换为 404。"""
    return JSONResponse(
        status_code=status.HTTP_404_NOT_FOUND, content={"detail": str(exc)}
    )


@app.exception_handler(TicketAlreadyPaidError)
@app.exception_handler(IdempotencyStateError)
@app.exception_handler(IdempotencyConflictError)
@app.exception_handler(TicketTypeQuantity)
@app.exception_handler(TicketTypeDeleteError)
@app.exception_handler(EventDeleteError)
@app.exception_handler(UserAlreadyExistsError)
@app.exception_handler(TicketsSoldOutError)
@app.exception_handler(BookingModeError)
async def conflict_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """将售罄、账号重复、幂等冲突和非法业务操作统一转换为 409。"""
    return JSONResponse(
        status_code=status.HTTP_409_CONFLICT, content={"detail": str(exc)}
    )


@app.exception_handler(DatabasePoolTimeout)
async def database_pool_timeout(
    request: Request, exc: DatabasePoolTimeout
) -> JSONResponse:
    """连接池耗尽属于暂时过载，保持有限等待并提示沿用原幂等键重试。"""
    logger.warning(
        "数据库连接池等待超时 method=%s path=%s", request.method, request.url.path
    )
    return JSONResponse(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        content={"detail": "数据库繁忙，请稍后重试；同次操作请沿用原幂等键"},
        headers={"Retry-After": "1"},
    )


@app.exception_handler(SQLAlchemyError)
async def internal_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """记录数据库异常并返回 500，避免向客户端暴露内部数据库信息。"""
    logger.error(f"数据库异常，发生于 {request.method} {request.url.path} : {str(exc)}")
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={"detail": "服务器内部错误"},
    )


@app.exception_handler(TicketReservationExpireError)
@app.exception_handler(EmptyUpdateDataError)
async def empty_update_data_exception_handler(
    request: Request, exc: Exception
) -> JSONResponse:
    """将空更新请求、预订过期等不满足操作条件的异常转换为 400。"""
    return JSONResponse(
        status_code=status.HTTP_400_BAD_REQUEST, content={"detail": str(exc)}
    )


@app.exception_handler(HealthError)
@app.exception_handler(CacheUnavailableError)
async def service_unavailable(request: Request, exc: Exception) -> JSONResponse:
    """将数据库、缓存等关键依赖不可用的异常转换为 503。"""
    return JSONResponse(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        content={"detail": str(exc)},
        headers={"Retry-After": "1"},
    )


@app.exception_handler(QueryRateLimitError)
async def query_rate_limited(
    request: Request, exc: QueryRateLimitError
) -> JSONResponse:
    """区分请求限流和业务不存在，并告诉客户端何时可以重试。"""
    return JSONResponse(
        status_code=status.HTTP_429_TOO_MANY_REQUESTS,
        content={"detail": str(exc)},
        headers={"Retry-After": str(exc.retry_after)},
    )
