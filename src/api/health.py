from fastapi import APIRouter, status

from src.api.deps import HealthServiceDep
from src.core import settings

router = APIRouter()


@router.get("/", tags=["系统检查"], summary="查询应用基本状态")
async def public_root() -> dict[str, str]:
    """查询应用基本状态。

    返回应用名称、运行环境和基本状态，不检查数据库及 Redis 连接。
    """
    return {
        "status": "healthy",
        "app": settings.PROJECT_NAME,
        "env": settings.ENVIRONMENT,
    }


@router.get(
    "/healthz", status_code=status.HTTP_200_OK, summary="检查后端进程是否可以响应请求"
)
async def liveness_probe() -> dict[str, str]:
    """检查后端进程是否可以响应请求。

    这是轻量存活检查，不访问数据库和 Redis；成功时返回 status=ok。
    """
    return {"status": "ok"}


@router.get(
    "/readyz", status_code=status.HTTP_200_OK, summary="检查数据库和 Redis 是否就绪"
)
async def readiness_probe(health_service: HealthServiceDep) -> dict[str, str]:
    """检查数据库和 Redis 是否就绪。

    成功时返回 status=ready；任一关键依赖不可用时返回 503。
    """
    await health_service.check_readiness()
    return {"status": "ready"}
