"""仅由内部 Prometheus 抓取的指标入口；公网代理应拒绝此路径。"""

from fastapi import APIRouter, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from src.core.metrics import registry

router = APIRouter()


@router.get("/metrics", include_in_schema=False)
async def metrics() -> Response:
    """导出当前进程独立注册表，不触发数据库或 Redis 访问。"""
    return Response(
        content=generate_latest(registry),
        headers={"Content-Type": CONTENT_TYPE_LATEST},
    )
