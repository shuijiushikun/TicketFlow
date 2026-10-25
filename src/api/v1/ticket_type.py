from typing import Annotated

from fastapi import APIRouter, Depends, Path, Response, status

from src.api.decorators import idempotent
from src.api.deps import (
    IdempotencyHeader,
    IdempotencyServiceDep,
    InventoryQueryServiceDep,
    TicketTypeServiceDep,
    get_current_superuser,
    limit_event_queries,
)
from src.core.exception import EmptyUpdateDataError
from src.models import User
from src.schemas.ticket_type import (
    TicketTypeCreate,
    TicketTypeDetailResponse,
    TicketTypeInventoryResponse,
    TicketTypeResponse,
    TicketTypeUpdate,
)

router = APIRouter()


@router.get(
    "/{ticket_type_id}/inventory",
    response_model=TicketTypeInventoryResponse,
    summary="查询展示余票",
    dependencies=[Depends(limit_event_queries)],
)
async def ticket_type_inventory(
    ticket_type_id: Annotated[int, Path(gt=0, le=2**31 - 1)],
    inventory_service: InventoryQueryServiceDep,
    response: Response,
) -> TicketTypeInventoryResponse:
    """无需登录，返回供页面展示的余票快照。

    余票可能存在同步延迟；不能依据该值保证购票成功或认定最终售罄，
    预订时仍由 PostgreSQL 原子扣票决定结果。
    """
    response.headers["Cache-Control"] = "no-store"
    return await inventory_service.get(ticket_type_id)


@router.post(
    "",
    response_model=TicketTypeResponse,
    status_code=status.HTTP_201_CREATED,
    summary="创建票种并初始化库存",
)
@idempotent(action="ticket_type_create")
async def ticket_type_create(
    ticket_type_data: TicketTypeCreate,
    ticket_type_service: TicketTypeServiceDep,
    admin: Annotated[User, Depends(get_current_superuser)],
    idempotency_service: IdempotencyServiceDep,
    idempotency_key: IdempotencyHeader = None,
) -> TicketTypeResponse:
    """创建票种并初始化库存。

    仅管理员可操作。指定活动 ID、票种名称、以分计价的价格和总票数。
    保存票种及持久化同步待办，提交后尝试初始化 Redis 库存副本，失败由后台恢复。
    """
    return await ticket_type_service.create(
        event_id=ticket_type_data.event_id, type_data=ticket_type_data
    )


@router.get(
    "/{ticket_type_id}",
    response_model=TicketTypeDetailResponse,
    status_code=status.HTTP_200_OK,
    summary="查询票种详情和库存占用",
)
async def ticket_type_get(
    ticket_type_id: int,
    ticket_type_service: TicketTypeServiceDep,
) -> TicketTypeDetailResponse:
    """查询票种详情和库存占用。

    ticket_type_id 是票种 ID。tickets_sold 表示预留与已售出共同占用的数量。
    剩余可用数量为 tickets_quantity - tickets_sold。无需登录。
    """
    return await ticket_type_service.get(ticket_type_id)


@router.delete(
    "/{ticket_type_id}", status_code=status.HTTP_204_NO_CONTENT, summary="删除票种"
)
async def ticket_type_delete(
    ticket_type_id: int,
    ticket_type_service: TicketTypeServiceDep,
    admin: Annotated[User, Depends(get_current_superuser)],
) -> None:
    """删除票种。

    仅管理员可操作。已有票据或抢票申请时保留票种，可通过修改票种关闭秒杀模式。
    """
    await ticket_type_service.delete(ticket_type_id)


@router.patch(
    "/{ticket_type_id}",
    response_model=TicketTypeResponse,
    status_code=status.HTTP_200_OK,
    summary="修改票种信息及预约模式",
)
async def ticket_type_update(
    ticket_type_id: int,
    update_data: TicketTypeUpdate,
    ticket_type_service: TicketTypeServiceDep,
    admin: Annotated[User, Depends(get_current_superuser)],
) -> TicketTypeResponse:
    """修改票种名称、价格、总票数或秒杀模式。

    仅管理员可操作，只更新提供的字段。总票数不能小于已占用数量。
    is_seckill 为 true 时只接受秒杀申请；切回 false 后，尚未处理的申请会被拒绝。
    请求体没有待修改字段时返回 400。
    """
    if not update_data.model_dump(exclude_unset=True):
        raise EmptyUpdateDataError("请至少提供一个需要修改的字段")

    return await ticket_type_service.update(
        ticket_type_id=ticket_type_id, update_data=update_data
    )
