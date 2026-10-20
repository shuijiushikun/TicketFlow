from typing import Annotated

from fastapi import APIRouter, Depends, status

from src.api.decorators import idempotent
from src.api.deps import (
    IdempotencyHeader,
    IdempotencyServiceDep,
    TicketTypeServiceDep,
    get_current_superuser,
)
from src.core.exception import EmptyUpdateDataError
from src.models import User
from src.schemas.ticket_type import (
    TicketTypeCreate,
    TicketTypeDetailResponse,
    TicketTypeResponse,
    TicketTypeUpdate,
)

router = APIRouter()


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

    仅管理员可操作。已有相关票据时，数据库关联约束会阻止删除。
    """
    await ticket_type_service.delete(ticket_type_id)


@router.patch(
    "/{ticket_type_id}",
    response_model=TicketTypeResponse,
    status_code=status.HTTP_200_OK,
    summary="修改票种名称、价格或总票数",
)
async def ticket_type_update(
    ticket_type_id: int,
    update_data: TicketTypeUpdate,
    ticket_type_service: TicketTypeServiceDep,
    admin: Annotated[User, Depends(get_current_superuser)],
) -> TicketTypeResponse:
    """修改票种名称、价格或总票数。

    仅管理员可操作，只更新提供的字段。总票数不能小于已占用数量。
    请求体没有待修改字段时返回 400。
    """
    if not update_data.model_dump(exclude_unset=True):
        raise EmptyUpdateDataError("请至少提供一个需要修改的字段")

    return await ticket_type_service.update(
        ticket_type_id=ticket_type_id, update_data=update_data
    )
