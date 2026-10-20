from collections.abc import Sequence
from typing import Annotated

from fastapi import APIRouter, status
from fastapi.params import Depends, Query

from src.api.decorators import idempotent
from src.api.deps import (
    IdempotencyHeader,
    IdempotencyServiceDep,
    TicketServiceDep,
    get_current_user,
)
from src.core.config import settings
from src.models import User
from src.schemas.ticket import TicketCreate, TicketDetailResponse, TicketResponse

router = APIRouter()


@router.post(
    "",
    response_model=TicketResponse,
    status_code=status.HTTP_201_CREATED,
    summary="预订一张票",
)
@idempotent(action="create_ticket")
async def create_ticket(
    owner: Annotated[User, Depends(get_current_user)],
    ticket: TicketCreate,
    ticket_service: TicketServiceDep,
    idempotency_service: IdempotencyServiceDep,
    idempotency_key: IdempotencyHeader = None,
) -> TicketResponse:
    """预订一张票。

    需要登录。请求体只需提供 ticket_type_id，票据所属用户由登录身份确定。
    预订成功后状态为 RESERVED（已预留），同时占用库存并安排超时回收任务。
    可携带 Idempotency-Key 处理记录有效期内的重复请求。
    """
    return await ticket_service.create(user_id=owner.id, ticket_data=ticket)


@router.get(
    "/{ticket_id}",
    response_model=TicketDetailResponse,
    status_code=status.HTTP_200_OK,
    summary="查询自己的票据详情",
)
async def get_ticket(
    owner: Annotated[User, Depends(get_current_user)],
    ticket_id: int,
    ticket_service: TicketServiceDep,
) -> TicketDetailResponse:
    """查询自己的票据详情。

    需要登录。ticket_id 是具体票据的 ID，返回票种 ID、当前状态和活动标题。
    只能查询自己的票据；票据不存在或不属于当前用户时返回 404。
    """
    return await ticket_service.get(owner_id=owner.id, ticket_id=ticket_id)


@router.get(
    "",
    response_model=list[TicketResponse],
    status_code=status.HTTP_200_OK,
    summary="分页查询自己的票据",
)
async def get_tickets(
    owner: Annotated[User, Depends(get_current_user)],
    ticket_service: TicketServiceDep,
    offset: Annotated[int, Query(ge=0, description="跳过的记录数，从 0 开始")] = 0,
    limit: Annotated[
        int, Query(ge=1, le=100, description="本次最多返回的记录数，范围为 1～100")
    ] = settings.DEFAULT_PAGE_LIMIT,
) -> Sequence[TicketResponse]:
    """分页查询自己的票据。

    需要登录，根据当前用户筛选票据，使用 offset 和 limit 分页。
    返回票据 ID、票种 ID 和状态。
    """
    return await ticket_service.get_all_for_user(
        owner_id=owner.id, offset=offset, limit=limit
    )


@router.delete(
    "/{ticket_id}", status_code=status.HTTP_204_NO_CONTENT, summary="取消未付款的预订"
)
async def delete_ticket(
    owner: Annotated[User, Depends(get_current_user)],
    ticket_id: int,
    ticket_service: TicketServiceDep,
) -> None:
    """取消未付款的预订。

    需要登录，只能取消自己的票。票据改为 CANCELED（已取消），保留记录并释放库存。
    重复取消同一张已取消的票返回 204，不重复回补库存；已付款的票返回 409。
    """
    await ticket_service.delete(owner_id=owner.id, ticket_id=ticket_id)
