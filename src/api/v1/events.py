from collections.abc import Sequence
from typing import Annotated

from fastapi import APIRouter, Depends, status
from fastapi.params import Path, Query

from src.api.decorators import idempotent
from src.api.deps import (
    EventServiceDep,
    IdempotencyHeader,
    IdempotencyServiceDep,
    TicketTypeServiceDep,
    get_current_superuser,
    get_hotspot_tracker,
    limit_event_queries,
)
from src.core.config import settings
from src.core.exception import EmptyUpdateDataError
from src.models import User
from src.schemas.event import (
    EventCreate,
    EventDetailResponse,
    EventResponse,
    EventUpdate,
)
from src.schemas.ticket_type import TicketTypeResponse
from src.services.hotspot import HotspotTracker

router = APIRouter()


@router.post(
    "",
    response_model=EventResponse,
    status_code=status.HTTP_201_CREATED,
    summary="创建活动",
)
@idempotent(action="create_event")
async def create_event(
    event: EventCreate,
    event_service: EventServiceDep,
    admin_user: Annotated[User, Depends(get_current_superuser)],
    idempotency_service: IdempotencyServiceDep,
    idempotency_key: IdempotencyHeader = None,
) -> EventResponse:
    """创建活动。

    仅管理员可操作。提交活动标题、带时区的时间和地址信息，成功时返回活动及其 ID。
    可通过 Idempotency-Key 在记录有效期内处理重复提交。
    """
    return await event_service.create(event_data=event)


@router.get(
    "/{event_id}",
    response_model=EventDetailResponse,
    status_code=status.HTTP_200_OK,
    summary="查询活动详情",
    dependencies=[Depends(limit_event_queries)],
    responses={429: {"description": "查询过于频繁"}, 503: {"description": "暂时繁忙"}},
)
async def get_event(
    event_id: Annotated[int, Path(ge=1)],
    event_service: EventServiceDep,
    tracker: Annotated[HotspotTracker, Depends(get_hotspot_tracker)],
) -> EventDetailResponse:
    """查询活动详情。

    通过 event_id 查询活动基本信息及其票种列表，无需登录。
    """
    response = await event_service.get(event_id=event_id)
    await tracker.record(event_id)
    return response


@router.get(
    "",
    response_model=list[EventResponse],
    status_code=status.HTTP_200_OK,
    summary="分页查询活动列表",
    dependencies=[Depends(limit_event_queries)],
    responses={429: {"description": "查询过于频繁"}, 503: {"description": "暂时繁忙"}},
)
async def get_events(
    event_service: EventServiceDep,
    offset: Annotated[int, Query(ge=0, description="跳过的记录数，从 0 开始")] = 0,
    limit: Annotated[
        int, Query(ge=1, le=100, description="本次最多返回的记录数，范围为 1～100")
    ] = settings.DEFAULT_PAGE_LIMIT,
) -> Sequence[EventResponse]:
    """分页查询活动列表。

    offset 表示跳过的记录数，limit 表示本次最多返回的记录数。无需登录。
    """
    return await event_service.get_all(offset=offset, limit=limit)


@router.get(
    "/{event_id}/ticket-types",
    response_model=list[TicketTypeResponse],
    status_code=status.HTTP_200_OK,
    summary="查询指定活动的票种列表",
)
async def ticket_type_get_all_for_event(
    event_id: int,
    ticket_type_service: TicketTypeServiceDep,
    offset: Annotated[int, Query(ge=0, description="跳过的记录数，从 0 开始")] = 0,
    limit: Annotated[
        int, Query(ge=1, le=100, description="本次最多返回的记录数，范围为 1～100")
    ] = settings.DEFAULT_PAGE_LIMIT,
) -> Sequence[TicketTypeResponse]:
    """查询指定活动的票种列表。

    event_id 是活动 ID；offset 和 limit 控制分页。无需登录。
    """
    return await ticket_type_service.get_all_for_event(
        event_id=event_id, offset=offset, limit=limit
    )


@router.delete(
    "/{event_id}", status_code=status.HTTP_204_NO_CONTENT, summary="删除活动"
)
async def delete_event(
    event_id: int,
    event_service: EventServiceDep,
    admin_user: Annotated[User, Depends(get_current_superuser)],
) -> None:
    """删除活动。

    仅管理员可操作。活动已有相关票种或票据、受到数据库关联约束时，删除会被拒绝。
    """
    await event_service.delete(event_id)


@router.patch(
    "/{event_id}",
    response_model=EventResponse,
    status_code=status.HTTP_200_OK,
    summary="修改活动信息",
)
async def update_event(
    event_id: int,
    update_data: EventUpdate,
    event_service: EventServiceDep,
    admin_user: Annotated[User, Depends(get_current_superuser)],
) -> EventResponse:
    """修改活动信息。

    仅管理员可操作，只更新请求中提供的字段；未提供的字段保持原值。
    请求体没有任何待修改字段时返回 400。
    """
    if not update_data.model_dump(exclude_unset=True):
        raise EmptyUpdateDataError("请至少提供一个需要修改的字段")
    return await event_service.update(event_id=event_id, update_data=update_data)
