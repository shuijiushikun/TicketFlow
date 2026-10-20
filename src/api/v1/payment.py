from typing import Annotated

from fastapi import APIRouter, status
from fastapi.params import Depends

from src.api.decorators import idempotent
from src.api.deps import (
    IdempotencyHeader,
    IdempotencyServiceDep,
    PaymentServiceDep,
    get_current_user,
)
from src.models import User
from src.schemas.payment import TicketPaymentSchema

router = APIRouter()


@router.post(
    "/{ticket_id}/payments",
    response_model=TicketPaymentSchema,
    status_code=status.HTTP_201_CREATED,
    summary="模拟支付预订的票据",
)
@idempotent(action="ticket_payment")
async def ticket_payment(
    ticket_id: int,
    user: Annotated[User, Depends(get_current_user)],
    payment_service: PaymentServiceDep,
    idempotency_service: IdempotencyServiceDep,
    idempotency_key: IdempotencyHeader = None,
) -> TicketPaymentSchema:
    """模拟支付预订的票据。

    需要登录，只能支付自己的票。预订未过期时将 RESERVED（已预留）改为 SOLD（已售出）。
    这是状态流转演示，不调用真实支付渠道，也不会实际扣款。
    已付款返回 409，已取消或已过支付期限返回 400。
    """
    return await payment_service.pay_for_ticket(ticket_id=ticket_id, owner_id=user.id)
