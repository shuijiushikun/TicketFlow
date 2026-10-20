class EventError(Exception):
    """活动业务异常的基类。"""

    pass


class EventNotFoundError(EventError):
    """查询的活动不存在。"""

    pass


class EventDeleteError(EventError):
    """活动存在关联数据，无法删除。"""

    pass


class TicketError(Exception):
    """票据业务异常的基类。"""

    pass


class TicketsSoldOutError(TicketError):
    """指定票种的可用库存不足，无法预订。"""

    pass


class TicketNotFoundError(TicketError):
    """票据不存在，或不属于当前用户。"""

    pass


class AuthError(Exception):
    """身份认证和权限异常的基类。"""

    pass


class UserAlreadyExistsError(AuthError):
    """注册邮箱已被使用。"""

    pass


class InvalidCredentialsError(AuthError):
    """邮箱或密码等身份凭证不正确。"""

    pass


class InactiveUserError(AuthError):
    """账号未启用，无法登录。"""

    pass


class TicketTypeError(Exception):
    """票种业务异常的基类。"""

    pass


class TicketTypeNotFoundError(TicketTypeError):
    """查询的票种不存在。"""

    pass


class TicketTypeDeleteError(TicketTypeError):
    """票种存在关联票据，无法删除。"""

    pass


class TicketTypeQuantity(TicketTypeError):
    """调整后的总票数小于已预留和已售出共同占用的数量。"""

    pass


class EmptyUpdateDataError(Exception):
    """更新请求没有提供需要修改的字段。"""

    pass


class IdempotencyError(Exception):
    """请求幂等处理异常的基类。"""

    pass


class IdempotencyConflictError(IdempotencyError):
    """幂等键对应的请求内容不同，或原请求仍在处理中。"""

    pass


class IdempotencyStateError(IdempotencyError):
    """幂等缓存数据损坏，或并发竞争导致临时状态不可用。"""

    pass


class PaymentError(Exception):
    """模拟支付异常的基类。"""

    pass


class TicketReservationExpireError(PaymentError):
    """预订已取消或超过支付期限。"""

    pass


class TicketAlreadyPaidError(PaymentError):
    """票据已付款，不能重复付款或直接取消。"""

    pass


class HealthError(Exception):
    """数据库或 Redis 等关键依赖不可用。"""

    pass
