"""为 HTTP 请求绑定服务端生成的追踪 ID，并采集有限标签的请求指标。"""

from contextvars import ContextVar
from time import monotonic
from uuid import uuid4

from starlette.datastructures import MutableHeaders
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from src.core.metrics import HTTP_DURATION, HTTP_REQUESTS

request_id_context: ContextVar[str | None] = ContextVar("request_id", default=None)
_HTTP_METHODS = frozenset(
    {"GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "TRACE", "CONNECT"}
)


class RequestContextMiddleware:
    """隔离并发请求上下文；不采信客户端提供的请求 ID 或转发地址。"""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        """在响应头中返回请求 ID；响应完成后记录耗时并恢复原上下文。"""
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        request_id = str(uuid4())
        token = request_id_context.set(request_id)
        scope.setdefault("state", {})["request_id"] = request_id
        started = monotonic()
        status = 500

        async def send_with_context(message: Message) -> None:
            nonlocal status
            if message["type"] == "http.response.start":
                status = message["status"]
                headers = MutableHeaders(scope=message)
                headers["X-Request-ID"] = request_id
            await send(message)

        try:
            await self.app(scope, receive, send_with_context)
        finally:
            if scope.get("path") != "/metrics":
                method = scope.get("method", "OTHER")
                method = method if method in _HTTP_METHODS else "OTHER"
                route = getattr(scope.get("route"), "path", None)
                route = route if isinstance(route, str) else "__unmatched__"
                status_label = str(status) if 100 <= status <= 599 else "other"
                HTTP_REQUESTS.labels(method, route, status_label).inc()
                HTTP_DURATION.labels(method, route, status_label).observe(
                    monotonic() - started
                )
            request_id_context.reset(token)
