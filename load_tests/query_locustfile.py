"""独立压测公开活动详情；不登录、不预订、不重置数据库或 Redis。"""

import os
from typing import Any

from locust import FastHttpUser, between, events, task
from locust.env import Environment


def get_event_id() -> int:
    """要求显式指定已经确认存在的活动 ID。"""
    try:
        event_id = int(os.environ.get("LOADTEST_EVENT_ID", ""))
    except ValueError as error:
        raise ValueError("请把 LOADTEST_EVENT_ID 设置为正整数活动 ID") from error
    if event_id <= 0:
        raise ValueError("LOADTEST_EVENT_ID 必须是正整数活动 ID")
    return event_id


@events.init.add_listener
def validate_target(environment: Environment, **kwargs: Any) -> None:
    """在启动压测前检查活动参数。"""
    try:
        get_event_id()
    except ValueError as error:
        raise SystemExit(f"压测配置错误：{error}") from error


class EventQueryUser(FastHttpUser):
    """模拟用户反复浏览一个活动，按 HTTP 状态分别记录请求。"""

    wait_time = between(0.1, 0.5)  # type: ignore[no-untyped-call]
    connection_timeout = 5.0
    network_timeout = 5.0

    def on_start(self) -> None:
        """固定本用户访问的活动地址。"""
        self.detail_path = f"/api/v1/events/{get_event_id()}"

    @task
    def browse_event(self) -> None:
        """只有 200 算成功，限流和服务不可用分别显示且计为失败。"""
        with self.client.get(
            self.detail_path,
            name="/api/v1/events/[id]",
            catch_response=True,
        ) as response:
            code = response.status_code
            outcome = f"HTTP {code}" if code else "network_error"
            response.request_meta["name"] = f"/api/v1/events/[id] [{outcome}]"
            if code == 200:
                response.success()
            elif code:
                response.failure(f"HTTP {code}")
