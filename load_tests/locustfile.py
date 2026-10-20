import json
import os
import random
import sys
from datetime import UTC, datetime, timedelta
from typing import Any, cast

import jwt
from locust import FastHttpUser, between, events, task
from locust.env import Environment

_event_id_str = os.getenv("TARGET_EVENT_ID")
_ticket_types_str = os.getenv("AVAILABLE_TICKET_TYPES")

if not _ticket_types_str or not _event_id_str:
    print("配置错误：必须设置 TARGET_EVENT_ID 和 AVAILABLE_TICKET_TYPES")
    sys.exit(1)

TARGET_EVENT_ID: int = int(_event_id_str)
AVAILABLE_TICKET_TYPES: list[int] = [
    int(t_id.strip()) for t_id in _ticket_types_str.split(",") if t_id.strip().isdigit()
]

SECRET_KEY = "2b55886afc889358509ae2a6fe93ac8b881d7985b1984bf71b9d41e5a0c1a281"
ALGORITHM = "HS256"


@events.test_start.add_listener
def on_test_start(environment: Environment, **kwargs: Any) -> None:
    """Locust 开始生成模拟用户前，检查目标活动和票种配置。"""
    if not TARGET_EVENT_ID or not AVAILABLE_TICKET_TYPES:
        environment.runner.quit()


class RammsteinFan(FastHttpUser):
    """模拟活动浏览和票据预订的压测用户。"""

    wait_time = between(0.1, 0.5)

    def __init__(self, environment: Environment) -> None:
        super().__init__(environment)
        self.headers: dict[str, str] = {"Content-Type": "application/json"}

    def on_start(self) -> None:
        """模拟用户启动时随机选择用户 ID，并准备访问令牌。"""
        user_id = random.randint(1, 10000)

        token_payload = {
            "sub": str(user_id),
            "exp": datetime.now(UTC) + timedelta(minutes=30),
        }

        token = cast(str, jwt.encode(token_payload, SECRET_KEY, algorithm=ALGORITHM))
        self.headers["Authorization"] = f"Bearer {token}"

    @task(4)
    def browse_event(self) -> None:
        """模拟用户浏览目标活动详情。"""
        self.client.get(
            f"/api/v1/events/{TARGET_EVENT_ID}",
            headers=self.headers,
            name="/api/v1/events/[id]",
        )

    @task(1)
    def attempt_purchase(self) -> None:
        """模拟用户尝试预订票据，并区分售罄等预期业务结果与异常。"""
        ticket_type_id = random.choice(AVAILABLE_TICKET_TYPES)
        payload: dict[str, Any] = {
            "event_id": TARGET_EVENT_ID,
            "ticket_type_id": ticket_type_id,
            "quantity": random.randint(1, 2),
        }

        with self.client.post(
            "/api/v1/tickets/",
            json=payload,
            headers=self.headers,
            name="/api/v1/tickets/",
            catch_response=True,
        ) as response:
            if response.status_code in (200, 201):
                response.success()
            elif response.status_code in (400, 404, 409):
                try:
                    error_detail = response.json().get("detail", "").lower()
                    if any(
                        kw in error_detail
                        for kw in [
                            "sold out",
                            "not enough",
                            "unavailable",
                            "售罄",
                            "库存不足",
                            "暂无可用库存",
                        ]
                    ):
                        response.success()
                    else:
                        response.failure(f"非预期业务错误：{error_detail}")
                except json.JSONDecodeError:
                    response.failure(f"错误响应不是 JSON：{response.text}")
            elif response.status_code == 422:
                response.failure(f"请求参数校验失败：{response.text}")
            else:
                response.failure(f"HTTP {response.status_code}: {response.text}")
