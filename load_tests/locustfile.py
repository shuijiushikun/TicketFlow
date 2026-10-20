import json
import random
from functools import cache
from typing import Any
from uuid import uuid4

from locust import FastHttpUser, between, events, task
from locust.env import Environment

from load_tests.targets import LoadTestConfig, TokenProvider


@cache
def get_config() -> LoadTestConfig:
    """每个 Locust 进程只解析一次目标清单，供模拟用户复用。"""
    return LoadTestConfig.from_env()


@events.init.add_listener
def on_init(environment: Environment, **kwargs: Any) -> None:
    """Locust 启动即校验清单和认证配置，失败时中止运行。"""
    get_config.cache_clear()
    try:
        get_config()
    except ValueError as error:
        raise SystemExit(f"压测配置错误：{error}") from error


class RammsteinFan(FastHttpUser):
    """模拟活动浏览和票据预订的压测用户。"""

    # Locust 的 between 没有类型注解，运行时返回用户等待时间函数。
    wait_time = between(0.1, 0.5)  # type: ignore[no-untyped-call]

    def __init__(self, environment: Environment) -> None:
        super().__init__(environment)
        self.config: LoadTestConfig = get_config()
        self.headers: dict[str, str] = {"Content-Type": "application/json"}

    def on_start(self) -> None:
        """模拟用户启动时随机选择用户 ID，并准备访问令牌。"""
        user_id = random.choice(self.config.targets.user_ids)
        self.token_provider = TokenProvider(user_id, self.config)
        self.headers["Authorization"] = self.token_provider.authorization()

    @task(4)
    def browse_event(self) -> None:
        """模拟用户浏览目标活动详情。"""
        self.headers["Authorization"] = self.token_provider.authorization()
        self.client.get(
            f"/api/v1/events/{self.config.targets.event_id}",
            headers=self.headers,
            name="/api/v1/events/[id]",
        )

    @task(1)
    def attempt_purchase(self) -> None:
        """模拟用户尝试预订票据，并区分售罄等预期业务结果与异常。"""
        ticket_type_id = random.choice(self.config.targets.ticket_type_ids)
        payload = {"ticket_type_id": ticket_type_id}
        self.headers["Authorization"] = self.token_provider.authorization()
        headers = {**self.headers, "Idempotency-Key": str(uuid4())}

        with self.client.post(
            "/api/v1/tickets/",
            json=payload,
            headers=headers,
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
