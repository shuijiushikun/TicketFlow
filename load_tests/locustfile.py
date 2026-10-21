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
    connection_timeout = 5.0
    network_timeout = 5.0

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
        with self.client.get(
            f"/api/v1/events/{self.config.targets.event_id}",
            headers=self.headers,
            name="/api/v1/events/[id]",
            catch_response=True,
        ) as response:
            code = response.status_code
            outcome = f"HTTP {code}" if code else "network_error"
            response.request_meta["name"] = f"/api/v1/events/[id] [{outcome}]"
            if code == 200:
                try:
                    payload = response.json()
                except ValueError:
                    response.failure("活动详情响应不是有效 JSON")
                    return
                if (
                    isinstance(payload, dict)
                    and payload.get("id") == self.config.targets.event_id
                ):
                    response.success()
                else:
                    response.failure("活动详情与目标 ID 不一致")
            elif code:
                response.failure(f"HTTP {code}")

    @task(1)
    def attempt_purchase(self) -> None:
        """只把有效预订算成功，售罄、限流及异常按响应分别记录。"""
        ticket_type_id = random.choice(self.config.targets.ticket_type_ids)
        payload = {"ticket_type_id": ticket_type_id}
        self.headers["Authorization"] = self.token_provider.authorization()
        headers = {**self.headers, "Idempotency-Key": str(uuid4())}

        with self.client.post(
            "/api/v1/tickets",
            json=payload,
            headers=headers,
            name="/api/v1/tickets",
            catch_response=True,
        ) as response:
            code = response.status_code
            outcome = f"HTTP {code}" if code else "network_error"
            response.request_meta["name"] = f"/api/v1/tickets [{outcome}]"
            if code == 201:
                try:
                    payload = response.json()
                except ValueError:
                    response.failure("HTTP 201 响应不是有效 JSON")
                    return
                if (
                    isinstance(payload, dict)
                    and type(payload.get("id")) is int
                    and payload["id"] > 0
                    and payload.get("ticket_type_id") == ticket_type_id
                    and payload.get("status") == "RESERVED"
                ):
                    response.success()
                else:
                    response.failure("HTTP 201 响应的票据、票种或预订状态不正确")
            elif code:
                response.failure(f"HTTP {code}: {response.text[:200]}")
