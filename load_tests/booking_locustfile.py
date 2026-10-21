"""独立压测单张票预订，只有有效的 201 RESERVED 响应计为成功。"""

import random
from functools import cache
from typing import Any
from uuid import uuid4

from locust import FastHttpUser, between, events, task
from locust.env import Environment

from load_tests.targets import LoadTestConfig, TokenProvider


@cache
def get_config() -> LoadTestConfig:
    """复用显式目标清单与签名配置，不读取后端默认身份。"""
    return LoadTestConfig.from_env()


@events.init.add_listener
def validate_config(environment: Environment, **kwargs: Any) -> None:
    """在启动前检查目标和签名参数，配置缺失时中止运行。"""
    get_config.cache_clear()
    try:
        get_config()
    except ValueError as error:
        raise SystemExit(f"压测配置错误：{error}") from error


class BookingUser(FastHttpUser):
    """持续预订目标票种，不在本场景支付或取消。"""

    wait_time = between(0.1, 0.5)  # type: ignore[no-untyped-call]
    connection_timeout = 5.0
    network_timeout = 5.0

    def on_start(self) -> None:
        """为模拟用户固定身份，凭证按原配置在到期前刷新。"""
        self.config = get_config()
        self.token_provider = TokenProvider(
            random.choice(self.config.targets.user_ids), self.config
        )

    @task
    def reserve_ticket(self) -> None:
        """每次使用新幂等键预订一张票，售罄等拒绝单独分组。"""
        ticket_type_id = random.choice(self.config.targets.ticket_type_ids)
        headers = {
            "Authorization": self.token_provider.authorization(),
            "Idempotency-Key": str(uuid4()),
        }
        with self.client.post(
            "/api/v1/tickets",
            json={"ticket_type_id": ticket_type_id},
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
