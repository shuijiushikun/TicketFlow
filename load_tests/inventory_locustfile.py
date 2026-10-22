"""只读余票展示压测；显式指定票种，不创建订单或重置数据。"""

import os

from locust import FastHttpUser, constant, task


class InventoryQueryUser(FastHttpUser):
    """持续查询余票，按状态统计，并检查响应内容。"""

    wait_time = constant(0)  # type: ignore[no-untyped-call]
    connection_timeout = 5.0
    network_timeout = 5.0

    def on_start(self) -> None:
        """要求目标为已确认存在的正整数票种 ID。"""
        self.ticket_type_id = int(os.environ["LOADTEST_TICKET_TYPE_ID"])
        if self.ticket_type_id <= 0:
            raise ValueError("LOADTEST_TICKET_TYPE_ID 必须是正整数")

    @task
    def browse_inventory(self) -> None:
        """只有 200 且余票结构正确才计成功，429/503 均保留为失败。"""
        with self.client.get(
            f"/api/v1/ticket-types/{self.ticket_type_id}/inventory",
            name="/api/v1/ticket-types/[id]/inventory",
            catch_response=True,
        ) as response:
            code = response.status_code
            response.request_meta["name"] = (
                f"/api/v1/ticket-types/[id]/inventory [HTTP {code}]"
            )
            if code != 200:
                response.failure(f"HTTP {code}")
                return
            try:
                data = response.json()
                remaining = data["remaining"]
                valid = (
                    data["ticket_type_id"] == self.ticket_type_id
                    and type(remaining) is int
                    and remaining >= 0
                )
            except (ValueError, KeyError, TypeError):
                valid = False
            if valid:
                response.success()
            else:
                response.failure("余票响应内容不符合约定")
