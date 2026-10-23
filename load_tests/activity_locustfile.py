"""多活动详情压测：80% 流量访问清单中的热点，只将内容正确的 200 计为成功。"""

import json
import os
import random
from functools import cache
from pathlib import Path

from locust import FastHttpUser, constant, events, task

ENVIRONMENT = None


@events.init.add_listener
def remember_environment(environment, **kwargs):
    """保存当前运行环境，以便升压完成后重置统计。"""
    global ENVIRONMENT
    ENVIRONMENT = environment


@cache
def targets():
    """读取专用活动及进程端口清单。"""
    data = json.loads(Path(os.environ["ACTIVITY_MANIFEST"]).read_text(encoding="utf-8"))
    assert data["hot_ids"] and data["cold_ids"] and data["ports"]
    return data


@events.spawning_complete.add_listener
def reset_after_spawn(user_count, **kwargs):
    """排除启动升压期间的统计。"""
    # 排除升压阶段；最终 HTML/CSV 使用 Locust 的实际统计窗口。
    if ENVIRONMENT is not None:
        ENVIRONMENT.stats.reset_all()


class ActivityUser(FastHttpUser):
    """持续查询真实活动，保留热点与分散访问比例。"""

    wait_time = constant(0)
    connection_timeout = 5.0
    network_timeout = 5.0

    def on_start(self):
        """为客户端分配一个 API 进程。"""
        data = targets()
        self.base = f"http://127.0.0.1:{random.choice(data['ports'])}"

    @task
    def detail(self):
        """校验活动 ID，并按状态分别统计有效响应。"""
        data = targets()
        group = "hot_ids" if random.random() < 0.8 else "cold_ids"
        event_id = random.choice(data[group])
        with self.client.get(
            f"{self.base}/api/v1/events/{event_id}",
            name="activity [200]",
            catch_response=True,
        ) as response:
            code = response.status_code
            response.request_meta["name"] = f"activity [{code or 'network'}]"
            if code != 200:
                response.failure(f"HTTP {code or 'network'}")
                return
            try:
                valid = response.json().get("id") == event_id
            except (ValueError, AttributeError):
                valid = False
            if valid:
                response.success()
            else:
                response.failure("活动响应内容不正确")
