"""外部测试资源的显式配置与所有权校验，不读取业务 .env。"""

from __future__ import annotations

import re
from collections.abc import MutableMapping
from dataclasses import dataclass, field
from urllib.parse import unquote, urlsplit, urlunsplit

RESOURCE_MARKER_KEY = "ticketflow:test-resource-owner"
RESOURCE_CONTROL_DB = 15


@dataclass(frozen=True)
class TestResources:
    """只有显式授权的专用数据库与 Redis 才能执行重置。"""

    __test__ = False

    postgres_dsn: str = field(repr=False)
    redis_url: str
    token: str = field(repr=False)

    @classmethod
    def from_environment(cls, environment: MutableMapping[str, str]) -> TestResources:
        """校验专用目标、重置开关和实例标记；错误中不包含连接凭证。"""
        required = (
            "TICKETFLOW_TEST_POSTGRES_DSN",
            "TICKETFLOW_TEST_REDIS_URL",
            "TICKETFLOW_TEST_RESOURCE_TOKEN",
        )
        if environment.get("TICKETFLOW_TEST_ALLOW_RESET") != "1" or any(
            not environment.get(key) for key in required
        ):
            raise ValueError("必须显式配置专用测试资源及重置授权，拒绝使用业务配置")

        dsn, redis_url, token = (environment[key] for key in required)
        try:
            postgres = urlsplit(dsn)
            database = postgres.path.removeprefix("/")
            if (
                postgres.scheme != "postgresql"
                or not postgres.hostname
                or not postgres.port
                or not postgres.username
                or not postgres.password
                or not re.fullmatch(r"[a-z][a-z0-9_]*_test", database)
                or postgres.query
                or postgres.fragment
            ):
                raise ValueError
        except ValueError:
            raise ValueError(
                "测试 PostgreSQL 需显式端口、凭证与 _test 后缀库名"
            ) from None

        try:
            redis = urlsplit(redis_url)
            if (
                redis.scheme != "redis"
                or not redis.hostname
                or not redis.port
                or redis.path != "/0"
                or redis.username
                or redis.password
                or redis.query
                or redis.fragment
            ):
                raise ValueError
        except ValueError:
            raise ValueError("测试 Redis 需显式端口及专用实例的 0 号库") from None
        if len(token) < 16:
            raise ValueError("专用测试实例标记长度至少为 16")
        return cls(dsn, redis_url, token)

    @property
    def database_name(self) -> str:
        """返回已经验证过、可安全作为 SQL 标识符的测试库名。"""
        return urlsplit(self.postgres_dsn).path.removeprefix("/")

    @property
    def admin_dsn(self) -> str:
        """只替换数据库路径，保留 URL 编码的连接凭证。"""
        parsed = urlsplit(self.postgres_dsn)
        return urlunsplit(parsed._replace(path="/postgres"))

    @property
    def sqlalchemy_url(self) -> str:
        """返回 SQLAlchemy 异步驱动 URL。"""
        return self.postgres_dsn.replace("postgresql://", "postgresql+asyncpg://", 1)

    @property
    def marker(self) -> str:
        """生成本轮隔离资源的所有权标记。"""
        return f"ticketflow-test:{self.token}"

    @property
    def redis_control_url(self) -> str:
        """返回独立控制库地址，不依赖连接上的临时 SELECT 状态。"""
        parsed = urlsplit(self.redis_url)
        return urlunsplit(parsed._replace(path=f"/{RESOURCE_CONTROL_DB}"))

    def require_marker(self, actual: str | None) -> None:
        """未确认所有权时拒绝重建数据库或清空 Redis。"""
        if actual != self.marker:
            raise RuntimeError("专用测试资源所有权标记不匹配，拒绝重置")

    def configure_environment(self, environment: MutableMapping[str, str]) -> None:
        """在导入应用之前，把测试连接注入现有应用配置。"""
        postgres = urlsplit(self.postgres_dsn)
        redis = urlsplit(self.redis_url)
        environment.update(
            ENVIRONMENT="test",
            POSTGRES_USER=unquote(postgres.username or ""),
            POSTGRES_PASSWORD=unquote(postgres.password or ""),
            POSTGRES_DB=self.database_name,
            POSTGRES_HOST=postgres.hostname or "",
            POSTGRES_PORT=str(postgres.port),
            REDIS_HOST=redis.hostname or "",
            REDIS_PORT=str(redis.port),
        )
