"""压测目标清单与显式认证配置，不读取业务默认配置。"""

import json
import os
import time
from collections.abc import Mapping
from dataclasses import asdict, dataclass, field
from pathlib import Path

import jwt

DEFAULT_MANIFEST = Path(__file__).parent / "artifacts" / "targets.json"


@dataclass(frozen=True)
class LoadTestTargets:
    """初始化后实际存在的活动、票种和启用用户 ID。"""

    event_id: int
    ticket_type_ids: list[int]
    user_ids: list[int]

    def __post_init__(self) -> None:
        """拒绝空目标、布尔值和非正整数 ID。"""
        if type(self.event_id) is not int or self.event_id <= 0:
            raise ValueError("manifest 的 event_id 必须是正整数")
        for name in ("ticket_type_ids", "user_ids"):
            values = getattr(self, name)
            if not isinstance(values, list) or not values:
                raise ValueError(f"manifest 的 {name} 必须是非空 ID 列表")
            if any(type(value) is not int or value <= 0 for value in values):
                raise ValueError(f"manifest 的 {name} 只能包含正整数")


def load_targets(path: str | Path = DEFAULT_MANIFEST) -> LoadTestTargets:
    """读取并校验 manifest，错误中保留路径以便定位配置问题。"""
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
        return LoadTestTargets(
            event_id=payload["event_id"],
            ticket_type_ids=payload["ticket_type_ids"],
            user_ids=payload["user_ids"],
        )
    except (OSError, ValueError, KeyError, TypeError) as error:
        raise ValueError(f"无法读取有效的 manifest：{path}；{error}") from error


def write_targets(
    targets: LoadTestTargets, path: str | Path = DEFAULT_MANIFEST
) -> None:
    """写入仅含目标 ID 的清单，不包含数据库地址、密钥或 Token。"""
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(
        json.dumps(asdict(targets), ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


@dataclass(frozen=True)
class LoadTestConfig:
    """Locust 使用的目标和 JWT 签名配置。"""

    targets: LoadTestTargets
    secret_key: str = field(repr=False)
    algorithm: str = "HS256"
    token_ttl_seconds: int = 1800
    token_refresh_seconds: int = 60

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> "LoadTestConfig":
        """从显式 LOADTEST_* 环境变量读取配置，无有效配置时立即失败。"""
        source = os.environ if environ is None else environ
        secret_key = source.get("LOADTEST_SECRET_KEY", "")
        if not secret_key.strip():
            raise ValueError("必须显式设置 LOADTEST_SECRET_KEY，与被测服务保持一致")
        algorithm = source.get("LOADTEST_JWT_ALGORITHM", "HS256")
        if algorithm not in {"HS256", "HS384", "HS512"}:
            raise ValueError("LOADTEST_JWT_ALGORITHM 仅支持 HS256、HS384、HS512")
        try:
            ttl = int(source.get("LOADTEST_TOKEN_TTL_SECONDS", "1800"))
            refresh = int(source.get("LOADTEST_TOKEN_REFRESH_SECONDS", "60"))
        except ValueError as error:
            raise ValueError("LOADTEST_TOKEN_*_SECONDS 必须是整数") from error
        if ttl <= 0 or not 0 < refresh < ttl:
            raise ValueError(
                "LOADTEST_TOKEN_TTL_SECONDS 必须为正数，"
                "LOADTEST_TOKEN_REFRESH_SECONDS 必须介于 0 与有效期之间"
            )
        return cls(
            targets=load_targets(
                source.get("LOADTEST_MANIFEST", str(DEFAULT_MANIFEST))
            ),
            secret_key=secret_key,
            algorithm=algorithm,
            token_ttl_seconds=ttl,
            token_refresh_seconds=refresh,
        )


class TokenProvider:
    """为同一专用用户在到期前刷新 JWT，不改变模拟身份。"""

    def __init__(self, user_id: int, config: LoadTestConfig) -> None:
        self.user_id = user_id
        self.config = config
        self._token = ""
        self._expires_at = 0

    def authorization(self, *, now: float | None = None) -> str:
        """返回 Bearer 凭证，在剩余有效期进入刷新窗口时重新签名。"""
        current = time.time() if now is None else now
        if not self._token or current >= (
            self._expires_at - self.config.token_refresh_seconds
        ):
            self._expires_at = int(current + self.config.token_ttl_seconds)
            self._token = jwt.encode(
                {"sub": str(self.user_id), "exp": self._expires_at},
                self.config.secret_key,
                algorithm=self.config.algorithm,
            )
        return f"Bearer {self._token}"
