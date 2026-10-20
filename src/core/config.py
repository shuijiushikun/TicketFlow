from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

BASE_DIR = Path(__file__).resolve().parent.parent.parent


class Settings(BaseSettings):
    """应用配置，自动读取环境变量和项目根目录的 .env 文件，并校验配置类型。"""

    PROJECT_NAME: str
    PROJECT_VERSION: str

    API_V1_STR: str
    ENVIRONMENT: str
    LOG_LEVEL: str

    SECRET_KEY: str
    ALGORITHM: str
    ACCESS_TOKEN_EXPIRE_MINUTES: int

    DEFAULT_PAGE_LIMIT: int

    POSTGRES_USER: str
    POSTGRES_PASSWORD: str
    POSTGRES_DB: str
    POSTGRES_HOST: str
    POSTGRES_PORT: str

    REDIS_HOST: str
    REDIS_PORT: int
    REDIS_TTL_SECONDS: int

    TICKET_RESERVATION_TIME_SECONDS: int

    # 仅用于活动查询缓存；库存、支付和 Arq 不使用这些过期与准入参数。
    CACHE_DETAIL_TTL_SECONDS: int = Field(default=86400, ge=1)
    CACHE_DETAIL_JITTER_SECONDS: int = Field(default=3600, ge=0)
    CACHE_LIST_TTL_SECONDS: int = Field(default=3600, ge=1)
    CACHE_LIST_JITTER_SECONDS: int = Field(default=600, ge=0)
    CACHE_NEGATIVE_TTL_SECONDS: int = Field(default=30, ge=1)
    CACHE_NEGATIVE_JITTER_SECONDS: int = Field(default=30, ge=0)
    CACHE_REDIS_TIMEOUT_SECONDS: float = Field(default=0.15, gt=0)
    CACHE_CIRCUIT_COOLDOWN_SECONDS: float = Field(default=2.0, gt=0)
    CACHE_LOCK_LEASE_MS: int = Field(default=5000, ge=1)
    CACHE_LOCK_WAIT_SECONDS: float = Field(default=0.3, gt=0)
    CACHE_DB_CONCURRENCY: int = Field(default=4, ge=1)
    CACHE_DB_QUEUE_SIZE: int = Field(default=8, ge=0)
    CACHE_DB_QUEUE_TIMEOUT_SECONDS: float = Field(default=0.05, gt=0)
    CACHE_DB_QUERY_TIMEOUT_SECONDS: float = Field(default=1.5, gt=0)
    EVENT_QUERY_RATE: float = Field(default=100, gt=0)
    EVENT_QUERY_BURST: int = Field(default=200, ge=1)
    EVENT_QUERY_GLOBAL_RATE: float = Field(default=1000, gt=0)
    EVENT_QUERY_GLOBAL_BURST: int = Field(default=2000, ge=1)
    EVENT_QUERY_MAX_CLIENTS: int = Field(default=10000, ge=1)

    @property
    def DATABASE_URL(self) -> str:
        """组合 PostgreSQL 异步连接地址，供 SQLAlchemy 和 asyncpg 使用。"""
        return (
            f"postgresql+asyncpg://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}"
            f"@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
        )

    @property
    def REDIS_URL(self) -> str:
        """组合业务 Redis 连接地址，使用 0 号库保存缓存和库存。"""
        return f"redis://{self.REDIS_HOST}:{self.REDIS_PORT}/0"

    model_config = SettingsConfigDict(
        env_file=str(BASE_DIR / ".env"), env_ignore_empty=True, extra="ignore"
    )


settings = Settings()
