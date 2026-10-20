from pathlib import Path

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
