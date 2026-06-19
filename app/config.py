from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    DATABASE_URL: str = "postgresql+asyncpg://events:events@postgres:5432/events"

    REDIS_URL: str = "redis://redis:6379/0"
    STREAM_KEY: str = "transactions"
    CONSUMER_GROUP: str = "processors"
    DLQ_STREAM_KEY: str = "transactions:dead"
    RETRY_ZSET_KEY: str = "transactions:retry"

    FX_RATE_TTL_SECONDS: int = 300
    FX_API_BASE_URL: str = "https://api.exchangerate.host"

    MAX_ATTEMPTS: int = 5
    RETRY_BASE_DELAY_SECONDS: int = 1
    RETRY_MAX_DELAY_SECONDS: int = 300

    MAX_PAGE_SIZE: int = 100
    DEFAULT_PAGE_SIZE: int = 50

    METRICS_PORT: int = 9100


settings = Settings()
