from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        extra="ignore",
        enable_decoding=False,
    )

    ENV: str = "dev"
    DATABASE_URL: str
    REDIS_URL: str
    CORS_ALLOW_ORIGINS: list[str] = [
        "http://localhost:3000",
        "http://127.0.0.1:3000",
    ]
    APP_URL: str = "http://localhost:3000"
    SESSION_SECRET: str = "change-me"
    SESSION_COOKIE_NAME: str = "pmd_session"
    SESSION_TTL_DAYS: int = 14

    STRIPE_SECRET_KEY: str | None = None
    STRIPE_WEBHOOK_SECRET: str | None = None
    STRIPE_BASIC_PRICE_ID: str | None = None
    STRIPE_PRO_PRICE_ID: str | None = None
    STRIPE_ELITE_PRICE_ID: str | None = None

    POLYMARKET_BASE_URL: str
    POLY_PAGE_LIMIT: int = 100
    POLY_MAX_EVENTS: int | None = None
    POLY_MAX_PAGES: int | None = 100
    POLY_START_OFFSET: int = 0
    POLY_ORDER: str | None = None
    POLY_ASCENDING: bool | None = None
    POLY_USE_SERVER_FILTERS: bool = True
    POLY_LIQUIDITY_MIN: float | None = None
    POLY_VOLUME_MIN: float | None = None
    POLY_USE_GLOBAL_MINIMUMS: bool = True
    INGEST_INTERVAL_SECONDS: int = 300
    SNAPSHOT_RETENTION_DAYS: int = 7
    ALERT_RETENTION_DAYS: int = 30
    DELIVERY_RETENTION_DAYS: int = 30
    CLEANUP_ENABLED: bool = True
    CLEANUP_SCHEDULE_HOUR_UTC: int = 3
    FAST_SIGNALS_GLOBAL_ENABLED: bool = False

    TELEGRAM_BOT_TOKEN: str | None = None

    EXECUTION_ENABLED: bool = False

    ADMIN_API_KEY: str | None = None
    OPENAI_API_KEY: str | None = None
    LLM_API_BASE: str = "https://api.openai.com/v1/chat/completions"
    LLM_MODEL: str = "gpt-4o-mini"
    LLM_TIMEOUT_SECONDS: int = 15
    LLM_MAX_RETRIES: int = 2
    LLM_CACHE_TTL_SECONDS: int = 3600

    DEFAULT_TENANT_ID: str = "default"
    RATE_LIMIT_DEFAULT_PER_MIN: int = 60

    LOG_LEVEL: str = "INFO"
    LOG_JSON: bool = True
    HTTPX_SLOW_REQUEST_THRESHOLD_SECONDS: float = 2.0

    @field_validator("POLY_ORDER", "POLY_ASCENDING", mode="before")
    @classmethod
    def _empty_str_to_none(cls, value):
        if value == "":
            return None
        return value

    @field_validator("CORS_ALLOW_ORIGINS", mode="before")
    @classmethod
    def _split_origins(cls, value):
        if value is None:
            return value
        if isinstance(value, str):
            parts = [part.strip() for part in value.split(",") if part.strip()]
            return parts
        return value

    @field_validator(
        "POLY_MAX_EVENTS",
        "POLY_MAX_PAGES",
        "POLY_LIQUIDITY_MIN",
        "POLY_VOLUME_MIN",
        mode="before",
    )
    @classmethod
    def _none_str_to_none(cls, value):
        if value is None:
            return None
        if isinstance(value, str) and value.strip().lower() in {"", "none", "null"}:
            return None
        return value

settings = Settings()
