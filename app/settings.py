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
    POLY_TIMEOUT_SECONDS: int = 12
    POLY_CONNECT_TIMEOUT_SECONDS: int = 5
    POLY_MAX_RETRIES: int = 2
    POLY_RETRY_BACKOFF_SECONDS: float = 0.5
    POLY_CIRCUIT_MAX_FAILURES: int = 5
    POLY_CIRCUIT_RESET_SECONDS: int = 60
    INGEST_INTERVAL_SECONDS: int = 300
    SNAPSHOT_RETENTION_DAYS: int = 7
    ALERT_RETENTION_DAYS: int = 30
    DELIVERY_RETENTION_DAYS: int = 30
    CLEANUP_ENABLED: bool = True
    CLEANUP_SCHEDULE_HOUR_UTC: int = 3
    FAST_SIGNALS_GLOBAL_ENABLED: bool = False

    TELEGRAM_BOT_TOKEN: str | None = None
    TELEGRAM_TIMEOUT_SECONDS: int = 10
    TELEGRAM_CONNECT_TIMEOUT_SECONDS: int = 5
    TELEGRAM_MAX_RETRIES: int = 2
    TELEGRAM_RETRY_BACKOFF_SECONDS: float = 0.5
    TELEGRAM_CIRCUIT_MAX_FAILURES: int = 5
    TELEGRAM_CIRCUIT_RESET_SECONDS: int = 60

    EXECUTION_ENABLED: bool = False

    ADMIN_API_KEY: str | None = None
    OPENAI_API_KEY: str | None = None
    LLM_API_BASE: str = "https://api.openai.com/v1/chat/completions"
    LLM_MODEL: str = "gpt-4o-mini"
    LLM_TIMEOUT_SECONDS: int = 15
    LLM_CONNECT_TIMEOUT_SECONDS: int = 5
    LLM_MAX_RETRIES: int = 2
    LLM_RETRY_BACKOFF_SECONDS: float = 0.5
    LLM_CACHE_TTL_SECONDS: int = 3600
    LLM_CIRCUIT_MAX_FAILURES: int = 5
    LLM_CIRCUIT_RESET_SECONDS: int = 60

    DEFAULT_TENANT_ID: str = "default"
    RATE_LIMIT_DEFAULT_PER_MIN: int = 60
    RATE_LIMIT_WINDOW_SECONDS: int = 60
    RATE_LIMIT_ALERTS_PER_MIN: int = 60
    RATE_LIMIT_COPILOT_PER_MIN: int = 30
    RATE_LIMIT_ME_PER_MIN: int = 60
    RATE_LIMIT_WRITE_PER_MIN: int = 20
    RATE_LIMIT_AUTH_PER_MIN: int = 20
    RATE_LIMIT_IP_PER_MIN: int = 120
    EXTERNAL_MAX_CONCURRENT_POLY_CALLS: int = 4
    EXTERNAL_MAX_CONCURRENT_LLM_CALLS: int = 4
    EXTERNAL_MAX_CONCURRENT_TELEGRAM_CALLS: int = 4

    CACHE_ENABLED: bool = True
    CACHE_STALE_GRACE_SECONDS: int = 30
    CACHE_TTL_ALERTS_LATEST_SECONDS: int = 20
    CACHE_TTL_ALERT_HISTORY_SECONDS: int = 300
    CACHE_TTL_ALERT_SUMMARY_SECONDS: int = 30
    CACHE_TTL_ALERT_LAST_DIGEST_SECONDS: int = 30
    CACHE_TTL_COPILOT_FEED_SECONDS: int = 30
    CACHE_TTL_COPILOT_RUNS_SECONDS: int = 15
    CACHE_TTL_ME_SECONDS: int = 20
    CACHE_TTL_SETTINGS_SECONDS: int = 60
    CACHE_TTL_ENTITLEMENTS_SECONDS: int = 60
    CACHE_TTL_SNAPSHOTS_LATEST_SECONDS: int = 15
    CACHE_TTL_STATUS_SECONDS: int = 15
    DB_STATEMENT_TIMEOUT_SECONDS: int = 10

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
