from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    ENV: str = "dev"
    DATABASE_URL: str
    REDIS_URL: str

    POLYMARKET_BASE_URL: str
    POLY_CLOB_HOST: str = "https://clob.polymarket.com"
    POLY_CHAIN_ID: int = 137
    POLY_SIGNATURE_TYPE: int = 0
    POLY_FUNDER_ADDRESS: str | None = None
    POLY_CREDENTIALS_ENCRYPTION_KEY: str | None = None
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

    @field_validator("POLY_ORDER", "POLY_ASCENDING", mode="before")
    @classmethod
    def _empty_str_to_none(cls, value):
        if value == "":
            return None
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
