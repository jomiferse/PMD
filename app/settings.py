from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    ENV: str = "dev"
    DATABASE_URL: str
    REDIS_URL: str

    POLYMARKET_BASE_URL: str
    POLY_LIMIT: int = 100
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

    MIN_LIQUIDITY: float = 1000.0
    MIN_VOLUME_24H: float = 1000.0
    MIN_PRICE_THRESHOLD: float = 0.02
    MIN_ABS_MOVE: float = 0.01
    FLOOR_PRICE: float = 0.05
    WINDOW_MINUTES: int = 60
    ALERT_COOLDOWN_MINUTES: int = 30
    DIGEST_WINDOW_MINUTES: int = 60
    FAST_SIGNALS_ENABLED: bool = False
    FAST_WINDOW_MINUTES: int = 15
    FAST_MIN_LIQUIDITY: float = 20000.0
    FAST_MIN_VOLUME_24H: float = 20000.0
    FAST_MIN_ABS_MOVE: float = 0.015
    FAST_MIN_PCT_MOVE: float = 0.05
    FAST_PYES_MIN: float = 0.15
    FAST_PYES_MAX: float = 0.85
    FAST_COOLDOWN_MINUTES: int = 10
    FAST_MAX_THEMES_PER_DIGEST: int = 2
    FAST_MAX_MARKETS_PER_THEME: int = 2
    FAST_DIGEST_MODE: str = "separate"

    STRONG_ABS_MOVE_THRESHOLD: float = 0.02
    STRONG_MIN_LIQUIDITY: float = 5000.0
    STRONG_MIN_VOLUME_24H: float = 5000.0
    MEDIUM_MOVE_THRESHOLD: float = 0.05
    MEDIUM_ABS_MOVE_THRESHOLD: float = 0.01
    MEDIUM_MIN_LIQUIDITY: float = 1000.0
    MEDIUM_MIN_VOLUME_24H: float = 1000.0
    MAX_STRONG_ALERTS: int = 5
    MAX_MEDIUM_ALERTS: int = 3

    TELEGRAM_BOT_TOKEN: str | None = None

    ADMIN_API_KEY: str | None = None
    OPENAI_API_KEY: str | None = None
    LLM_API_BASE: str = "https://api.openai.com/v1/chat/completions"
    LLM_MODEL: str = "gpt-4o-mini"
    LLM_TIMEOUT_SECONDS: int = 15
    LLM_MAX_RETRIES: int = 2
    LLM_CACHE_TTL_SECONDS: int = 3600

    GLOBAL_MIN_LIQUIDITY: float = 1000.0
    GLOBAL_MIN_VOLUME_24H: float = 1000.0
    GLOBAL_DIGEST_WINDOW: int = 60
    GLOBAL_MAX_ALERTS: int = 7
    PYES_ACTIONABLE_MIN: float = 0.15
    PYES_ACTIONABLE_MAX: float = 0.85
    MAX_ACTIONABLE_PER_DIGEST: int = 5
    DIGEST_ACTIONABLE_ONLY: bool = True
    THEME_GROUPING_ENABLED: bool = True
    MAX_THEMES_PER_DIGEST: int = 5
    MAX_RELATED_MARKETS_PER_THEME: int = 3
    MAX_AI_RECS_PER_DAY: int = 5
    MAX_AI_RECS_PER_DIGEST: int = 2
    MAX_COPILOT_PER_DAY: int = 5
    MAX_COPILOT_THEMES_PER_DIGEST: int = 1
    COPILOT_THEME_DEDUPE_TTL_SECONDS: int = 21600
    AI_RECOMMENDATION_EXPIRES_MINUTES: int = 30

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
