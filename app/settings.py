from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    ENV: str = "dev"
    DATABASE_URL: str
    REDIS_URL: str

    POLYMARKET_BASE_URL: str
    POLY_LIMIT: int = 100
    INGEST_INTERVAL_SECONDS: int = 300

    EDGE_THRESHOLD: float = 0.08
    MIN_LIQUIDITY: float = 1000.0
    MIN_VOLUME_24H: float = 1000.0
    MOVE_THRESHOLD: float = 0.05
    WINDOW_MINUTES: int = 60
    ALERT_COOLDOWN_MINUTES: int = 30

    TELEGRAM_BOT_TOKEN: str | None = None
    TELEGRAM_CHAT_ID: str | None = None
    TELEGRAM_THROTTLE_SECONDS: int = 900

    DEFAULT_TENANT_ID: str = "default"
    RATE_LIMIT_DEFAULT_PER_MIN: int = 60

    LOG_LEVEL: str = "INFO"
    LOG_JSON: bool = True

settings = Settings()
