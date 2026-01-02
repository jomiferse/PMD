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
    MIN_PRICE_THRESHOLD: float = 0.02
    MIN_ABS_MOVE: float = 0.01
    FLOOR_PRICE: float = 0.05
    WINDOW_MINUTES: int = 60
    ALERT_COOLDOWN_MINUTES: int = 30
    DIGEST_WINDOW_MINUTES: int = 60

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
    TELEGRAM_CHAT_ID: str | None = None
    TELEGRAM_THROTTLE_SECONDS: int = 900
    TELEGRAM_MAX_ALERTS: int = 7
    TELEGRAM_STRONG_MOVE_PCT: float = 0.15

    DEFAULT_TENANT_ID: str = "default"
    RATE_LIMIT_DEFAULT_PER_MIN: int = 60

    LOG_LEVEL: str = "INFO"
    LOG_JSON: bool = True

settings = Settings()
