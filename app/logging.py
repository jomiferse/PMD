import json
import logging
from datetime import datetime, timezone

from .settings import settings

_MODULE_LEVELS = {
    "app.polymarket.client": logging.WARNING,
    "app.core.alerts": logging.INFO,
    "app.core.ai_copilot": logging.INFO,
    "app.ingestion.polymarket": logging.INFO,
    "httpx": logging.WARNING,
    "httpcore": logging.WARNING,
}

class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        try:
            message = record.getMessage()
        except TypeError:
            # Guard against mismatched printf-style arguments; fallback to raw message.
            message = str(record.msg)
        payload = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": message,
        }
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=True)


def configure_logging() -> None:
    root = logging.getLogger()
    root_level = _coerce_log_level(settings.LOG_LEVEL, default=logging.INFO)
    if settings.ENV.lower() == "prod" and root_level < logging.INFO:
        root_level = logging.INFO
    root.setLevel(root_level)

    handler = logging.StreamHandler()
    if settings.LOG_JSON:
        handler.setFormatter(JsonFormatter())
    else:
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s"))

    root.handlers = [handler]

    for name, level in _MODULE_LEVELS.items():
        logger = logging.getLogger(name)
        target_level = level
        if root_level <= logging.DEBUG and name not in {"httpx", "httpcore"}:
            target_level = logging.DEBUG
        logger.setLevel(target_level)


def _coerce_log_level(value: str, default: int) -> int:
    if not value:
        return default
    level = logging.getLevelName(str(value).upper())
    return level if isinstance(level, int) else default
