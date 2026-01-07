import json
import logging
import logging.config
from datetime import datetime, timezone

from ..settings import settings

_CONFIGURED = False

_STANDARD_ATTRS = {
    "name",
    "msg",
    "args",
    "levelname",
    "levelno",
    "pathname",
    "filename",
    "module",
    "exc_info",
    "exc_text",
    "stack_info",
    "lineno",
    "funcName",
    "created",
    "msecs",
    "relativeCreated",
    "thread",
    "threadName",
    "processName",
    "process",
}


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        try:
            message = record.getMessage()
        except TypeError:
            message = str(record.msg)
        payload = {
            "ts": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": message,
        }
        extra = _extract_extra(record)
        if extra:
            payload["extra"] = extra
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=True)


def configure_logging() -> None:
    global _CONFIGURED
    if _CONFIGURED:
        return

    root_level = _coerce_log_level(settings.LOG_LEVEL, default=logging.INFO)
    handler_name = "json" if settings.LOG_JSON else "plain"

    config = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "json": {
                "()": "app.core.logging_config.JsonFormatter",
            },
            "plain": {
                "class": "logging.Formatter",
                "format": "%(levelname)s %(name)s %(message)s",
            },
        },
        "handlers": {
            "default": {
                "class": "logging.StreamHandler",
                "formatter": handler_name,
            },
        },
        "root": {
            "level": root_level,
            "handlers": ["default"],
        },
        "loggers": _build_logger_levels(root_level),
    }

    logging.config.dictConfig(config)
    _CONFIGURED = True


def _build_logger_levels(root_level: int) -> dict[str, dict]:
    return {
        "rq": _logger_config(_rq_level(root_level), handlers=[]),
        "rq.worker": _logger_config(_rq_level(root_level), handlers=[]),
        "httpx": _logger_config(_httpx_level(root_level), handlers=[]),
        "httpcore": _logger_config(_httpx_level(root_level), handlers=[]),
        "uvicorn": _logger_config(root_level, handlers=["default"], propagate=False),
        "uvicorn.error": _logger_config(root_level, handlers=["default"], propagate=False),
        "uvicorn.access": _logger_config(_uvicorn_access_level(root_level), handlers=["default"], propagate=False),
        "sqlalchemy.engine": _logger_config(_sqlalchemy_level(root_level), handlers=[]),
    }


def _logger_config(level: int, handlers: list[str] | None = None, propagate: bool = True) -> dict:
    config = {"level": level, "propagate": propagate}
    if handlers is not None:
        config["handlers"] = handlers
    return config


def _rq_level(root_level: int) -> int:
    return root_level if root_level <= logging.INFO else root_level


def _httpx_level(root_level: int) -> int:
    if root_level <= logging.DEBUG:
        return logging.DEBUG
    return logging.WARNING if root_level <= logging.WARNING else root_level


def _uvicorn_access_level(root_level: int) -> int:
    if root_level <= logging.DEBUG:
        return logging.INFO
    return logging.WARNING if root_level <= logging.WARNING else root_level


def _sqlalchemy_level(root_level: int) -> int:
    if root_level <= logging.DEBUG:
        return logging.DEBUG
    return logging.WARNING if root_level <= logging.WARNING else root_level


def _extract_extra(record: logging.LogRecord) -> dict:
    extras = {}
    for key, value in record.__dict__.items():
        if key in _STANDARD_ATTRS:
            continue
        if key.startswith("_"):
            continue
        extras[key] = value
    return extras


def _coerce_log_level(value: str, default: int) -> int:
    if not value:
        return default
    level = logging.getLevelName(str(value).upper())
    return level if isinstance(level, int) else default
