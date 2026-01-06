import logging
import time

import httpx

from .settings import settings

logger = logging.getLogger("app.http")


def _slow_threshold_seconds() -> float:
    try:
        return max(float(settings.HTTPX_SLOW_REQUEST_THRESHOLD_SECONDS), 0.0)
    except (TypeError, ValueError):
        return 0.0


def log_httpx_response(
    response: httpx.Response,
    duration_seconds: float,
    *,
    log_error: bool = True,
) -> None:
    threshold = _slow_threshold_seconds()
    is_slow = threshold > 0 and duration_seconds >= threshold
    is_error = not response.is_success
    if not is_slow and not (log_error and is_error):
        return
    if is_slow and is_error:
        tag = "error_slow"
    elif is_error:
        tag = "error"
    else:
        tag = "slow"
    logger.warning(
        "httpx_request_%s method=%s url=%s status=%s latency_ms=%s",
        tag,
        response.request.method,
        response.request.url,
        response.status_code,
        int(duration_seconds * 1000),
    )


class HttpxTimer:
    def __init__(self) -> None:
        self._start = time.monotonic()

    def elapsed(self) -> float:
        return time.monotonic() - self._start
