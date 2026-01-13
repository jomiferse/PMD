import logging
import time

from starlette.middleware.base import BaseHTTPMiddleware

logger = logging.getLogger("app.http")


class RequestLoggingMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        start = time.monotonic()
        status_code = 500
        try:
            response = await call_next(request)
            status_code = response.status_code
            return response
        finally:
            duration_ms = int((time.monotonic() - start) * 1000)
            cache_status = getattr(request.state, "cache_status", "none")
            scope = getattr(request.state, "rate_limit_subject", "unknown")
            logger.info(
                "http_request method=%s path=%s status=%s duration_ms=%s cache=%s scope=%s",
                request.method.upper(),
                request.url.path,
                status_code,
                duration_ms,
                cache_status,
                scope,
            )
