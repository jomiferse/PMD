import hashlib
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone

from fastapi import Depends, HTTPException, Request
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse

from .auth import api_key_auth
from .db import SessionLocal
from .models import ApiKey, UserSession
from .services.sessions_service import cache_session_user_id, get_cached_session_user_id
from .integrations.redis_client import redis_conn
from .settings import settings

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RateLimitResult:
    allowed: bool
    remaining: int
    retry_after: int
    limit: int


class RateLimitMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        if request.method.upper() == "OPTIONS":
            return await call_next(request)
        path = request.url.path
        method = request.method.upper()
        rule = _rule_for_request(path, method)

        if rule:
            rule_name, limit = rule
            scope_type, scope_id = _resolve_scope_identity(request)
            if scope_id:
                scope_key = f"{scope_type}:{scope_id}:{rule_name}"
                result = _apply_rate_limit(scope_key, limit, settings.RATE_LIMIT_WINDOW_SECONDS)
                request.state.rate_limit_scope = rule_name
                request.state.rate_limit_subject = scope_type
                request.state.rate_limit_applied = True
                if not result.allowed:
                    return _rate_limited_response(result.retry_after)

        ip_limit = settings.RATE_LIMIT_IP_PER_MIN
        client_ip = _get_client_ip(request)
        if client_ip and ip_limit > 0:
            ip_key = f"ip:{client_ip}"
            result = _apply_rate_limit(ip_key, ip_limit, settings.RATE_LIMIT_WINDOW_SECONDS)
            request.state.rate_limit_applied = True
            if not getattr(request.state, "rate_limit_subject", None):
                request.state.rate_limit_subject = "ip"
                request.state.rate_limit_scope = "ip_cap"
            if not result.allowed:
                return _rate_limited_response(result.retry_after)

        response = await call_next(request)
        return response


def rate_limit(request: Request, api_key: ApiKey = Depends(api_key_auth)) -> ApiKey:
    limit = api_key.rate_limit_per_min or settings.RATE_LIMIT_DEFAULT_PER_MIN
    if limit <= 0:
        return api_key

    scope_key = f"api_key_global:{api_key.id}"
    result = _apply_rate_limit(scope_key, limit, settings.RATE_LIMIT_WINDOW_SECONDS)
    request.state.rate_limit_scope = "api_key_global"
    request.state.rate_limit_subject = "api_key"
    request.state.rate_limit_applied = True

    if not result.allowed:
        raise HTTPException(
            status_code=429,
            detail="Rate limit exceeded",
            headers={"Retry-After": str(result.retry_after)},
        )
    return api_key


def _rule_for_request(path: str, method: str) -> tuple[str, int] | None:
    if method == "POST" and path == "/auth/logout":
        return "session_read", settings.RATE_LIMIT_ME_PER_MIN
    if method == "GET" and path.startswith("/alerts"):
        return "alerts_read", settings.RATE_LIMIT_ALERTS_PER_MIN
    if method == "GET" and path == "/status":
        return "alerts_read", settings.RATE_LIMIT_ALERTS_PER_MIN
    if method == "GET" and path.startswith("/copilot"):
        return "copilot_read", settings.RATE_LIMIT_COPILOT_PER_MIN
    if method == "GET" and path in {"/me", "/settings/me", "/entitlements/me"}:
        return "session_read", settings.RATE_LIMIT_ME_PER_MIN
    if method == "GET" and path.startswith("/snapshots"):
        return "alerts_read", settings.RATE_LIMIT_ALERTS_PER_MIN
    if method in {"POST", "PUT", "PATCH", "DELETE"} and path.startswith("/settings"):
        return "write", settings.RATE_LIMIT_WRITE_PER_MIN
    if method in {"POST", "PUT", "PATCH", "DELETE"} and path.startswith("/billing"):
        return "write", settings.RATE_LIMIT_WRITE_PER_MIN
    if method in {"POST", "PUT", "PATCH", "DELETE"} and path.startswith("/jobs"):
        return "write", settings.RATE_LIMIT_WRITE_PER_MIN
    if method == "POST" and path.startswith("/auth"):
        return "auth", settings.RATE_LIMIT_AUTH_PER_MIN
    return None


def _resolve_scope_identity(request: Request) -> tuple[str, str | None]:
    api_key_header = (request.headers.get("x-api-key") or "").strip()
    if api_key_header:
        digest = hashlib.sha256(api_key_header.encode("utf-8")).hexdigest()
        return "api_key", digest

    token = request.cookies.get(settings.SESSION_COOKIE_NAME)
    user_id = _resolve_session_user_id(token)
    if user_id:
        return "user", user_id

    return "ip", _get_client_ip(request)


def _resolve_session_user_id(token: str | None) -> str | None:
    cached = get_cached_session_user_id(token)
    if cached:
        return cached
    if not token:
        return None

    db = SessionLocal()
    try:
        now_ts = datetime.now(timezone.utc)
        session = (
            db.query(UserSession)
            .filter(
                UserSession.token == token,
                UserSession.revoked_at.is_(None),
                UserSession.expires_at > now_ts,
            )
            .one_or_none()
        )
        if not session:
            return None
        ttl_seconds = int((session.expires_at - now_ts).total_seconds())
        user_id = str(session.user_id)
        cache_session_user_id(token, user_id, ttl_seconds)
        return user_id
    finally:
        db.close()


def _apply_rate_limit(scope_key: str, limit: int, window_seconds: int) -> RateLimitResult:
    if not scope_key or limit <= 0 or window_seconds <= 0:
        return RateLimitResult(True, limit, window_seconds, limit)

    bucket = int(time.time() // window_seconds)
    redis_key = f"rate:{scope_key}:{bucket}"
    try:
        count = redis_conn.incr(redis_key)
        if count == 1:
            redis_conn.expire(redis_key, window_seconds * 2)
    except Exception:
        logger.exception("rate_limit_failed key=%s", redis_key)
        return RateLimitResult(True, limit, window_seconds, limit)

    remaining = max(limit - count, 0)
    retry_after = max(int(window_seconds - (time.time() % window_seconds)), 1)
    return RateLimitResult(count <= limit, remaining, retry_after, limit)


def _rate_limited_response(retry_after: int) -> JSONResponse:
    return JSONResponse(
        {"detail": "Rate limit exceeded"},
        status_code=429,
        headers={"Retry-After": str(retry_after)},
    )


def _get_client_ip(request: Request) -> str | None:
    forwarded = request.headers.get("x-forwarded-for")
    if forwarded:
        parts = [part.strip() for part in forwarded.split(",") if part.strip()]
        if parts:
            return parts[0]
    if request.client:
        return request.client.host
    return None
