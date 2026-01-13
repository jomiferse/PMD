import hashlib
import json
import logging
import time
from dataclasses import dataclass
from typing import Any, Callable

from fastapi import Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse, Response

from .integrations.redis_client import redis_conn
from .settings import settings

logger = logging.getLogger(__name__)

CACHE_PREFIX = "cache"


@dataclass(frozen=True)
class CacheEntry:
    value: Any
    etag: str | None
    expires_at: float
    stale_until: float
    is_fresh: bool
    is_stale: bool


def build_cache_key(
    prefix: str,
    request: Request,
    *,
    user_id: str | None = None,
    tenant_id: str | None = None,
    plan_id: str | int | None = None,
    extra: str | None = None,
) -> str:
    return build_cache_key_from_parts(
        prefix,
        request.url.path,
        list(request.query_params.multi_items()),
        user_id=user_id,
        tenant_id=tenant_id,
        plan_id=plan_id,
        extra=extra,
    )


def build_cache_key_from_parts(
    prefix: str,
    path: str,
    query_items: list[tuple[str, str]] | None = None,
    *,
    user_id: str | None = None,
    tenant_id: str | None = None,
    plan_id: str | int | None = None,
    extra: str | None = None,
) -> str:
    raw_parts = [f"path={path}"]
    if query_items:
        normalized = "&".join(f"{key}={value}" for key, value in sorted(query_items))
        raw_parts.append(f"query={normalized}")
    if user_id:
        raw_parts.append(f"user={user_id}")
    if tenant_id:
        raw_parts.append(f"tenant={tenant_id}")
    if plan_id is not None:
        raw_parts.append(f"plan={plan_id}")
    if extra:
        raw_parts.append(f"extra={extra}")
    raw = "|".join(raw_parts)
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()
    return f"{CACHE_PREFIX}:{prefix}:{digest}"


def cached_json_response(
    request: Request,
    *,
    cache_key: str,
    ttl_seconds: int,
    fetch_fn: Callable[[], Any],
    stale_ttl_seconds: int | None = None,
    private: bool = True,
) -> Response:
    cached = cache_get(cache_key)
    if cached and cached.is_fresh:
        _set_cache_status(request, "hit")
        return _build_response(request, cached.value, cached.etag, ttl_seconds, private, is_fresh=True)

    try:
        payload = fetch_fn()
    except Exception:
        if cached and cached.is_stale:
            _set_cache_status(request, "stale")
            return _build_response(request, cached.value, cached.etag, ttl_seconds, private, is_fresh=False)
        _set_cache_status(request, "miss")
        raise

    _set_cache_status(request, "miss")
    normalized, etag = cache_set(cache_key, payload, ttl_seconds, stale_ttl_seconds)
    return _build_response(request, normalized, etag, ttl_seconds, private, is_fresh=True)


def cache_get(cache_key: str) -> CacheEntry | None:
    if not settings.CACHE_ENABLED:
        return None
    try:
        raw = redis_conn.get(cache_key)
    except Exception:
        logger.exception("cache_read_failed key=%s", cache_key)
        return None
    if not raw:
        return None
    try:
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode()
        payload = json.loads(raw)
    except Exception:
        return None
    value = payload.get("value")
    etag = payload.get("etag")
    now_ts = time.time()
    expires_at = float(payload.get("expires_at") or 0)
    stale_until = float(payload.get("stale_until") or expires_at)
    is_fresh = now_ts <= expires_at
    is_stale = not is_fresh and now_ts <= stale_until
    return CacheEntry(
        value=value,
        etag=etag,
        expires_at=expires_at,
        stale_until=stale_until,
        is_fresh=is_fresh,
        is_stale=is_stale,
    )


def cache_set(
    cache_key: str,
    payload: Any,
    ttl_seconds: int,
    stale_ttl_seconds: int | None = None,
) -> tuple[Any, str | None]:
    normalized = jsonable_encoder(payload)
    etag = _compute_etag(normalized)
    if not settings.CACHE_ENABLED:
        return normalized, etag

    ttl_seconds = max(int(ttl_seconds), 1)
    stale_ttl = max(int(stale_ttl_seconds or settings.CACHE_STALE_GRACE_SECONDS), 0)
    now_ts = time.time()
    expires_at = now_ts + ttl_seconds
    stale_until = expires_at + stale_ttl
    payload_wrapper = {
        "value": normalized,
        "etag": etag,
        "expires_at": expires_at,
        "stale_until": stale_until,
        "cached_at": now_ts,
    }
    try:
        redis_conn.set(
            cache_key,
            json.dumps(payload_wrapper, ensure_ascii=True),
            ex=int(max(stale_until - now_ts, 1)),
        )
    except Exception:
        logger.exception("cache_write_failed key=%s", cache_key)
    return normalized, etag


def invalidate_cache_key(cache_key: str) -> None:
    if not cache_key:
        return
    try:
        redis_conn.delete(cache_key)
    except Exception:
        logger.exception("cache_invalidate_failed key=%s", cache_key)


def invalidate_cache_prefix(prefix: str) -> None:
    if not prefix:
        return
    pattern = f"{CACHE_PREFIX}:{prefix}:*"
    try:
        scan_iter = getattr(redis_conn, "scan_iter", None)
        keys = list(scan_iter(match=pattern)) if scan_iter else redis_conn.keys(pattern)
    except Exception:
        logger.exception("cache_scan_failed prefix=%s", prefix)
        return
    if not keys:
        return
    try:
        redis_conn.delete(*keys)
    except Exception:
        logger.exception("cache_bulk_delete_failed prefix=%s", prefix)


def invalidate_user_caches(user_id: str, plan_id: str | int | None = None) -> None:
    if not user_id:
        return
    invalidate_cache_key(
        build_cache_key_from_parts(
            "me",
            "/me",
            user_id=user_id,
            plan_id=plan_id,
        )
    )
    invalidate_cache_key(
        build_cache_key_from_parts(
            "settings_me",
            "/settings/me",
            user_id=user_id,
            plan_id=plan_id,
        )
    )
    invalidate_cache_key(
        build_cache_key_from_parts(
            "entitlements_me",
            "/entitlements/me",
            user_id=user_id,
            plan_id=plan_id,
        )
    )


def apply_cache_headers(
    response: Response,
    *,
    etag: str | None,
    max_age: int,
    private: bool,
) -> None:
    scope = "private" if private else "public"
    response.headers["Cache-Control"] = f"{scope}, max-age={max(max_age, 0)}"
    if etag:
        response.headers["ETag"] = f"\"{etag}\""


def _build_response(
    request: Request,
    payload: Any,
    etag: str | None,
    ttl_seconds: int,
    private: bool,
    *,
    is_fresh: bool,
) -> Response:
    response = JSONResponse(content=payload)
    cache_age = ttl_seconds if is_fresh else 0
    apply_cache_headers(response, etag=etag, max_age=cache_age, private=private)
    if etag and _etag_matches(request, etag):
        return Response(status_code=304, headers=dict(response.headers))
    return response


def _etag_matches(request: Request, etag: str) -> bool:
    header = request.headers.get("if-none-match")
    if not header:
        return False
    candidates = [part.strip() for part in header.split(",") if part.strip()]
    quoted = f"\"{etag}\""
    weak = f"W/{quoted}"
    return quoted in candidates or weak in candidates or etag in candidates


def _compute_etag(payload: Any) -> str:
    raw = json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _set_cache_status(request: Request, status: str) -> None:
    try:
        request.state.cache_status = status
    except Exception:
        return
