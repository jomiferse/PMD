from datetime import datetime, timezone

from fastapi import HTTPException, Request
from sqlalchemy.orm import Session

from ..integrations.redis_client import redis_conn
from ..models import User, UserAuth, UserSession
from ..settings import settings

SESSION_USER_CACHE_KEY = "session:user:{token}"


def _normalize_email(raw: str) -> str:
    return raw.strip().lower()


def _get_session_token(request: Request) -> str | None:
    return request.cookies.get(settings.SESSION_COOKIE_NAME)


def get_cached_session_user_id(token: str | None) -> str | None:
    if not token:
        return None
    key = SESSION_USER_CACHE_KEY.format(token=token)
    try:
        cached = redis_conn.get(key)
    except Exception:
        return None
    if not cached:
        return None
    if isinstance(cached, (bytes, bytearray)):
        return cached.decode()
    return str(cached)


def cache_session_user_id(token: str, user_id: str, ttl_seconds: int) -> None:
    if not token or not user_id or ttl_seconds <= 0:
        return
    key = SESSION_USER_CACHE_KEY.format(token=token)
    try:
        redis_conn.set(key, str(user_id), ex=ttl_seconds)
    except Exception:
        return


def clear_cached_session_user_id(token: str | None) -> None:
    if not token:
        return
    key = SESSION_USER_CACHE_KEY.format(token=token)
    try:
        redis_conn.delete(key)
    except Exception:
        return


def _get_active_session(db: Session, token: str | None) -> UserSession | None:
    if not token:
        return None
    now_ts = datetime.now(timezone.utc)
    return (
        db.query(UserSession)
        .filter(
            UserSession.token == token,
            UserSession.revoked_at.is_(None),
            UserSession.expires_at > now_ts,
        )
        .one_or_none()
    )


def _get_session_user(db: Session, token: str | None) -> User | None:
    session = _get_active_session(db, token)
    if not session:
        return None
    return (
        db.query(User)
        .filter(User.user_id == session.user_id, User.is_active.is_(True))
        .one_or_none()
    )


def _require_session_user(request: Request, db: Session) -> tuple[User, UserAuth | None]:
    user = _get_session_user(db, _get_session_token(request))
    if not user:
        raise HTTPException(status_code=401, detail="not_authenticated")
    auth = db.query(UserAuth).filter(UserAuth.user_id == user.user_id).one_or_none()
    return user, auth


def _resolve_default_user(db: Session) -> User | None:
    return (
        db.query(User)
        .filter(User.is_active.is_(True))
        .order_by(User.created_at.desc())
        .first()
    )
