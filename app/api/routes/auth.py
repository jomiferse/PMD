from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from pydantic import BaseModel
from sqlalchemy.orm import Session

from ...auth import create_session_token, hash_password, verify_password
from ...cache import build_cache_key, cached_json_response
from ...core.plans import DEFAULT_PLAN_NAME
from ...db import get_db
from ...deps import (
    _get_active_session,
    _get_session_token,
    _get_session_user,
    _normalize_email,
)
from ...models import Plan, User, UserAuth, UserSession
from ...services.entitlements_service import _build_session_payload
from ...services.sessions_service import cache_session_user_id, clear_cached_session_user_id
from ...settings import settings

router = APIRouter()


class RegisterPayload(BaseModel):
    email: str
    password: str


class LoginPayload(BaseModel):
    email: str
    password: str


@router.post("/auth/register")
def register(payload: RegisterPayload, db: Session = Depends(get_db)):
    email = _normalize_email(payload.email)
    if "@" not in email:
        raise HTTPException(status_code=400, detail="invalid_email")
    if len(payload.password.strip()) < 8:
        raise HTTPException(status_code=400, detail="password_too_short")

    existing = db.query(UserAuth).filter(UserAuth.email == email).one_or_none()
    if existing:
        raise HTTPException(status_code=409, detail="email_in_use")

    plan = db.query(Plan).filter(Plan.name == DEFAULT_PLAN_NAME).one_or_none()
    display_name = email.split("@", 1)[0]
    user = User(name=display_name or "PMD User", plan_id=plan.id if plan else None)
    db.add(user)
    db.flush()

    auth = UserAuth(user_id=user.user_id, email=email, password_hash=hash_password(payload.password))
    db.add(auth)
    db.commit()
    return {"ok": True, "user_id": str(user.user_id)}


@router.post("/auth/login")
def login(payload: LoginPayload, response: Response, db: Session = Depends(get_db)):
    email = _normalize_email(payload.email)
    auth = db.query(UserAuth).filter(UserAuth.email == email).one_or_none()
    if not auth or not verify_password(payload.password, auth.password_hash):
        raise HTTPException(status_code=401, detail="invalid_credentials")

    user = db.query(User).filter(User.user_id == auth.user_id, User.is_active.is_(True)).one_or_none()
    if not user:
        raise HTTPException(status_code=401, detail="user_inactive")

    token = create_session_token()
    now_ts = datetime.now(timezone.utc)
    expires_at = now_ts + timedelta(days=settings.SESSION_TTL_DAYS)
    session = UserSession(token=token, user_id=user.user_id, expires_at=expires_at)
    db.add(session)
    db.commit()
    ttl_seconds = int((expires_at - now_ts).total_seconds())
    cache_session_user_id(token, str(user.user_id), ttl_seconds)

    secure = settings.SESSION_COOKIE_SECURE
    if secure is None:
        secure = settings.ENV.lower() in {"prod", "production"}
    if settings.SESSION_COOKIE_SAMESITE.lower() == "none":
        secure = True
    response.set_cookie(
        settings.SESSION_COOKIE_NAME,
        token,
        httponly=True,
        secure=secure,
        samesite=settings.SESSION_COOKIE_SAMESITE,
        max_age=int(timedelta(days=settings.SESSION_TTL_DAYS).total_seconds()),
        path="/",
        domain=settings.SESSION_COOKIE_DOMAIN,
    )
    return {"ok": True}


@router.post("/auth/logout")
def logout(request: Request, response: Response, db: Session = Depends(get_db)):
    token = _get_session_token(request)
    session = _get_active_session(db, token)
    if session:
        session.revoked_at = datetime.now(timezone.utc)
        db.commit()
    clear_cached_session_user_id(token)
    response.delete_cookie(
        settings.SESSION_COOKIE_NAME,
        path="/",
        domain=settings.SESSION_COOKIE_DOMAIN,
    )
    return {"ok": True}


@router.get("/me")
def me(request: Request, db: Session = Depends(get_db)):
    user = _get_session_user(db, _get_session_token(request))
    if not user:
        raise HTTPException(status_code=401, detail="not_authenticated")
    cache_key = build_cache_key(
        "me",
        request,
        user_id=str(user.user_id),
        plan_id=user.plan_id,
    )

    def _build_payload():
        return _build_session_payload(db, user)

    return cached_json_response(
        request,
        cache_key=cache_key,
        ttl_seconds=settings.CACHE_TTL_ME_SECONDS,
        fetch_fn=_build_payload,
        private=True,
    )
