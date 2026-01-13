from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from pydantic import BaseModel
from sqlalchemy.orm import Session

from ...auth import api_key_auth, create_session_token, hash_password, verify_password
from ...cache import build_cache_key, cached_json_response
from ...core.plans import DEFAULT_PLAN_NAME
from ...db import get_db
from ...deps import (
    _get_active_session,
    _get_session_token,
    _get_session_user,
    _normalize_email,
    _resolve_default_user,
)
from ...models import Plan, User, UserAuth, UserSession
from ...rate_limit import rate_limit
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

    response.set_cookie(
        settings.SESSION_COOKIE_NAME,
        token,
        httponly=True,
        secure=settings.ENV == "prod",
        samesite="lax",
        max_age=int(timedelta(days=settings.SESSION_TTL_DAYS).total_seconds()),
        path="/",
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
    response.delete_cookie(settings.SESSION_COOKIE_NAME, path="/")
    return {"ok": True}


@router.get("/me")
def me(request: Request, db: Session = Depends(get_db)):
    api_key_header = request.headers.get("x-api-key")
    if api_key_header:
        api_key = api_key_auth(x_api_key=api_key_header, db=db)
        rate_limit(request, api_key)
        cache_key = build_cache_key(
            "me_api_key",
            request,
            tenant_id=api_key.tenant_id,
            plan_id=api_key.plan,
        )

        def _build_payload():
            user = _resolve_default_user(db)
            if not user:
                return {
                    "user_id": None,
                    "plan": api_key.plan,
                    "telegram_chat_id": None,
                    "telegram_pending": False,
                }
            plan_name = user.plan.name if user.plan else api_key.plan
            return {
                "user_id": str(user.user_id),
                "plan": plan_name,
                "telegram_chat_id": user.telegram_chat_id,
                "telegram_pending": user.telegram_chat_id is None,
            }

        return cached_json_response(
            request,
            cache_key=cache_key,
            ttl_seconds=settings.CACHE_TTL_ME_SECONDS,
            fetch_fn=_build_payload,
            private=True,
        )

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
