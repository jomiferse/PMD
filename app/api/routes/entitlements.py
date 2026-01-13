from fastapi import APIRouter, Depends, Request
from sqlalchemy.orm import Session

from ...cache import build_cache_key, cached_json_response
from ...db import get_db
from ...deps import _require_session_user
from ...services.entitlements_service import build_settings_entitlements
from ...settings import settings

router = APIRouter()


@router.get("/entitlements/me")
def entitlements_me(request: Request, db: Session = Depends(get_db)):
    user, _ = _require_session_user(request, db)
    cache_key = build_cache_key(
        "entitlements_me",
        request,
        user_id=str(user.user_id),
        plan_id=user.plan_id,
    )

    def _build_payload():
        return build_settings_entitlements(user)

    return cached_json_response(
        request,
        cache_key=cache_key,
        ttl_seconds=settings.CACHE_TTL_ENTITLEMENTS_SECONDS,
        fetch_fn=_build_payload,
        private=True,
    )
