from fastapi import APIRouter, Depends, Request
from sqlalchemy.orm import Session

from ...db import get_db
from ...deps import _require_session_user
from ...services.entitlements_service import build_settings_entitlements

router = APIRouter()


@router.get("/entitlements/me")
def entitlements_me(request: Request, db: Session = Depends(get_db)):
    user, _ = _require_session_user(request, db)
    return build_settings_entitlements(user)
