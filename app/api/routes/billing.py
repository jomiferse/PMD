from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel
from sqlalchemy.orm import Session

from ...db import get_db
from ...deps import _require_session_user
from ...services.stripe_service import _create_checkout_session, _create_portal_session

router = APIRouter()


class CheckoutPayload(BaseModel):
    plan_id: str


@router.post("/billing/checkout-session")
def billing_checkout_session(
    payload: CheckoutPayload,
    request: Request,
    db: Session = Depends(get_db),
):
    user, auth = _require_session_user(request, db)
    checkout_url = _create_checkout_session(db, user, auth, payload.plan_id)
    return {"checkout_url": checkout_url}


@router.post("/billing/portal-session")
def billing_portal_session(request: Request, db: Session = Depends(get_db)):
    user, auth = _require_session_user(request, db)
    portal_url = _create_portal_session(db, user, auth)
    return {"portal_url": portal_url}
