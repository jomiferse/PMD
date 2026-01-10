import uuid

from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy.orm import Session
import stripe

from ...db import get_db
from ...models import StripeEvent, Subscription
from ...services.stripe_service import (
    _get_latest_subscription,
    _require_stripe,
    _sync_subscription_from_stripe,
)
from ...services.telegram_service import handle_telegram_webhook
from ...settings import settings

router = APIRouter()


@router.post("/webhooks/stripe")
async def stripe_webhook(request: Request, db: Session = Depends(get_db)):
    if not settings.STRIPE_WEBHOOK_SECRET:
        raise HTTPException(status_code=500, detail="stripe_not_configured")
    _require_stripe()

    payload = await request.body()
    sig_header = request.headers.get("stripe-signature")
    if not sig_header:
        raise HTTPException(status_code=400, detail="missing_signature")

    try:
        event = stripe.Webhook.construct_event(payload, sig_header, settings.STRIPE_WEBHOOK_SECRET)
    except stripe.error.SignatureVerificationError:
        raise HTTPException(status_code=400, detail="invalid_signature")

    existing = db.query(StripeEvent).filter(StripeEvent.event_id == event["id"]).one_or_none()
    if existing:
        return {"ok": True}
    db.add(StripeEvent(event_id=event["id"]))
    db.commit()

    event_type = event["type"]
    data_object = event["data"]["object"]

    if event_type == "checkout.session.completed":
        subscription_id = data_object.get("subscription")
        customer_id = data_object.get("customer")
        metadata = data_object.get("metadata") or {}
        user_id = metadata.get("user_id")
        try:
            user_uuid = uuid.UUID(user_id) if user_id else None
        except ValueError:
            user_uuid = None
        if subscription_id:
            subscription_obj = stripe.Subscription.retrieve(subscription_id)
            _sync_subscription_from_stripe(db, subscription_obj, user_uuid)
        elif customer_id and user_uuid:
            sub = _get_latest_subscription(db, user_uuid)
            if sub:
                sub.stripe_customer_id = customer_id
                db.commit()
    elif event_type in {"customer.subscription.created", "customer.subscription.updated", "customer.subscription.deleted"}:
        metadata = data_object.get("metadata") or {}
        user_id = metadata.get("user_id")
        try:
            user_uuid = uuid.UUID(user_id) if user_id else None
        except ValueError:
            user_uuid = None
        _sync_subscription_from_stripe(db, data_object, user_uuid)
    elif event_type == "invoice.paid":
        subscription_id = data_object.get("subscription")
        customer_id = data_object.get("customer")
        if subscription_id:
            subscription_obj = stripe.Subscription.retrieve(subscription_id)
            _sync_subscription_from_stripe(db, subscription_obj, None)
        elif customer_id:
            subscription = (
                db.query(Subscription)
                .filter(Subscription.stripe_customer_id == customer_id)
                .order_by(Subscription.created_at.desc())
                .first()
            )
            if subscription and subscription.stripe_subscription_id:
                subscription_obj = stripe.Subscription.retrieve(subscription.stripe_subscription_id)
                _sync_subscription_from_stripe(db, subscription_obj, subscription.user_id)

    return {"ok": True}


@router.post("/telegram/webhook")
async def telegram_webhook(request: Request, db: Session = Depends(get_db)):
    payload = await request.json()
    return handle_telegram_webhook(db, payload)
