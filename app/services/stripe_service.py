from datetime import datetime, timezone
import uuid

from fastapi import HTTPException
from sqlalchemy.orm import Session
import stripe

from ..models import Plan, Subscription, User, UserAuth
from ..settings import settings


def _price_by_plan() -> dict[str, str]:
    return {
        "basic": settings.STRIPE_BASIC_PRICE_ID or "",
        "pro": settings.STRIPE_PRO_PRICE_ID or "",
        "elite": settings.STRIPE_ELITE_PRICE_ID or "",
    }


def _plan_for_price_id(price_id: str) -> str | None:
    for plan_name, stored_price in _price_by_plan().items():
        if stored_price and stored_price == price_id:
            return plan_name
    return None


def _require_stripe() -> None:
    if not settings.STRIPE_SECRET_KEY:
        raise HTTPException(status_code=500, detail="stripe_not_configured")
    stripe.api_key = settings.STRIPE_SECRET_KEY


def _get_latest_subscription(db: Session, user_id: uuid.UUID) -> Subscription | None:
    return (
        db.query(Subscription)
        .filter(Subscription.user_id == user_id)
        .order_by(Subscription.created_at.desc())
        .first()
    )


def _get_or_create_customer_id(db: Session, user: User, auth: UserAuth | None) -> str:
    subscription = _get_latest_subscription(db, user.user_id)
    if subscription and subscription.stripe_customer_id:
        return subscription.stripe_customer_id

    _require_stripe()
    customer = stripe.Customer.create(
        email=auth.email if auth else None,
        metadata={"user_id": str(user.user_id)},
    )

    if subscription:
        subscription.stripe_customer_id = customer.id
        db.commit()
    else:
        db.add(
            Subscription(
                user_id=user.user_id,
                status="incomplete",
                stripe_customer_id=customer.id,
            )
        )
        db.commit()
    return customer.id


def _sync_subscription_from_stripe(
    db: Session,
    subscription_payload: dict,
    user_id: uuid.UUID | None = None,
) -> Subscription:
    stripe_subscription_id = subscription_payload.get("id")
    stripe_customer_id = subscription_payload.get("customer")
    status = subscription_payload.get("status") or "incomplete"
    current_period_end = subscription_payload.get("current_period_end")
    current_period_end_dt = None
    if current_period_end:
        current_period_end_dt = datetime.fromtimestamp(int(current_period_end), tz=timezone.utc)

    items = subscription_payload.get("items", {}).get("data", [])
    price_id = None
    if items:
        price_id = items[0].get("price", {}).get("id")
    plan_name = _plan_for_price_id(price_id) if price_id else None

    plan = None
    if plan_name:
        plan = db.query(Plan).filter(Plan.name == plan_name).one_or_none()

    subscription = None
    if stripe_subscription_id:
        subscription = (
            db.query(Subscription)
            .filter(Subscription.stripe_subscription_id == stripe_subscription_id)
            .one_or_none()
        )
    if not subscription and stripe_customer_id:
        subscription = (
            db.query(Subscription)
            .filter(Subscription.stripe_customer_id == stripe_customer_id)
            .order_by(Subscription.created_at.desc())
            .first()
        )
    if not subscription and user_id:
        subscription = (
            db.query(Subscription)
            .filter(Subscription.user_id == user_id)
            .order_by(Subscription.created_at.desc())
            .first()
        )

    if subscription is None:
        if not user_id:
            raise HTTPException(status_code=400, detail="subscription_user_missing")
        subscription = Subscription(user_id=user_id, status=status)

    subscription.status = status
    subscription.current_period_end = current_period_end_dt
    subscription.stripe_customer_id = stripe_customer_id
    subscription.stripe_subscription_id = stripe_subscription_id
    subscription.plan_id = plan.id if plan else subscription.plan_id

    db.add(subscription)
    db.commit()

    if user_id and plan and status in {"active", "trialing"}:
        user = db.query(User).filter(User.user_id == user_id).one_or_none()
        if user and user.plan_id != plan.id:
            user.plan_id = plan.id
            db.commit()

    return subscription


def _create_checkout_session(
    db: Session,
    user: User,
    auth: UserAuth | None,
    raw_plan_id: str,
) -> str:
    plan_name = raw_plan_id.strip().lower()
    price_id = _price_by_plan().get(plan_name)
    if not price_id:
        raise HTTPException(status_code=400, detail="invalid_plan")

    plan = db.query(Plan).filter(Plan.name == plan_name).one_or_none()
    if not plan:
        raise HTTPException(status_code=400, detail="plan_not_found")

    _require_stripe()
    customer_id = _get_or_create_customer_id(db, user, auth)
    session = stripe.checkout.Session.create(
        customer=customer_id,
        mode="subscription",
        line_items=[{"price": price_id, "quantity": 1}],
        subscription_data={
            "metadata": {"user_id": str(user.user_id), "plan_name": plan_name},
        },
        success_url=f"{settings.APP_URL}/app?checkout=success",
        cancel_url=f"{settings.APP_URL}/app/billing?checkout=cancel",
    )

    subscription = _get_latest_subscription(db, user.user_id)
    if subscription:
        subscription.plan_id = plan.id
        subscription.status = "incomplete"
        db.commit()

    if not session.url:
        raise HTTPException(status_code=500, detail="checkout_session_missing_url")
    return session.url


def _create_portal_session(
    db: Session,
    user: User,
    auth: UserAuth | None,
) -> str:
    _require_stripe()

    customer_id = _get_or_create_customer_id(db, user, auth)
    portal = stripe.billing_portal.Session.create(
        customer=customer_id,
        return_url=f"{settings.APP_URL}/app/billing",
    )
    if not portal.url:
        raise HTTPException(status_code=500, detail="portal_session_missing_url")
    return portal.url
