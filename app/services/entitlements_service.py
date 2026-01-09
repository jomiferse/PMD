from sqlalchemy.orm import Session

from ..models import Plan, User, UserAuth
from .stripe_service import _get_latest_subscription


def _build_entitlements(plan: Plan | None) -> dict[str, object]:
    if not plan:
        return {}
    return {
        "plan": plan.name,
        "digest_window": plan.digest_window_minutes,
        "max_themes": plan.max_themes_per_digest,
        "allowed_strengths": plan.allowed_strengths,
        "copilot_enabled": plan.copilot_enabled,
        "fast_mode": plan.fast_mode,
        "caps": {
            "max_copilot_per_day": plan.max_copilot_per_day,
            "max_copilot_per_hour": plan.max_copilot_per_hour,
            "max_copilot_per_digest": plan.max_copilot_per_digest,
            "max_fast_copilot_per_day": plan.max_fast_copilot_per_day,
        },
    }


def _build_session_payload(db: Session, user: User) -> dict[str, object]:
    auth = db.query(UserAuth).filter(UserAuth.user_id == user.user_id).one_or_none()
    subscription = _get_latest_subscription(db, user.user_id)
    plan = subscription.plan if subscription else None
    subscription_payload = None
    if subscription:
        subscription_payload = {
            "status": subscription.status,
            "plan_id": subscription.plan_id,
            "plan_name": plan.name if plan else None,
            "current_period_end": subscription.current_period_end.isoformat() if subscription.current_period_end else None,
            "stripe_customer_id": subscription.stripe_customer_id,
        }
    return {
        "user": {
            "id": str(user.user_id),
            "email": auth.email if auth else "",
            "name": user.name,
        },
        "subscription": subscription_payload,
        "entitlements": _build_entitlements(plan),
    }
