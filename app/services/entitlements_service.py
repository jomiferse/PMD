from datetime import datetime, timezone
from typing import Iterable
import uuid

from sqlalchemy.orm import Session

from ..core.effective_settings import EffectiveSettings, resolve_effective_settings
from ..core.plans import upgrade_target_name
from ..models import Plan, Subscription, User, UserAuth


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


def _build_settings_limits(effective: EffectiveSettings) -> dict[str, dict[str, object]]:
    return {
        "min_liquidity": {"min": effective.min_liquidity, "max": None, "allowed_values": None},
        "min_volume_24h": {"min": effective.min_volume_24h, "max": None, "allowed_values": None},
        "min_abs_price_move": {"min": effective.min_abs_move, "max": None, "allowed_values": None},
        "alert_strengths": {
            "min": None,
            "max": None,
            "allowed_values": sorted(effective.allowed_strengths),
        },
        "digest_window_minutes": {
            "min": effective.digest_window_minutes,
            "max": None,
            "allowed_values": [effective.digest_window_minutes],
        },
        "max_alerts_per_digest": {"min": 0, "max": effective.max_alerts_per_digest, "allowed_values": None},
        "max_themes_per_digest": {"min": 0, "max": effective.max_themes_per_digest, "allowed_values": None},
        "max_markets_per_theme": {"min": 0, "max": effective.max_markets_per_theme, "allowed_values": None},
        "p_min": {"min": effective.p_min, "max": effective.p_max, "allowed_values": None},
        "p_max": {"min": effective.p_min, "max": effective.p_max, "allowed_values": None},
        "fast_window_minutes": {"min": effective.fast_window_minutes, "max": None, "allowed_values": None},
        "fast_max_themes_per_digest": {"min": 0, "max": effective.fast_max_themes_per_digest, "allowed_values": None},
        "fast_max_markets_per_theme": {"min": 0, "max": effective.fast_max_markets_per_theme, "allowed_values": None},
    }


def _build_plan_features(plan: Plan | None, effective: EffectiveSettings) -> dict[str, object]:
    copilot_enabled = False
    if plan is not None:
        copilot_enabled = bool(plan.copilot_enabled) if plan.copilot_enabled is not None else True
    return {
        "copilot_enabled": copilot_enabled,
        "fast_signals_enabled": bool(effective.fast_signals_enabled),
        "fast_mode": effective.fast_mode,
    }


def build_settings_entitlements(user: User) -> dict[str, object]:
    effective = resolve_effective_settings(user, pref=None)
    plan_name = effective.plan_name
    plan = getattr(user, "plan", None)
    return {
        "plan": plan_name,
        "upgrade_target": upgrade_target_name(plan_name),
        "features": _build_plan_features(plan, effective),
        "limits": _build_settings_limits(effective),
    }


def _build_session_payload(db: Session, user: User) -> dict[str, object]:
    from .stripe_service import _get_latest_subscription, _refresh_subscription_from_stripe

    auth = db.query(UserAuth).filter(UserAuth.user_id == user.user_id).one_or_none()
    subscription = _get_latest_subscription(db, user.user_id)
    plan = subscription.plan if subscription else None
    subscription_payload = None
    cancel_at_period_end = None
    if subscription and subscription.stripe_subscription_id and _is_active_status(subscription.status):
        try:
            subscription, flags = _refresh_subscription_from_stripe(db, subscription, user.user_id)
            cancel_at_period_end = flags.get("cancel_at_period_end")
        except Exception:
            cancel_at_period_end = None
    if subscription:
        subscription_payload = {
            "status": subscription.status,
            "plan_id": subscription.plan_id,
            "plan_name": plan.name if plan else None,
            "current_period_end": subscription.current_period_end.isoformat() if subscription.current_period_end else None,
            "stripe_customer_id": subscription.stripe_customer_id,
            "stripe_subscription_id": subscription.stripe_subscription_id,
            "cancel_at_period_end": cancel_at_period_end,
        }
    return {
        "user": {
            "id": str(user.user_id),
            "email": auth.email if auth else "",
            "name": user.name,
            "telegram_chat_id": user.telegram_chat_id,
            "telegram_pending": user.telegram_chat_id is None,
        },
        "subscription": subscription_payload,
        "entitlements": _build_entitlements(plan),
    }


def _is_active_status(status: str | None) -> bool:
    return status in {"active", "trialing"}


def get_active_subscription(
    db: Session,
    *,
    user_id: uuid.UUID | None = None,
    user_ids: Iterable[uuid.UUID] | None = None,
    include_latest: bool = False,
):
    if user_id is None and user_ids is None:
        return None
    if user_ids is not None:
        ids = [value for value in user_ids if value is not None]
    else:
        ids = [user_id] if user_id is not None else []
    if not ids:
        return {} if user_ids is not None else None

    rows = (
        db.query(Subscription)
        .filter(Subscription.user_id.in_(ids))
        .order_by(Subscription.user_id.asc(), Subscription.created_at.desc())
        .all()
    )
    latest_by_user: dict[uuid.UUID, Subscription] = {}
    for subscription in rows:
        if subscription.user_id not in latest_by_user:
            latest_by_user[subscription.user_id] = subscription

    now_ts = datetime.now(timezone.utc)
    active_by_user: dict[uuid.UUID, Subscription] = {}
    for latest_user_id, subscription in latest_by_user.items():
        if not _is_active_status(subscription.status):
            continue
        current_period_end = subscription.current_period_end
        if current_period_end is not None:
            if current_period_end.tzinfo is None:
                current_period_end = current_period_end.replace(tzinfo=timezone.utc)
            if current_period_end < now_ts:
                continue
        active_by_user[latest_user_id] = subscription

    if include_latest:
        if user_id is not None and user_ids is None:
            return active_by_user.get(user_id), latest_by_user.get(user_id)
        return active_by_user, latest_by_user
    if user_id is not None and user_ids is None:
        return active_by_user.get(user_id)
    return active_by_user
