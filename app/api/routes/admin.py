from datetime import datetime, timedelta, timezone
import json
import logging
import uuid

from fastapi import APIRouter, Depends, Request
from sqlalchemy import func
from sqlalchemy.orm import Session

from ...auth import admin_key_auth
from ...core.ai_copilot import COPILOT_LAST_STATUS_KEY, COPILOT_THEME_DEDUPE_KEY
from ...core.alerts import USER_DIGEST_LAST_PAYLOAD_KEY
from ...core.effective_settings import invalidate_effective_settings_cache
from ...core.plans import recommended_plan_name
from ...core.user_settings import get_effective_user_settings
from ...cache import invalidate_user_caches
from ...db import get_db
from ...integrations.redis_client import redis_conn
from ...models import (
    AiRecommendation,
    Alert,
    AlertDelivery,
    MarketSnapshot,
    Plan,
    User,
    UserAlertPreference,
)

router = APIRouter()
logger = logging.getLogger("app.main")


@router.get("/admin/users")
def admin_users(
    db: Session = Depends(get_db),
    _=Depends(admin_key_auth),
):
    rows = (
        db.query(User, UserAlertPreference)
        .outerjoin(UserAlertPreference, User.user_id == UserAlertPreference.user_id)
        .order_by(User.created_at.desc())
        .all()
    )
    return [
        {
            "user_id": str(user.user_id),
            "name": user.name,
            "telegram_chat_id": user.telegram_chat_id,
            "is_active": user.is_active,
            "created_at": user.created_at.isoformat(),
            "preferences": {
                "min_liquidity": pref.min_liquidity if pref else None,
                "min_volume_24h": pref.min_volume_24h if pref else None,
                "min_abs_price_move": pref.min_abs_price_move if pref else None,
                "alert_strengths": pref.alert_strengths if pref else None,
                "digest_window_minutes": pref.digest_window_minutes if pref else None,
                "max_alerts_per_digest": pref.max_alerts_per_digest if pref else None,
                "max_themes_per_digest": pref.max_themes_per_digest if pref else None,
                "max_markets_per_theme": pref.max_markets_per_theme if pref else None,
                "p_min": pref.p_min if pref else None,
                "p_max": pref.p_max if pref else None,
                "ai_copilot_enabled": user.copilot_enabled,
                "fast_signals_enabled": pref.fast_signals_enabled if pref else None,
                "fast_window_minutes": pref.fast_window_minutes if pref else None,
                "fast_max_themes_per_digest": pref.fast_max_themes_per_digest if pref else None,
                "fast_max_markets_per_theme": pref.fast_max_markets_per_theme if pref else None,
                "created_at": pref.created_at.isoformat() if pref else None,
            },
        }
        for user, pref in rows
    ]


@router.get("/admin/copilot/dedupe/{user_id}")
def admin_copilot_dedupe(
    user_id: str,
    _=Depends(admin_key_auth),
):
    pattern = COPILOT_THEME_DEDUPE_KEY.format(user_id=user_id, theme_key="*")
    try:
        scan_iter = getattr(redis_conn, "scan_iter", None)
        if scan_iter:
            raw_keys = list(scan_iter(match=pattern))
        elif hasattr(redis_conn, "keys"):
            raw_keys = redis_conn.keys(pattern)  # type: ignore[arg-type]
        else:
            raw_keys = []
    except Exception:
        logger.exception("copilot_dedupe_list_failed user_id=%s", user_id)
        raw_keys = []

    keys: list[dict[str, object]] = []
    for raw in raw_keys:
        key = raw.decode() if isinstance(raw, (bytes, bytearray)) else str(raw)
        try:
            ttl = redis_conn.ttl(key)
        except Exception:
            ttl = None
        keys.append({"key": key, "ttl": ttl})
    return {"user_id": user_id, "keys": keys}


@router.get("/admin/users/{user_id}/last-digest")
def admin_user_last_digest(
    user_id: str,
    _=Depends(admin_key_auth),
):
    key = USER_DIGEST_LAST_PAYLOAD_KEY.format(user_id=user_id)
    payload = redis_conn.get(key)
    if not payload:
        return {"user_id": user_id, "last_digest": None}
    try:
        return {"user_id": user_id, "last_digest": json.loads(payload)}
    except Exception:
        return {"user_id": user_id, "last_digest": None}


@router.get("/admin/users/{user_id}/copilot-last-status")
def admin_user_copilot_last_status(
    user_id: str,
    _=Depends(admin_key_auth),
):
    key = COPILOT_LAST_STATUS_KEY.format(user_id=user_id)
    payload = redis_conn.get(key)
    if not payload:
        return {"user_id": user_id, "last_status": None}
    try:
        return {"user_id": user_id, "last_status": json.loads(payload)}
    except Exception:
        return {"user_id": user_id, "last_status": None}


@router.get("/admin/plans")
def admin_plans(
    db: Session = Depends(get_db),
    _=Depends(admin_key_auth),
):
    rows = db.query(Plan).order_by(Plan.id.asc()).all()
    recommended = recommended_plan_name()
    return [
        {
            "id": plan.id,
            "name": plan.name,
            "price_monthly": plan.price_monthly,
            "is_active": plan.is_active,
            "copilot_enabled": plan.copilot_enabled,
            "max_copilot_per_day": plan.max_copilot_per_day,
            "max_fast_copilot_per_day": plan.max_fast_copilot_per_day,
            "max_copilot_per_hour": plan.max_copilot_per_hour,
            "max_copilot_per_digest": plan.max_copilot_per_digest,
            "copilot_theme_ttl_minutes": plan.copilot_theme_ttl_minutes,
            "fast_signals_enabled": plan.fast_signals_enabled,
            "digest_window_minutes": plan.digest_window_minutes,
            "max_themes_per_digest": plan.max_themes_per_digest,
            "max_alerts_per_digest": plan.max_alerts_per_digest,
            "max_markets_per_theme": plan.max_markets_per_theme,
            "min_liquidity": plan.min_liquidity,
            "min_volume_24h": plan.min_volume_24h,
            "min_abs_move": plan.min_abs_move,
            "p_min": plan.p_min,
            "p_max": plan.p_max,
            "allowed_strengths": plan.allowed_strengths,
            "fast_mode": plan.fast_mode,
            "fast_window_minutes": plan.fast_window_minutes,
            "fast_max_themes_per_digest": plan.fast_max_themes_per_digest,
            "fast_max_markets_per_theme": plan.fast_max_markets_per_theme,
            "created_at": plan.created_at.isoformat() if plan.created_at else None,
            "recommended": plan.name == recommended,
        }
        for plan in rows
    ]


@router.post("/admin/plans")
async def admin_upsert_plan(
    request: Request,
    db: Session = Depends(get_db),
    _=Depends(admin_key_auth),
):
    payload = await request.json()
    plan_id = payload.get("id")
    name = payload.get("name")
    if not plan_id and not name:
        return {"ok": False, "error": "missing_name"}

    plan = None
    if plan_id is not None:
        plan = db.query(Plan).filter(Plan.id == plan_id).one_or_none()
    if plan is None and name:
        plan = db.query(Plan).filter(Plan.name == name).one_or_none()
    if plan is None:
        if not name:
            return {"ok": False, "error": "plan_not_found"}
        plan = Plan(name=name)

    allowed_fields = {
        "name",
        "price_monthly",
        "is_active",
        "copilot_enabled",
        "max_copilot_per_day",
        "max_fast_copilot_per_day",
        "max_copilot_per_hour",
        "max_copilot_per_digest",
        "copilot_theme_ttl_minutes",
        "fast_signals_enabled",
        "digest_window_minutes",
        "max_themes_per_digest",
        "max_alerts_per_digest",
        "max_markets_per_theme",
        "min_liquidity",
        "min_volume_24h",
        "min_abs_move",
        "p_min",
        "p_max",
        "allowed_strengths",
        "fast_mode",
        "fast_window_minutes",
        "fast_max_themes_per_digest",
        "fast_max_markets_per_theme",
    }
    for key, value in payload.items():
        if key not in allowed_fields:
            continue
        if key == "allowed_strengths":
            value = _normalize_strengths_input(value)
        setattr(plan, key, value)

    db.add(plan)
    db.commit()
    db.refresh(plan)
    return {"ok": True, "plan_id": plan.id}


@router.patch("/admin/users/{user_id}/plan")
async def admin_assign_plan(
    user_id: str,
    request: Request,
    db: Session = Depends(get_db),
    _=Depends(admin_key_auth),
):
    payload = await request.json()
    plan_id = payload.get("plan_id")
    plan_name = payload.get("plan_name")
    if plan_id is None and not plan_name:
        return {"ok": False, "error": "missing_plan"}

    plan = None
    if plan_id is not None:
        plan = db.query(Plan).filter(Plan.id == plan_id).one_or_none()
    if plan is None and plan_name:
        plan = db.query(Plan).filter(Plan.name == plan_name).one_or_none()
    if plan is None:
        return {"ok": False, "error": "plan_not_found"}

    parsed_user_id = user_id
    if isinstance(user_id, str):
        try:
            parsed_user_id = uuid.UUID(user_id)
        except ValueError:
            return {"ok": False, "error": "invalid_user_id"}
    user = db.query(User).filter(User.user_id == parsed_user_id).one_or_none()
    if not user:
        return {"ok": False, "error": "user_not_found"}

    user.plan_id = plan.id
    db.commit()
    invalidate_effective_settings_cache(user.user_id)
    invalidate_user_caches(str(user.user_id), plan_id=plan.id)
    return {"ok": True, "user_id": str(user.user_id), "plan_id": plan.id, "plan_name": plan.name}


@router.get("/admin/users/{user_id}/effective-settings")
def admin_user_effective_settings(
    user_id: str,
    db: Session = Depends(get_db),
    _=Depends(admin_key_auth),
):
    parsed_user_id = user_id
    if isinstance(user_id, str):
        try:
            parsed_user_id = uuid.UUID(user_id)
        except ValueError:
            return {"user_id": user_id, "error": "invalid_user_id"}
    user = db.query(User).filter(User.user_id == parsed_user_id).one_or_none()
    if not user:
        return {"user_id": user_id, "error": "user_not_found"}
    effective = get_effective_user_settings(user, db=db)
    return {
        "user_id": str(user.user_id),
        "plan_name": effective.plan_name,
        "copilot_enabled": effective.copilot_enabled,
        "max_copilot_per_day": effective.max_copilot_per_day,
        "max_fast_copilot_per_day": effective.max_fast_copilot_per_day,
        "max_copilot_per_digest": effective.max_copilot_per_digest,
        "copilot_theme_ttl_minutes": effective.copilot_theme_ttl_minutes,
        "fast_signals_enabled": effective.fast_signals_enabled,
        "fast_mode": effective.fast_mode,
        "fast_window_minutes": effective.fast_window_minutes,
        "fast_max_themes_per_digest": effective.fast_max_themes_per_digest,
        "fast_max_markets_per_theme": effective.fast_max_markets_per_theme,
        "digest_window_minutes": effective.digest_window_minutes,
        "max_themes_per_digest": effective.max_themes_per_digest,
        "max_markets_per_theme": effective.max_markets_per_theme,
        "max_alerts_per_digest": effective.max_alerts_per_digest,
        "min_liquidity": effective.min_liquidity,
        "min_volume_24h": effective.min_volume_24h,
        "min_abs_move": effective.min_abs_move,
        "p_min": effective.p_min,
        "p_max": effective.p_max,
        "allowed_strengths": sorted(effective.allowed_strengths),
        "overrides_json": user.overrides_json,
    }


def _normalize_strengths_input(value) -> str | None:
    if value is None:
        return None
    if isinstance(value, (list, tuple, set)):
        parts = [str(part).strip().upper() for part in value if str(part).strip()]
        return ",".join(parts) if parts else None
    if isinstance(value, str):
        parts = [part.strip().upper() for part in value.split(",") if part.strip()]
        return ",".join(parts) if parts else None
    return None


@router.get("/admin/ai-recommendations")
def admin_ai_recommendations(
    db: Session = Depends(get_db),
    _=Depends(admin_key_auth),
    limit: int = 50,
):
    rows = (
        db.query(AiRecommendation)
        .order_by(AiRecommendation.created_at.desc())
        .limit(limit)
        .all()
    )
    return [
        {
            "id": rec.id,
            "user_id": str(rec.user_id),
            "alert_id": rec.alert_id,
            "created_at": rec.created_at.isoformat(),
            "recommendation": rec.recommendation,
            "confidence": rec.confidence,
            "rationale": rec.rationale,
            "risks": rec.risks,
            "status": rec.status,
            "telegram_message_id": rec.telegram_message_id,
            "expires_at": rec.expires_at.isoformat() if rec.expires_at else None,
        }
        for rec in rows
    ]


@router.get("/admin/stats")
def admin_stats(
    db: Session = Depends(get_db),
    _=Depends(admin_key_auth),
):
    since = datetime.now(timezone.utc) - timedelta(hours=24)
    snapshot_count = db.query(func.count()).select_from(MarketSnapshot).scalar()
    alert_count = db.query(func.count()).select_from(Alert).scalar()
    delivery_count = db.query(func.count()).select_from(AlertDelivery).scalar()
    snapshot_oldest = db.query(func.min(MarketSnapshot.asof_ts)).scalar()
    snapshot_newest = db.query(func.max(MarketSnapshot.asof_ts)).scalar()
    alert_oldest = db.query(func.min(Alert.triggered_at)).scalar()
    alert_newest = db.query(func.max(Alert.triggered_at)).scalar()
    delivery_oldest = db.query(func.min(AlertDelivery.delivered_at)).scalar()
    delivery_newest = db.query(func.max(AlertDelivery.delivered_at)).scalar()
    active_users = (
        db.query(func.count())
        .select_from(User)
        .filter(User.is_active.is_(True))
        .scalar()
    )
    alerts_generated = (
        db.query(func.count())
        .select_from(Alert)
        .filter(Alert.created_at >= since)
        .scalar()
    )
    alerts_delivered = (
        db.query(func.count())
        .select_from(AlertDelivery)
        .filter(
            AlertDelivery.delivered_at >= since,
            AlertDelivery.delivery_status == "sent",
        )
        .scalar()
    )
    return {
        "since": since.isoformat(),
        "active_users": active_users or 0,
        "alerts_generated": alerts_generated or 0,
        "alerts_delivered": alerts_delivered or 0,
        "snapshot_count": snapshot_count or 0,
        "alert_count": alert_count or 0,
        "delivery_count": delivery_count or 0,
        "snapshot_oldest": snapshot_oldest.isoformat() if snapshot_oldest else None,
        "snapshot_newest": snapshot_newest.isoformat() if snapshot_newest else None,
        "alert_oldest": alert_oldest.isoformat() if alert_oldest else None,
        "alert_newest": alert_newest.isoformat() if alert_newest else None,
        "delivery_oldest": delivery_oldest.isoformat() if delivery_oldest else None,
        "delivery_newest": delivery_newest.isoformat() if delivery_newest else None,
    }
