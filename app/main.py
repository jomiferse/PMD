from datetime import datetime, timedelta, timezone
import logging
import uuid
import json

import redis
from fastapi import Depends, FastAPI, Request
from rq import Queue
from sqlalchemy.orm import Session
from sqlalchemy import func

from .db import get_db
from .auth import admin_key_auth
from .jobs.run import job_sync_wrapper
from .logging import configure_logging
from .models import (
    AiRecommendation,
    Alert,
    AlertDelivery,
    MarketSnapshot,
    Plan,
    User,
    UserAlertPreference,
)
from .rate_limit import rate_limit
from .settings import settings
from .core.alerts import USER_DIGEST_LAST_PAYLOAD_KEY
from .core.alert_classification import classify_alert_with_snapshots
from .core.ai_copilot import COPILOT_LAST_STATUS_KEY, COPILOT_THEME_DEDUPE_KEY, handle_telegram_callback
from .core.effective_settings import invalidate_effective_settings_cache
from .core.user_settings import get_effective_user_settings
from .core.plans import recommended_plan_name

configure_logging()
logger = logging.getLogger(__name__)

DISCLAIMER = (
    "Read-only analytics. Manual execution only. Not financial advice. "
    "No guarantee of outcomes. No custody. No execution."
)

app = FastAPI(
    title="PMD - Polymarket Mispricing Detector",
    description=DISCLAIMER,
)

redis_conn = redis.from_url(settings.REDIS_URL)
q = Queue("default", connection=redis_conn)
LAST_DIGEST_KEY = "alerts:last_digest:{tenant_id}"


@app.get("/health")
def health():
    return {"ok": True}


@app.post("/jobs/ingest")
def ingest_job(_=Depends(rate_limit)):
    job = q.enqueue(job_sync_wrapper)
    return {"job_id": job.id}


@app.get("/snapshots/latest")
def latest(
    db: Session = Depends(get_db),
    api_key=Depends(rate_limit),
    limit: int = 50,
):
    rows = (
        db.query(MarketSnapshot)
        .order_by(MarketSnapshot.asof_ts.desc())
        .limit(limit)
        .all()
    )
    return [
        {
            "market_id": r.market_id,
            "title": r.title,
            "category": r.category,
            "slug": r.slug,
            "market_p_yes": r.market_p_yes,
            "model_p_yes": r.model_p_yes,
            "edge": r.edge,
            "liquidity": r.liquidity,
            "volume_24h": r.volume_24h,
            "volume_1w": r.volume_1w,
            "best_ask": r.best_ask,
            "last_trade_price": r.last_trade_price,
            "source_ts": r.source_ts.isoformat() if r.source_ts else None,
            "snapshot_bucket": r.snapshot_bucket.isoformat(),
            "asof_ts": r.asof_ts.isoformat(),
        }
        for r in rows
    ]


@app.get("/alerts/latest")
def alerts_latest(
    db: Session = Depends(get_db),
    api_key=Depends(rate_limit),
    limit: int = 50,
):
    rows = (
        db.query(Alert)
        .filter(Alert.tenant_id == api_key.tenant_id)
        .order_by(Alert.created_at.desc())
        .limit(limit)
        .all()
    )
    results = []
    for r in rows:
        classification = classify_alert_with_snapshots(db, r)
        results.append(
            {
                "signal_type": classification.signal_type,
                "confidence": classification.confidence,
                "suggested_action": classification.suggested_action,
                "id": r.id,
                "type": r.alert_type,
                "market_id": r.market_id,
                "title": r.title,
                "category": r.category,
                "move": r.move,
                "delta_pct": r.delta_pct,
                "market_p_yes": r.market_p_yes,
                "prev_market_p_yes": r.prev_market_p_yes,
                "old_price": r.old_price,
                "new_price": r.new_price,
                "liquidity": r.liquidity,
                "volume_24h": r.volume_24h,
                "strength": r.strength,
                "snapshot_bucket": r.snapshot_bucket.isoformat(),
                "source_ts": r.source_ts.isoformat() if r.source_ts else None,
                "triggered_at": r.triggered_at.isoformat() if r.triggered_at else None,
                "created_at": r.created_at.isoformat(),
                "message": r.message,
            }
        )
    return results


@app.get("/alerts/summary")
def alerts_summary(
    db: Session = Depends(get_db),
    api_key=Depends(rate_limit),
):
    since = datetime.now(timezone.utc) - timedelta(hours=24)
    rows = (
        db.query(Alert.alert_type)
        .filter(Alert.tenant_id == api_key.tenant_id, Alert.created_at >= since)
        .all()
    )
    counts: dict[str, int] = {}
    for (alert_type,) in rows:
        counts[alert_type] = counts.get(alert_type, 0) + 1
    return {"since": since.isoformat(), "counts": counts}


@app.get("/alerts/last-digest")
def alerts_last_digest(api_key=Depends(rate_limit)):
    key = LAST_DIGEST_KEY.format(tenant_id=api_key.tenant_id)
    payload = redis_conn.get(key)
    if not payload:
        return {"last_digest": None}
    try:
        return json.loads(payload)
    except Exception:
        return {"last_digest": None}


@app.get("/admin/users")
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


@app.get("/admin/copilot/dedupe/{user_id}")
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


@app.get("/admin/users/{user_id}/last-digest")
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


@app.get("/admin/users/{user_id}/copilot-last-status")
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


@app.get("/admin/plans")
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
            "fast_window_minutes": plan.fast_window_minutes,
            "fast_max_themes_per_digest": plan.fast_max_themes_per_digest,
            "fast_max_markets_per_theme": plan.fast_max_markets_per_theme,
            "created_at": plan.created_at.isoformat() if plan.created_at else None,
            "recommended": plan.name == recommended,
        }
        for plan in rows
    ]


@app.post("/admin/plans")
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


@app.patch("/admin/users/{user_id}/plan")
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
    return {"ok": True, "user_id": str(user.user_id), "plan_id": plan.id, "plan_name": plan.name}


@app.get("/admin/users/{user_id}/effective-settings")
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


@app.get("/admin/ai-recommendations")
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


@app.get("/admin/stats")
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


@app.post("/telegram/webhook")
async def telegram_webhook(request: Request, db: Session = Depends(get_db)):
    payload = await request.json()
    return handle_telegram_callback(db, payload)


@app.get("/status")
def status(db: Session = Depends(get_db), api_key=Depends(rate_limit)):
    last_ingest_ts = redis_conn.get("ingest:last_ts")
    last_ingest_result = redis_conn.get("ingest:last_result")
    queue_count = q.count if isinstance(q.count, int) else q.count()
    since = datetime.now(timezone.utc) - timedelta(hours=24)
    last_snapshot = (
        db.query(MarketSnapshot)
        .order_by(MarketSnapshot.asof_ts.desc())
        .limit(1)
        .one_or_none()
    )
    snapshots_last_24h = (
        db.query(func.count())
        .select_from(MarketSnapshot)
        .filter(MarketSnapshot.asof_ts >= since)
        .scalar()
    )
    alerts_last_24h = (
        db.query(func.count())
        .select_from(Alert)
        .filter(Alert.created_at >= since)
        .scalar()
    )
    return {
        "last_ingest_time": last_ingest_ts.decode() if last_ingest_ts else None,
        "last_job_result": last_ingest_result.decode() if last_ingest_result else None,
        "redis_queue_length": queue_count,
        "last_snapshot_ts": last_snapshot.asof_ts.isoformat() if last_snapshot else None,
        "snapshots_last_24h": snapshots_last_24h or 0,
        "alerts_last_24h": alerts_last_24h or 0,
    }
