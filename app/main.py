from datetime import datetime, timedelta, timezone
import json

import redis
from fastapi import Depends, FastAPI
from rq import Queue
from sqlalchemy.orm import Session
from sqlalchemy import func

from .db import get_db
from .auth import admin_key_auth
from .jobs.run import job_sync_wrapper
from .logging import configure_logging
from .models import Alert, AlertDelivery, MarketSnapshot, User, UserAlertPreference
from .rate_limit import rate_limit
from .settings import settings
from .core.alerts import USER_DIGEST_LAST_PAYLOAD_KEY

configure_logging()

DISCLAIMER = (
    "Read-only analytics. Not financial advice. No guarantee of outcomes. "
    "No custody. No execution."
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
    return [
        {
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
        for r in rows
    ]


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
                "min_liquidity": pref.min_liquidity,
                "min_volume_24h": pref.min_volume_24h,
                "min_abs_price_move": pref.min_abs_price_move,
                "alert_strengths": pref.alert_strengths,
                "digest_window_minutes": pref.digest_window_minutes,
                "max_alerts_per_digest": pref.max_alerts_per_digest,
                "created_at": pref.created_at.isoformat(),
            }
            if pref
            else None,
        }
        for user, pref in rows
    ]


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


@app.get("/admin/stats")
def admin_stats(
    db: Session = Depends(get_db),
    _=Depends(admin_key_auth),
):
    since = datetime.now(timezone.utc) - timedelta(hours=24)
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
    }


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
