from datetime import datetime, timedelta, timezone

import redis
from fastapi import Depends, FastAPI
from rq import Queue
from sqlalchemy.orm import Session

from .db import get_db
from .jobs.run import job_sync_wrapper
from .logging import configure_logging
from .models import Alert, MarketSnapshot
from .rate_limit import rate_limit
from .settings import settings

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
            "market_p_yes": r.market_p_yes,
            "prev_market_p_yes": r.prev_market_p_yes,
            "liquidity": r.liquidity,
            "volume_24h": r.volume_24h,
            "snapshot_bucket": r.snapshot_bucket.isoformat(),
            "source_ts": r.source_ts.isoformat() if r.source_ts else None,
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


@app.get("/status")
def status(db: Session = Depends(get_db), api_key=Depends(rate_limit)):
    last_ingest_ts = redis_conn.get("ingest:last_ts")
    last_ingest_result = redis_conn.get("ingest:last_result")
    queue_count = q.count if isinstance(q.count, int) else q.count()
    last_snapshot = (
        db.query(MarketSnapshot)
        .order_by(MarketSnapshot.asof_ts.desc())
        .limit(1)
        .one_or_none()
    )
    return {
        "last_ingest_ts": last_ingest_ts.decode() if last_ingest_ts else None,
        "last_job_result": last_ingest_result.decode() if last_ingest_result else None,
        "queue_count": queue_count,
        "last_snapshot_ts": last_snapshot.asof_ts.isoformat() if last_snapshot else None,
    }
