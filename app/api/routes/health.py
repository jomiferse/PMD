from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends
from sqlalchemy import func
from sqlalchemy.orm import Session

from ...db import get_db
from ...integrations.redis_client import redis_conn
from ...integrations.rq_queue import q
from ...models import Alert, MarketSnapshot
from ...rate_limit import rate_limit

router = APIRouter()


@router.get("/health")
def health():
    return {"ok": True}


@router.get("/status")
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
