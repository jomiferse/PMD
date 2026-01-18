from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, Request
from sqlalchemy import func
from sqlalchemy.orm import Session

from ...cache import build_cache_key, cached_json_response
from ...db import get_db
from ...integrations.redis_client import redis_conn
from ...integrations.rq_queue import q
from ...models import Alert, MarketSnapshot
from ...deps import _require_session_user
from ...settings import settings

router = APIRouter()


@router.get("/health")
def health():
    return {"ok": True}


@router.get("/status")
def status(request: Request, db: Session = Depends(get_db)):
    user, _ = _require_session_user(request, db)
    cache_key = build_cache_key(
        "status",
        request,
        tenant_id=settings.DEFAULT_TENANT_ID,
        user_id=str(user.user_id),
    )

    def _build_payload():
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

    return cached_json_response(
        request,
        cache_key=cache_key,
        ttl_seconds=settings.CACHE_TTL_STATUS_SECONDS,
        fetch_fn=_build_payload,
        private=True,
    )
