from fastapi import APIRouter, Depends, Request
from sqlalchemy.orm import Session

from ...cache import build_cache_key, cached_json_response
from ...db import get_db
from ...models import MarketSnapshot
from ...rate_limit import rate_limit
from ...settings import settings

router = APIRouter()


@router.get("/snapshots/latest")
def latest(
    request: Request,
    db: Session = Depends(get_db),
    api_key=Depends(rate_limit),
    limit: int = 50,
):
    cache_key = build_cache_key(
        "snapshots_latest",
        request,
        tenant_id=api_key.tenant_id,
    )

    def _build_payload():
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
                "market_p_no": r.market_p_no,
                "market_p_no_derived": r.market_p_no_derived,
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

    return cached_json_response(
        request,
        cache_key=cache_key,
        ttl_seconds=settings.CACHE_TTL_SNAPSHOTS_LATEST_SECONDS,
        fetch_fn=_build_payload,
        private=True,
    )
