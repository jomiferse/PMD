from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from ...db import get_db
from ...models import MarketSnapshot
from ...rate_limit import rate_limit

router = APIRouter()


@router.get("/snapshots/latest")
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
