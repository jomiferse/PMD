from datetime import datetime, timedelta, timezone

from sqlalchemy.orm import Session

from ..models import Alert, MarketSnapshot


def compute_dislocation_alerts(
    db: Session,
    snapshots: list[dict],
    window_minutes: int,
    move_threshold: float,
    min_liquidity: float,
    min_volume_24h: float,
    tenant_id: str,
) -> list[Alert]:
    if not snapshots:
        return []

    window_start = datetime.now(timezone.utc) - timedelta(minutes=window_minutes)
    alerts: list[Alert] = []

    now_ts = datetime.now(timezone.utc)

    for snap in snapshots:
        if snap["liquidity"] < min_liquidity:
            continue
        if snap["volume_24h"] < min_volume_24h:
            continue

        prev = (
            db.query(MarketSnapshot)
            .filter(
                MarketSnapshot.market_id == snap["market_id"],
                MarketSnapshot.snapshot_bucket >= window_start,
                MarketSnapshot.snapshot_bucket < snap["snapshot_bucket"],
            )
            .order_by(MarketSnapshot.snapshot_bucket.asc())
            .first()
        )
        if not prev:
            continue

        move = abs(snap["market_p_yes"] - prev.market_p_yes)
        if move < move_threshold:
            continue

        message = f"Dislocation {move:.2f} over {window_minutes}m"
        alerts.append(
            Alert(
                tenant_id=tenant_id,
                alert_type="dislocation",
                market_id=snap["market_id"],
                title=snap["title"],
                category=snap["category"],
                move=move,
                market_p_yes=snap["market_p_yes"],
                prev_market_p_yes=prev.market_p_yes,
                liquidity=snap["liquidity"],
                volume_24h=snap["volume_24h"],
                snapshot_bucket=snap["snapshot_bucket"],
                source_ts=snap["source_ts"],
                message=message,
                created_at=now_ts,
            )
        )

    return alerts
