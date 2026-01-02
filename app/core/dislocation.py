from datetime import datetime, timedelta, timezone

from sqlalchemy.orm import Session

from ..models import Alert, MarketSnapshot

ALERT_TYPE = "DISLOCATION"


def compute_dislocation_alerts(
    db: Session,
    snapshots: list[dict],
    window_minutes: int,
    move_threshold: float,
    min_liquidity: float,
    min_volume_24h: float,
    cooldown_minutes: int,
    tenant_id: str,
    use_triggered_at: bool = True,
) -> list[Alert]:
    if not snapshots:
        return []

    window_start = datetime.now(timezone.utc) - timedelta(minutes=window_minutes)
    alerts: list[Alert] = []

    now_ts = datetime.now(timezone.utc)
    cooldown_start = now_ts - timedelta(minutes=cooldown_minutes)
    seen_market_ids: set[str] = set()

    for snap in snapshots:
        if snap["liquidity"] < min_liquidity:
            continue
        if snap["volume_24h"] < min_volume_24h:
            continue
        if snap["market_id"] in seen_market_ids:
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

        if prev.market_p_yes <= 0:
            continue

        delta_pct = abs(snap["market_p_yes"] - prev.market_p_yes) / prev.market_p_yes
        if delta_pct < move_threshold:
            continue

        cooldown_field = Alert.triggered_at if use_triggered_at else Alert.created_at
        recent = (
            db.query(Alert.id)
            .filter(
                Alert.tenant_id == tenant_id,
                Alert.alert_type == ALERT_TYPE,
                Alert.market_id == snap["market_id"],
                cooldown_field >= cooldown_start,
            )
            .limit(1)
            .one_or_none()
        )
        if recent:
            continue

        message = f"Dislocation {delta_pct * 100:.1f}% over {window_minutes}m"
        alerts.append(
            Alert(
                tenant_id=tenant_id,
                alert_type=ALERT_TYPE,
                market_id=snap["market_id"],
                title=snap["title"],
                category=snap["category"],
                move=delta_pct,
                market_p_yes=snap["market_p_yes"],
                prev_market_p_yes=prev.market_p_yes,
                old_price=prev.market_p_yes,
                new_price=snap["market_p_yes"],
                delta_pct=delta_pct,
                liquidity=snap["liquidity"],
                volume_24h=snap["volume_24h"],
                snapshot_bucket=snap["snapshot_bucket"],
                source_ts=snap["source_ts"],
                message=message,
                triggered_at=now_ts,
                created_at=now_ts,
            )
        )
        seen_market_ids.add(snap["market_id"])

    return alerts
