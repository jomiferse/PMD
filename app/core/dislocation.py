from datetime import datetime, timedelta, timezone

from sqlalchemy.orm import Session

from ..models import Alert, MarketSnapshot
from .alert_strength import AlertStrength

ALERT_TYPE = "DISLOCATION"


def compute_dislocation_alerts(
    db: Session,
    snapshots: list[dict],
    window_minutes: int,
    medium_move_threshold: float,
    min_price_threshold: float,
    medium_abs_move_threshold: float,
    floor_price: float,
    medium_min_liquidity: float,
    medium_min_volume_24h: float,
    strong_abs_move_threshold: float,
    strong_min_liquidity: float,
    strong_min_volume_24h: float,
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
        # Skip illiquid markets to avoid noisy, low-signal moves.
        if snap["liquidity"] < medium_min_liquidity:
            continue
        # Require minimum trading activity for meaningful alerts.
        if snap["volume_24h"] < medium_min_volume_24h:
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

        old_price = prev.market_p_yes
        new_price = snap["market_p_yes"]
        # Skip unchanged prices so % math can't create phantom moves.
        if old_price == new_price:
            continue
        # Avoid micro-priced markets where % moves are misleading.
        if old_price < min_price_threshold and new_price < min_price_threshold:
            continue

        abs_move = abs(new_price - old_price)
        # Require a real absolute move to filter out tiny jitter.
        if abs_move < medium_abs_move_threshold:
            continue

        # Use a floor to prevent % explosions on tiny bases.
        delta_pct = abs_move / max(old_price, floor_price)
        if delta_pct < medium_move_threshold:
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

        strength = AlertStrength.MEDIUM
        if (
            abs_move >= strong_abs_move_threshold
            and snap["liquidity"] >= strong_min_liquidity
            and snap["volume_24h"] >= strong_min_volume_24h
        ):
            strength = AlertStrength.STRONG

        message = f"Dislocation {delta_pct * 100:.1f}% over {window_minutes}m"
        alerts.append(
            Alert(
                tenant_id=tenant_id,
                alert_type=ALERT_TYPE,
                market_id=snap["market_id"],
                title=snap["title"],
                category=snap["category"],
                move=delta_pct,
                market_p_yes=new_price,
                prev_market_p_yes=old_price,
                primary_outcome_label=snap.get("primary_outcome_label"),
                is_yesno=snap.get("is_yesno"),
                old_price=old_price,
                new_price=new_price,
                delta_pct=delta_pct,
                liquidity=snap["liquidity"],
                volume_24h=snap["volume_24h"],
                strength=strength.value,
                snapshot_bucket=snap["snapshot_bucket"],
                source_ts=snap["source_ts"],
                message=message,
                triggered_at=now_ts,
                created_at=now_ts,
            )
        )
        seen_market_ids.add(snap["market_id"])

    return alerts
