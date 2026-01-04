from datetime import datetime, timedelta, timezone

from sqlalchemy.orm import Session

from ..models import Alert, MarketSnapshot
from ..settings import settings

FAST_ALERT_TYPE = "FAST_DISLOCATION"


def compute_fast_signals(
    db: Session,
    snapshots: list[dict],
    window_minutes: int,
    min_liquidity: float,
    min_volume_24h: float,
    min_abs_move: float,
    min_pct_move: float,
    p_yes_min: float,
    p_yes_max: float,
    cooldown_minutes: int,
    tenant_id: str,
    use_triggered_at: bool = True,
) -> list[Alert]:
    if not snapshots:
        return []

    now_ts = datetime.now(timezone.utc)
    window_start = now_ts - timedelta(minutes=window_minutes)
    cooldown_start = now_ts - timedelta(minutes=cooldown_minutes)

    alerts: list[Alert] = []
    seen_market_ids: set[str] = set()

    for snap in snapshots:
        if snap["liquidity"] < min_liquidity:
            continue
        if snap["volume_24h"] < min_volume_24h:
            continue
        if snap["market_id"] in seen_market_ids:
            continue

        market_p_yes = snap["market_p_yes"]
        if market_p_yes is None or not (p_yes_min <= market_p_yes <= p_yes_max):
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
        if not prev or prev.market_p_yes is None:
            continue

        old_price = prev.market_p_yes
        new_price = market_p_yes
        if old_price <= 0 or new_price <= 0:
            continue
        if old_price == new_price:
            continue
        if old_price < settings.MIN_PRICE_THRESHOLD and new_price < settings.MIN_PRICE_THRESHOLD:
            continue

        abs_move = abs(new_price - old_price)
        if abs_move < min_abs_move:
            continue

        delta_pct = abs_move / max(old_price, settings.FLOOR_PRICE)
        if delta_pct < min_pct_move:
            continue

        cooldown_field = Alert.triggered_at if use_triggered_at else Alert.created_at
        recent = (
            db.query(Alert.id)
            .filter(
                Alert.tenant_id == tenant_id,
                Alert.alert_type == FAST_ALERT_TYPE,
                Alert.market_id == snap["market_id"],
                cooldown_field >= cooldown_start,
            )
            .limit(1)
            .one_or_none()
        )
        if recent:
            continue

        confidence = _fast_confidence(
            db,
            snap["market_id"],
            snap["snapshot_bucket"],
            window_start,
            min_abs_move=min_abs_move,
        )
        message = f"FAST {confidence} watchlist move {delta_pct * 100:.1f}% over {window_minutes}m"

        alerts.append(
            Alert(
                tenant_id=tenant_id,
                alert_type=FAST_ALERT_TYPE,
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
                strength=confidence,
                snapshot_bucket=snap["snapshot_bucket"],
                source_ts=snap["source_ts"],
                message=message,
                triggered_at=now_ts,
                created_at=now_ts,
            )
        )
        seen_market_ids.add(snap["market_id"])

    return alerts


def _fast_confidence(
    db: Session,
    market_id: str,
    snapshot_bucket: datetime,
    window_start: datetime,
    min_abs_move: float,
) -> str:
    points = (
        db.query(MarketSnapshot.snapshot_bucket, MarketSnapshot.market_p_yes)
        .filter(
            MarketSnapshot.market_id == market_id,
            MarketSnapshot.snapshot_bucket >= window_start,
            MarketSnapshot.snapshot_bucket <= snapshot_bucket,
            MarketSnapshot.market_p_yes.isnot(None),
        )
        .order_by(MarketSnapshot.snapshot_bucket.desc())
        .limit(3)
        .all()
    )
    if len(points) < 3:
        return "LOW"

    ordered = sorted(points, key=lambda item: item[0])
    direction = 1 if ordered[-1][1] - ordered[0][1] >= 0 else -1
    deltas = [ordered[i + 1][1] - ordered[i][1] for i in range(len(ordered) - 1)]

    threshold = min_abs_move * 0.5
    consistent = all(delta * direction > 0 and abs(delta) >= threshold for delta in deltas)
    return "MEDIUM" if consistent else "LOW"
