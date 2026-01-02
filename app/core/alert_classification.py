from dataclasses import dataclass
from datetime import datetime

from sqlalchemy.orm import Session

from ..models import Alert, MarketSnapshot
from ..settings import settings


@dataclass(frozen=True)
class AlertClassification:
    signal_type: str
    confidence: str
    suggested_action: str


@dataclass(frozen=True)
class AlertBehavior:
    sustained: bool
    reversal: bool
    flatline: bool


def classify_alert(
    alert: Alert,
    price_points: list[tuple[datetime, float]] | None = None,
) -> AlertClassification:
    behavior = _analyze_price_behavior(alert, price_points or [])
    abs_move = _alert_abs_move(alert)
    large_move = abs_move >= settings.STRONG_ABS_MOVE_THRESHOLD
    moderate_move = abs_move >= settings.MEDIUM_ABS_MOVE_THRESHOLD

    high_liquidity = alert.liquidity >= settings.STRONG_MIN_LIQUIDITY
    high_volume = alert.volume_24h >= settings.STRONG_MIN_VOLUME_24H
    moderate_liquidity = alert.liquidity >= settings.GLOBAL_MIN_LIQUIDITY
    moderate_volume = alert.volume_24h >= settings.GLOBAL_MIN_VOLUME_24H
    base_price = max(alert.old_price or 0.0, alert.new_price or 0.0)
    low_base_price = base_price > 0 and (
        base_price < settings.MIN_PRICE_THRESHOLD or base_price < settings.FLOOR_PRICE
    )

    if low_base_price:
        return AlertClassification("NOISY", "LOW", "IGNORE")

    if behavior.sustained and not behavior.reversal and high_liquidity and high_volume:
        return AlertClassification("REPRICING", "HIGH", "FOLLOW")
    if behavior.sustained and not behavior.reversal and large_move and moderate_liquidity and moderate_volume:
        return AlertClassification("REPRICING", "MEDIUM", "FOLLOW")

    if large_move and (behavior.reversal or not behavior.sustained):
        if moderate_liquidity or moderate_volume:
            return AlertClassification("LIQUIDITY_SWEEP", "MEDIUM", "WAIT")
        return AlertClassification("NOISY", "LOW", "IGNORE")

    if moderate_move and (behavior.reversal or behavior.flatline):
        if moderate_liquidity or moderate_volume:
            return AlertClassification("LIQUIDITY_SWEEP", "MEDIUM", "WAIT")
        return AlertClassification("NOISY", "LOW", "IGNORE")

    if moderate_move and moderate_liquidity and moderate_volume and behavior.sustained:
        return AlertClassification("LIQUIDITY_SWEEP", "MEDIUM", "WAIT")

    return AlertClassification("NOISY", "LOW", "IGNORE")


def classify_alert_with_snapshots(db: Session, alert: Alert) -> AlertClassification:
    price_points = _load_price_points(db, alert)
    return classify_alert(alert, price_points=price_points)


def _alert_abs_move(alert: Alert) -> float:
    if alert.old_price is not None and alert.new_price is not None:
        return abs(alert.new_price - alert.old_price)
    if alert.delta_pct is not None:
        return abs(alert.delta_pct)
    return abs(alert.move or 0.0)


def _load_price_points(db: Session, alert: Alert, max_points: int = 5) -> list[tuple[datetime, float]]:
    if alert.snapshot_bucket is None:
        return []
    before = (
        db.query(MarketSnapshot.snapshot_bucket, MarketSnapshot.market_p_yes)
        .filter(
            MarketSnapshot.market_id == alert.market_id,
            MarketSnapshot.snapshot_bucket <= alert.snapshot_bucket,
            MarketSnapshot.market_p_yes.isnot(None),
        )
        .order_by(MarketSnapshot.snapshot_bucket.desc())
        .limit(max_points)
        .all()
    )
    after = (
        db.query(MarketSnapshot.snapshot_bucket, MarketSnapshot.market_p_yes)
        .filter(
            MarketSnapshot.market_id == alert.market_id,
            MarketSnapshot.snapshot_bucket >= alert.snapshot_bucket,
            MarketSnapshot.market_p_yes.isnot(None),
        )
        .order_by(MarketSnapshot.snapshot_bucket.asc())
        .limit(max_points)
        .all()
    )
    points: dict[datetime, float] = {}
    for bucket, price in before + after:
        points[bucket] = price
    return sorted(points.items(), key=lambda item: item[0])


def _analyze_price_behavior(
    alert: Alert,
    price_points: list[tuple[datetime, float]],
) -> AlertBehavior:
    if len(price_points) < 3 or alert.snapshot_bucket is None:
        return AlertBehavior(sustained=False, reversal=False, flatline=False)

    points = sorted(price_points, key=lambda item: item[0])
    direction = 1 if (alert.new_price or 0.0) - (alert.old_price or 0.0) >= 0 else -1

    idx = min(
        range(len(points)),
        key=lambda i: abs((points[i][0] - alert.snapshot_bucket).total_seconds()),
    )
    post = points[idx:]
    deltas = [post[i + 1][1] - post[i][1] for i in range(len(post) - 1)]
    sustained = False
    for i in range(len(deltas) - 1):
        if _delta_matches(deltas[i], direction) and _delta_matches(deltas[i + 1], direction):
            sustained = True
            break

    reversal = any(_delta_matches(delta, -direction) for delta in deltas)
    snapback = any(
        abs(price - (alert.old_price or 0.0)) < settings.MEDIUM_ABS_MOVE_THRESHOLD
        for _, price in post[1:]
    )
    reversal = reversal or snapback
    flatline = bool(deltas) and all(abs(delta) < settings.MEDIUM_ABS_MOVE_THRESHOLD for delta in deltas)

    return AlertBehavior(sustained=sustained, reversal=reversal, flatline=flatline)


def _delta_matches(delta: float, direction: int) -> bool:
    return delta * direction > 0 and abs(delta) >= settings.MEDIUM_ABS_MOVE_THRESHOLD
