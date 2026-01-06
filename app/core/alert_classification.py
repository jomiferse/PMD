from dataclasses import dataclass
from datetime import datetime
from enum import Enum

from sqlalchemy.orm import Session

from ..models import Alert, MarketSnapshot
from . import defaults


class AlertClass(str, Enum):
    ACTIONABLE_FAST = "ACTIONABLE_FAST"
    ACTIONABLE_STANDARD = "ACTIONABLE_STANDARD"
    INFO_ONLY = "INFO_ONLY"


@dataclass(frozen=True)
class AlertClassification:
    signal_type: str
    confidence: str
    suggested_action: str
    alert_class: AlertClass | None = None
    market_kind: str | None = None
    abs_move: float | None = None
    pct_move: float | None = None
    sustained: bool | None = None


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
    pct_move = _alert_pct_move(alert, abs_move)
    large_move = abs_move >= defaults.STRONG_ABS_MOVE_THRESHOLD
    moderate_move = abs_move >= defaults.MEDIUM_ABS_MOVE_THRESHOLD

    high_liquidity = alert.liquidity >= defaults.STRONG_MIN_LIQUIDITY
    high_volume = alert.volume_24h >= defaults.STRONG_MIN_VOLUME_24H
    moderate_liquidity = alert.liquidity >= defaults.GLOBAL_MIN_LIQUIDITY
    moderate_volume = alert.volume_24h >= defaults.GLOBAL_MIN_VOLUME_24H
    base_price = max(alert.old_price or 0.0, alert.new_price or 0.0)
    low_base_price = base_price > 0 and (
        base_price < defaults.MIN_PRICE_THRESHOLD or base_price < defaults.FLOOR_PRICE
    )

    if low_base_price:
        return _with_class(
            AlertClassification(
                "NOISY",
                "LOW",
                "IGNORE",
                market_kind=getattr(alert, "market_kind", None),
                abs_move=abs_move,
                pct_move=pct_move,
                sustained=behavior.sustained,
            )
        )

    if behavior.sustained and not behavior.reversal and high_liquidity and high_volume:
        return _with_class(
            AlertClassification(
                "REPRICING",
                "HIGH",
                "FOLLOW",
                market_kind=getattr(alert, "market_kind", None),
                abs_move=abs_move,
                pct_move=pct_move,
                sustained=behavior.sustained,
            )
        )
    if behavior.sustained and not behavior.reversal and large_move and moderate_liquidity and moderate_volume:
        return _with_class(
            AlertClassification(
                "REPRICING",
                "MEDIUM",
                "FOLLOW",
                market_kind=getattr(alert, "market_kind", None),
                abs_move=abs_move,
                pct_move=pct_move,
                sustained=behavior.sustained,
            )
        )

    if large_move and (behavior.reversal or not behavior.sustained):
        if moderate_liquidity or moderate_volume:
            return _with_class(
                AlertClassification(
                    "LIQUIDITY_SWEEP",
                    "MEDIUM",
                    "WAIT",
                    market_kind=getattr(alert, "market_kind", None),
                    abs_move=abs_move,
                    pct_move=pct_move,
                    sustained=behavior.sustained,
                )
            )
        return _with_class(
            AlertClassification(
                "NOISY",
                "LOW",
                "IGNORE",
                market_kind=getattr(alert, "market_kind", None),
                abs_move=abs_move,
                pct_move=pct_move,
                sustained=behavior.sustained,
            )
        )

    if moderate_move and (behavior.reversal or behavior.flatline):
        if moderate_liquidity or moderate_volume:
            return _with_class(
                AlertClassification(
                    "LIQUIDITY_SWEEP",
                    "MEDIUM",
                    "WAIT",
                    market_kind=getattr(alert, "market_kind", None),
                    abs_move=abs_move,
                    pct_move=pct_move,
                    sustained=behavior.sustained,
                )
            )
        return _with_class(
            AlertClassification(
                "NOISY",
                "LOW",
                "IGNORE",
                market_kind=getattr(alert, "market_kind", None),
                abs_move=abs_move,
                pct_move=pct_move,
                sustained=behavior.sustained,
            )
        )

    if moderate_move and moderate_liquidity and moderate_volume and behavior.sustained:
        return _with_class(
            AlertClassification(
                "LIQUIDITY_SWEEP",
                "MEDIUM",
                "WAIT",
                market_kind=getattr(alert, "market_kind", None),
                abs_move=abs_move,
                pct_move=pct_move,
                sustained=behavior.sustained,
            )
        )

    return _with_class(
        AlertClassification(
            "NOISY",
            "LOW",
            "IGNORE",
            market_kind=getattr(alert, "market_kind", None),
            abs_move=abs_move,
            pct_move=pct_move,
            sustained=behavior.sustained,
        )
    )


def classify_alert_with_snapshots(db: Session, alert: Alert) -> AlertClassification:
    price_points = _load_price_points(db, alert)
    return classify_alert(alert, price_points=price_points)


def _alert_abs_move(alert: Alert) -> float:
    if alert.old_price is not None and alert.new_price is not None:
        return abs(alert.new_price - alert.old_price)
    if alert.delta_pct is not None:
        return abs(alert.delta_pct)
    return abs(alert.move or 0.0)


def _alert_pct_move(alert: Alert, abs_move: float) -> float | None:
    if alert.old_price is not None and alert.old_price > 0:
        return abs_move / max(alert.old_price, defaults.FLOOR_PRICE)
    if alert.delta_pct is not None:
        return abs(alert.delta_pct)
    if alert.move is not None:
        return abs(alert.move)
    return None


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
        abs(price - (alert.old_price or 0.0)) < defaults.MEDIUM_ABS_MOVE_THRESHOLD
        for _, price in post[1:]
    )
    reversal = reversal or snapback
    flatline = bool(deltas) and all(abs(delta) < defaults.MEDIUM_ABS_MOVE_THRESHOLD for delta in deltas)

    return AlertBehavior(sustained=sustained, reversal=reversal, flatline=flatline)


def _delta_matches(delta: float, direction: int) -> bool:
    return delta * direction > 0 and abs(delta) >= defaults.MEDIUM_ABS_MOVE_THRESHOLD


def _with_class(classification: AlertClassification) -> AlertClassification:
    if classification.alert_class is not None:
        return classification

    alert_class = _determine_alert_class(
        classification.signal_type,
        classification.confidence,
        market_kind=classification.market_kind,
        sustained=classification.sustained,
    )
    return AlertClassification(
        classification.signal_type,
        classification.confidence,
        classification.suggested_action,
        alert_class=alert_class,
        market_kind=classification.market_kind,
        abs_move=classification.abs_move,
        pct_move=classification.pct_move,
        sustained=classification.sustained,
    )


def _determine_alert_class(
    signal_type: str,
    confidence: str,
    market_kind: str | None = None,
    sustained: bool | None = None,
) -> AlertClass:
    normalized_signal = (signal_type or "").upper()
    normalized_conf = (confidence or "").upper()
    normalized_kind = (market_kind or "").lower()

    if normalized_signal in {"REPRICING", "LIQUIDITY_SWEEP"} and normalized_conf == "HIGH":
        return AlertClass.ACTIONABLE_FAST
    if normalized_signal in {"REPRICING", "LIQUIDITY_SWEEP"}:
        return AlertClass.ACTIONABLE_STANDARD
    if normalized_signal == "MOMENTUM" and normalized_conf in {"HIGH", "MEDIUM"}:
        return AlertClass.ACTIONABLE_STANDARD
    if normalized_kind in {"range"} and sustained:
        return AlertClass.ACTIONABLE_STANDARD
    return AlertClass.INFO_ONLY
