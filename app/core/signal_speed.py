from __future__ import annotations

from ..models import Alert

SIGNAL_SPEED_FAST = "FAST"
SIGNAL_SPEED_STANDARD = "STANDARD"


def classify_signal_speed(alert: Alert, window_minutes: int) -> str:
    abs_move = _alert_abs_move(alert)
    sustained_snapshots = _sustained_snapshot_count(alert)
    if window_minutes <= 15 and abs_move >= 0.03 and sustained_snapshots >= 2:
        return SIGNAL_SPEED_FAST
    return SIGNAL_SPEED_STANDARD


def _alert_abs_move(alert: Alert) -> float:
    if alert.old_price is not None and alert.new_price is not None:
        return abs(alert.new_price - alert.old_price)
    if alert.delta_pct is not None:
        return abs(alert.delta_pct)
    return abs(alert.move or 0.0)


def _sustained_snapshot_count(alert: Alert) -> int:
    sustained = getattr(alert, "sustained_snapshots", None)
    if sustained is None:
        return 0
    try:
        return int(sustained)
    except (TypeError, ValueError):
        return 0
