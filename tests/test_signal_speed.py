from datetime import datetime, timezone

from app.core.signal_speed import SIGNAL_SPEED_FAST, SIGNAL_SPEED_STANDARD, classify_signal_speed
from app.models import Alert


def _make_alert(**overrides):
    now_ts = datetime.now(timezone.utc)
    data = dict(
        tenant_id="tenant-1",
        alert_type="DISLOCATION",
        market_id="market-1",
        title="Sample Market",
        category="testing",
        move=0.04,
        market_p_yes=0.5,
        prev_market_p_yes=0.46,
        old_price=0.46,
        new_price=0.5,
        delta_pct=0.04,
        liquidity=10000.0,
        volume_24h=12000.0,
        strength="STRONG",
        snapshot_bucket=now_ts,
        source_ts=now_ts,
        message="Test alert",
        triggered_at=now_ts,
        created_at=now_ts,
    )
    data.update(overrides)
    return Alert(**data)


def test_signal_speed_fast_for_short_window():
    alert = _make_alert()
    alert.sustained_snapshots = 2
    assert classify_signal_speed(alert, 15) == SIGNAL_SPEED_FAST


def test_signal_speed_standard_for_long_window():
    alert = _make_alert()
    alert.sustained_snapshots = 2
    assert classify_signal_speed(alert, 60) == SIGNAL_SPEED_STANDARD
