from datetime import datetime, timedelta, timezone

from app.core.alert_classification import classify_alert
from app.models import Alert


def _make_alert(**overrides):
    now_ts = datetime.now(timezone.utc)
    data = dict(
        tenant_id="tenant-1",
        alert_type="DISLOCATION",
        market_id="market-1",
        title="Sample Market",
        category="testing",
        move=0.05,
        market_p_yes=0.44,
        prev_market_p_yes=0.42,
        old_price=0.42,
        new_price=0.44,
        delta_pct=0.05,
        liquidity=6000.0,
        volume_24h=6000.0,
        strength="MEDIUM",
        snapshot_bucket=now_ts,
        source_ts=now_ts,
        message="Test alert",
        triggered_at=now_ts,
        created_at=now_ts,
    )
    data.update(overrides)
    return Alert(**data)


def test_classify_alert_sustained_high_liquidity_move():
    now_ts = datetime.now(timezone.utc)
    alert = _make_alert(
        liquidity=6000.0,
        volume_24h=7000.0,
        old_price=0.42,
        new_price=0.44,
        snapshot_bucket=now_ts,
    )
    price_points = [
        (now_ts - timedelta(minutes=10), 0.40),
        (now_ts - timedelta(minutes=5), 0.42),
        (now_ts, 0.44),
        (now_ts + timedelta(minutes=5), 0.46),
        (now_ts + timedelta(minutes=10), 0.48),
    ]
    classification = classify_alert(alert, price_points=price_points)
    assert classification.signal_type == "REPRICING"
    assert classification.confidence == "HIGH"
    assert classification.suggested_action == "FOLLOW"


def test_classify_alert_single_snapshot_spike():
    now_ts = datetime.now(timezone.utc)
    alert = _make_alert(
        liquidity=1500.0,
        volume_24h=1500.0,
        old_price=0.40,
        new_price=0.45,
        snapshot_bucket=now_ts,
    )
    price_points = [
        (now_ts - timedelta(minutes=5), 0.40),
        (now_ts, 0.45),
        (now_ts + timedelta(minutes=5), 0.41),
        (now_ts + timedelta(minutes=10), 0.42),
    ]
    classification = classify_alert(alert, price_points=price_points)
    assert classification.signal_type == "LIQUIDITY_SWEEP"
    assert classification.confidence == "MEDIUM"
    assert classification.suggested_action == "WAIT"


def test_classify_alert_noisy_low_liquidity_move():
    now_ts = datetime.now(timezone.utc)
    alert = _make_alert(
        liquidity=200.0,
        volume_24h=150.0,
        old_price=0.40,
        new_price=0.41,
        snapshot_bucket=now_ts,
    )
    price_points = [
        (now_ts - timedelta(minutes=5), 0.40),
        (now_ts, 0.41),
        (now_ts + timedelta(minutes=5), 0.409),
    ]
    classification = classify_alert(alert, price_points=price_points)
    assert classification.signal_type == "NOISY"
    assert classification.confidence == "LOW"
    assert classification.suggested_action == "IGNORE"
