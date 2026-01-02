from datetime import datetime, timezone

from app.core.alerts import _format_digest_message
from app.models import Alert


def test_digest_formatting_is_utf8_safe():
    now_ts = datetime.now(timezone.utc)
    alert = Alert(
        tenant_id="tenant-1",
        alert_type="DISLOCATION",
        market_id="market-1",
        title="Sample Market",
        category="testing",
        move=0.5,
        market_p_yes=0.6,
        prev_market_p_yes=0.4,
        old_price=0.4,
        new_price=0.6,
        delta_pct=0.5,
        liquidity=25000.0,
        volume_24h=50000.0,
        strength="STRONG",
        snapshot_bucket=now_ts,
        source_ts=now_ts,
        message="Test alert",
        triggered_at=now_ts,
        created_at=now_ts,
    )

    text = _format_digest_message(
        strong_alerts=[alert],
        medium_alerts=[],
        window_minutes=60,
        total_strong=1,
        total_medium=0,
        user_name="Alice",
    )

    assert text
    text.encode("utf-8")
    assert "\ufffd" not in text
