from datetime import datetime, timezone

from app.core.alerts import _format_digest_message
from app.models import Alert


def _make_alert(**overrides):
    now_ts = datetime.now(timezone.utc)
    data = dict(
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
    data.update(overrides)
    return Alert(**data)


def test_digest_formatting_is_utf8_safe():
    alert = _make_alert()

    text = _format_digest_message(
        alerts=[alert],
        window_minutes=60,
        total_actionable=1,
        user_name="Alice",
    )

    assert text
    text.encode("utf-8")
    assert "\ufffd" not in text
    assert "PMD - 1 theme (60m)" in text
    assert "Rep:" in text
    assert "Move" in text
    assert "Liq" in text
    assert "https://polymarket.com/market/market-1" in text


def test_digest_includes_p_yes_delta_when_available():
    alert = _make_alert(prev_market_p_yes=0.4, market_p_yes=0.6)
    text = _format_digest_message(
        alerts=[alert],
        window_minutes=60,
        total_actionable=1,
        user_name="Alice",
    )
    assert "p_yes 40.0->60.0" in text


def test_digest_falls_back_to_p_yes_now_when_missing_prev():
    alert = _make_alert(prev_market_p_yes=None, market_p_yes=0.52)
    text = _format_digest_message(
        alerts=[alert],
        window_minutes=60,
        total_actionable=1,
        user_name="Alice",
    )
    assert "p_yes 52.0" in text
    assert "->" not in text


def test_digest_formats_yesno_as_p_yes():
    alert = _make_alert(primary_outcome_label="Yes", is_yesno=True)
    text = _format_digest_message(
        alerts=[alert],
        window_minutes=60,
        total_actionable=1,
        user_name="Alice",
    )
    assert "p_yes" in text


def test_digest_formats_non_yesno_as_primary_label():
    alert = _make_alert(primary_outcome_label="CAR", is_yesno=False)
    text = _format_digest_message(
        alerts=[alert],
        window_minutes=60,
        total_actionable=1,
        user_name="Alice",
    )
    assert "p_CAR" in text
    assert "p_yes" not in text
