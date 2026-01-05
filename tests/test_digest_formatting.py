from datetime import datetime, timezone

from app.core.alerts import _format_digest_message
from app.polymarket.client import _parse_markets
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
        mapping_confidence="verified",
        market_kind="yesno",
        is_yesno=True,
    )
    data.update(overrides)
    return Alert(**data)


def _alert_from_market(market: dict) -> Alert:
    event = {"title": "Event", "slug": "event-1", "markets": [market]}
    markets, parsed = _parse_markets([event], None, None)
    assert parsed == 1
    parsed_market = markets[0]
    return _make_alert(
        market_p_yes=parsed_market.p_primary,
        primary_outcome_label=parsed_market.primary_outcome_label,
        is_yesno=parsed_market.is_yesno,
        mapping_confidence=parsed_market.mapping_confidence,
        market_kind=parsed_market.market_kind,
    )


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
    market = {
        "id": "m-team",
        "question": "Who wins?",
        "outcomePrices": '["0.37","0.63"]',
        "outcomes": '["DAL","NYG"]',
        "liquidityNum": 2000,
        "volume24hr": 2000,
    }
    alert = _alert_from_market(market)
    text = _format_digest_message(
        alerts=[alert],
        window_minutes=60,
        total_actionable=1,
        user_name="Alice",
    )
    assert "p_DAL" in text
    assert "p_yes" not in text


def test_digest_formats_verified_ou_label_as_p_over():
    market = {
        "id": "m-ou",
        "question": "Total points?",
        "outcomePrices": '["0.44","0.56"]',
        "outcomeLabels": ["OVER", "UNDER"],
        "liquidityNum": 2000,
        "volume24hr": 2000,
    }
    alert = _alert_from_market(market)
    text = _format_digest_message(
        alerts=[alert],
        window_minutes=60,
        total_actionable=1,
        user_name="Alice",
    )
    assert "p_OVER" in text


def test_digest_unknown_mapping_falls_back_to_outcome0():
    market = {
        "id": "m-unknown",
        "question": "Who wins?",
        "outcomePrices": '["0.44","0.56"]',
        "liquidityNum": 2000,
        "volume24hr": 2000,
    }
    alert = _alert_from_market(market)
    text = _format_digest_message(
        alerts=[alert],
        window_minutes=60,
        total_actionable=1,
        user_name="Alice",
    )
    assert "p_outcome0" in text
    assert "p_OVER" not in text
