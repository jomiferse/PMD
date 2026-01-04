from datetime import datetime, timezone

from app.core.alert_classification import AlertClassification
from app.core.alerts import _format_digest_message, group_alerts_into_themes
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
        market_p_yes=0.5,
        prev_market_p_yes=0.45,
        old_price=0.45,
        new_price=0.5,
        delta_pct=0.05,
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


def test_btc_range_alerts_group_into_single_theme():
    alerts = [
        _make_alert(
            market_id="btc-above-90k",
            title="Will the price of Bitcoin be above $90,000 on January 3?",
        ),
        _make_alert(
            market_id="btc-88-90",
            title="Will the price of Bitcoin be between $88,000 and $90,000 on January 3?",
        ),
        _make_alert(
            market_id="btc-90-92",
            title="Will the price of Bitcoin be between $90,000 and $92,000 on January 3?",
        ),
    ]

    themes = group_alerts_into_themes(alerts)

    assert len(themes) == 1
    assert len(themes[0].alerts) == 3


def test_representative_selection_prefers_stronger_move():
    def _fake_classify(alert):
        return AlertClassification("REPRICING", "HIGH", "FOLLOW")

    alerts = [
        _make_alert(
            market_id="market-1",
            move=0.05,
            old_price=None,
            new_price=None,
            delta_pct=None,
            liquidity=1000.0,
            volume_24h=1000.0,
        ),
        _make_alert(
            market_id="market-2",
            move=0.2,
            old_price=None,
            new_price=None,
            delta_pct=None,
            liquidity=900.0,
            volume_24h=900.0,
        ),
        _make_alert(
            market_id="market-3",
            move=0.1,
            old_price=None,
            new_price=None,
            delta_pct=None,
            liquidity=2000.0,
            volume_24h=2000.0,
        ),
    ]

    themes = group_alerts_into_themes(alerts, classifier=_fake_classify)

    assert themes[0].representative.market_id == "market-2"


def test_grouped_formatting_includes_theme_header_and_related_markets():
    alerts = [
        _make_alert(
            market_id="btc-above-90k",
            title="Will the price of Bitcoin be above $90,000 on January 3?",
        ),
        _make_alert(
            market_id="btc-88-90",
            title="Will the price of Bitcoin be between $88,000 and $90,000 on January 3?",
        ),
        _make_alert(
            market_id="btc-90-92",
            title="Will the price of Bitcoin be between $90,000 and $92,000 on January 3?",
        ),
    ]

    text = _format_digest_message(
        alerts=alerts,
        window_minutes=60,
        total_actionable=3,
        user_name="Alice",
    )

    assert "PMD - 1 theme (60m)" in text
    assert "#1 THEME" in text
    assert "Rep:" in text
    assert "range" in text


def test_grouping_is_deterministic_for_same_input_order():
    alerts = [
        _make_alert(
            market_id="btc-above-90k",
            title="Will the price of Bitcoin be above $90,000 on January 3?",
        ),
        _make_alert(
            market_id="btc-88-90",
            title="Will the price of Bitcoin be between $88,000 and $90,000 on January 3?",
        ),
        _make_alert(
            market_id="hawks-knicks",
            title="Hawks vs Knicks on January 3?",
        ),
        _make_alert(
            market_id="lakers-bulls",
            title="Lakers vs Bulls on January 3?",
        ),
    ]

    first = [theme.key for theme in group_alerts_into_themes(alerts)]
    second = [theme.key for theme in group_alerts_into_themes(alerts)]

    assert first == second
