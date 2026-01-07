from datetime import datetime, timedelta, timezone

from app.core.alerts import _format_digest_message
from app.polymarket.client import _parse_markets
from app.models import Alert, MarketSnapshot
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


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


def test_digest_prefers_slug_in_links():
    alert = _make_alert(market_id="market-123")
    alert.market_slug = "human-readable-slug"

    text = _format_digest_message(
        alerts=[alert],
        window_minutes=60,
        total_actionable=1,
        user_name="Alice",
    )

    assert "https://polymarket.com/market/human-readable-slug" in text
    assert "https://polymarket.com/market/market-123" not in text


def test_digest_link_falls_back_to_market_id_when_slug_missing():
    alert = _make_alert(market_id="market-456")
    alert.market_slug = None

    text = _format_digest_message(
        alerts=[alert],
        window_minutes=60,
        total_actionable=1,
        user_name="Alice",
    )

    assert "https://polymarket.com/market/market-456" in text


def test_digest_compact_move_sign_negative():
    alert = _make_alert(prev_market_p_yes=0.22, market_p_yes=0.205, old_price=0.22, new_price=0.205)
    text = _format_digest_message(
        alerts=[alert],
        window_minutes=60,
        total_actionable=1,
        user_name="Alice",
    )
    assert "Move -6.8%" in text


def test_digest_compact_move_sign_positive():
    alert = _make_alert(prev_market_p_yes=0.205, market_p_yes=0.22, old_price=0.205, new_price=0.22)
    text = _format_digest_message(
        alerts=[alert],
        window_minutes=60,
        total_actionable=1,
        user_name="Alice",
    )
    assert "Move +7.3%" in text


def test_digest_compact_move_uses_floor_price_with_sign():
    alert = _make_alert(prev_market_p_yes=0.02, market_p_yes=0.015, old_price=0.02, new_price=0.015)
    text = _format_digest_message(
        alerts=[alert],
        window_minutes=60,
        total_actionable=1,
        user_name="Alice",
    )
    assert "Move -10.0%" in text


def test_digest_includes_theme_evidence_line():
    now_ts = datetime.now(timezone.utc)
    alert = _make_alert(snapshot_bucket=now_ts, market_id="market-1", old_price=0.4, new_price=0.6)

    engine = create_engine("sqlite:///:memory:", future=True)
    MarketSnapshot.__table__.create(bind=engine)
    SessionLocal = sessionmaker(bind=engine)
    session = SessionLocal()
    try:
        snapshots = [
            MarketSnapshot(
                market_id="market-1",
                title="Sample Market",
                category="testing",
                market_p_yes=0.4,
                model_p_yes=0.4,
                edge=0.0,
                snapshot_bucket=now_ts - timedelta(minutes=10),
            ),
            MarketSnapshot(
                market_id="market-1",
                title="Sample Market",
                category="testing",
                market_p_yes=0.5,
                model_p_yes=0.5,
                edge=0.0,
                snapshot_bucket=now_ts - timedelta(minutes=5),
            ),
            MarketSnapshot(
                market_id="market-1",
                title="Sample Market",
                category="testing",
                market_p_yes=0.6,
                model_p_yes=0.6,
                edge=0.0,
                snapshot_bucket=now_ts,
            ),
        ]
        session.add_all(snapshots)
        session.commit()

        text = _format_digest_message(
            alerts=[alert],
            window_minutes=15,
            total_actionable=1,
            user_name="Alice",
            db=session,
            now_ts=now_ts,
        )

        assert "sustained_snapshots=3" in text
        assert "reversal_flag=none" in text
    finally:
        session.close()
