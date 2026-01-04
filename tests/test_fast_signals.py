from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.core.alerts import UserDigestConfig, _format_fast_digest_message, _prepare_fast_digest
from app.core.fast_signals import compute_fast_signals
from app.models import Alert, MarketSnapshot
from app.settings import settings


@pytest.fixture()
def db_session():
    engine = create_engine("sqlite:///:memory:", future=True)
    MarketSnapshot.__table__.create(bind=engine)
    Alert.__table__.create(bind=engine)
    SessionLocal = sessionmaker(bind=engine)
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()


def _snapshot_payload(
    market_id: str,
    title: str,
    category: str,
    price: float,
    now_ts: datetime,
    liquidity: float,
    volume_24h: float,
) -> dict:
    return {
        "market_id": market_id,
        "title": title,
        "category": category,
        "market_p_yes": price,
        "liquidity": liquidity,
        "volume_24h": volume_24h,
        "snapshot_bucket": now_ts,
        "source_ts": now_ts,
    }


def _make_pref(enabled: bool) -> UserDigestConfig:
    return UserDigestConfig(
        user_id=uuid4(),
        name="Trader",
        telegram_chat_id="12345",
        min_liquidity=0.0,
        min_volume_24h=0.0,
        min_abs_price_move=0.0,
        alert_strengths={"STRONG", "MEDIUM"},
        digest_window_minutes=60,
        max_alerts_per_digest=10,
        ai_copilot_enabled=False,
        risk_budget_usd_per_day=0.0,
        max_usd_per_trade=0.0,
        max_liquidity_fraction=0.01,
        fast_signals_enabled=enabled,
    )


def test_fast_signals_respect_window(db_session):
    now_ts = datetime.now(timezone.utc)
    market_id = "fast-window"

    older_bucket = now_ts - timedelta(minutes=20)
    db_session.add(
        MarketSnapshot(
            market_id=market_id,
            title="Fast Window Market",
            category="testing",
            market_p_yes=0.4,
            liquidity=30000.0,
            volume_24h=30000.0,
            volume_1w=0.0,
            best_ask=0.0,
            last_trade_price=0.0,
            model_p_yes=0.5,
            edge=0.1,
            source_ts=older_bucket,
            snapshot_bucket=older_bucket,
            asof_ts=older_bucket,
        )
    )
    db_session.commit()

    alerts = compute_fast_signals(
        db=db_session,
        snapshots=[
            _snapshot_payload(
                market_id,
                "Fast Window Market",
                "testing",
                0.44,
                now_ts,
                liquidity=30000.0,
                volume_24h=30000.0,
            )
        ],
        window_minutes=10,
        min_liquidity=20000.0,
        min_volume_24h=20000.0,
        min_abs_move=0.015,
        min_pct_move=0.05,
        p_yes_min=0.15,
        p_yes_max=0.85,
        cooldown_minutes=0,
        tenant_id="tenant-1",
        use_triggered_at=True,
    )

    assert alerts == []


def test_fast_signals_respect_stricter_thresholds(db_session):
    now_ts = datetime.now(timezone.utc)
    market_id = "fast-thresholds"

    prev_bucket = now_ts - timedelta(minutes=5)
    db_session.add(
        MarketSnapshot(
            market_id=market_id,
            title="Fast Threshold Market",
            category="testing",
            market_p_yes=0.4,
            liquidity=30000.0,
            volume_24h=30000.0,
            volume_1w=0.0,
            best_ask=0.0,
            last_trade_price=0.0,
            model_p_yes=0.5,
            edge=0.1,
            source_ts=prev_bucket,
            snapshot_bucket=prev_bucket,
            asof_ts=prev_bucket,
        )
    )
    db_session.commit()

    alerts = compute_fast_signals(
        db=db_session,
        snapshots=[
            _snapshot_payload(
                market_id,
                "Fast Threshold Market",
                "testing",
                0.44,
                now_ts,
                liquidity=15000.0,
                volume_24h=30000.0,
            )
        ],
        window_minutes=10,
        min_liquidity=20000.0,
        min_volume_24h=20000.0,
        min_abs_move=0.015,
        min_pct_move=0.05,
        p_yes_min=0.15,
        p_yes_max=0.85,
        cooldown_minutes=0,
        tenant_id="tenant-1",
        use_triggered_at=True,
    )

    assert alerts == []

    alerts = compute_fast_signals(
        db=db_session,
        snapshots=[
            _snapshot_payload(
                market_id,
                "Fast Threshold Market",
                "testing",
                0.44,
                now_ts,
                liquidity=30000.0,
                volume_24h=30000.0,
            )
        ],
        window_minutes=10,
        min_liquidity=20000.0,
        min_volume_24h=20000.0,
        min_abs_move=0.015,
        min_pct_move=0.05,
        p_yes_min=0.15,
        p_yes_max=0.85,
        cooldown_minutes=0,
        tenant_id="tenant-1",
        use_triggered_at=True,
    )

    assert len(alerts) == 1
    assert alerts[0].strength in {"LOW", "MEDIUM"}


def test_fast_formatting_is_watch_only():
    now_ts = datetime.now(timezone.utc)
    alert = Alert(
        tenant_id="tenant-1",
        alert_type="FAST_DISLOCATION",
        market_id="fast-market",
        title="BTC above 90k",
        category="testing",
        move=0.1,
        market_p_yes=0.52,
        prev_market_p_yes=0.48,
        old_price=0.48,
        new_price=0.52,
        delta_pct=0.1,
        liquidity=30000.0,
        volume_24h=30000.0,
        strength="MEDIUM",
        snapshot_bucket=now_ts,
        source_ts=now_ts,
        message="fast",
        triggered_at=now_ts,
        created_at=now_ts,
    )

    text = _format_fast_digest_message([alert], window_minutes=15)
    assert "WATCH" in text
    assert "FOLLOW" not in text
    assert "STRONG" not in text


def test_fast_not_prepared_when_user_pref_disabled(db_session):
    original_enabled = settings.FAST_SIGNALS_ENABLED
    settings.FAST_SIGNALS_ENABLED = True
    try:
        payload, reason = _prepare_fast_digest(
            db_session,
            "tenant-1",
            _make_pref(enabled=False),
            datetime.now(timezone.utc),
            include_footer=True,
        )
    finally:
        settings.FAST_SIGNALS_ENABLED = original_enabled

    assert payload is None
    assert reason == "fast_disabled"


def test_fast_throttle_blocks_digest(db_session, monkeypatch):
    original_enabled = settings.FAST_SIGNALS_ENABLED
    settings.FAST_SIGNALS_ENABLED = True
    try:
        monkeypatch.setattr("app.core.alerts._fast_digest_recently_sent", lambda *args, **kwargs: True)
        payload, reason = _prepare_fast_digest(
            db_session,
            "tenant-1",
            _make_pref(enabled=True),
            datetime.now(timezone.utc),
            include_footer=True,
        )
    finally:
        settings.FAST_SIGNALS_ENABLED = original_enabled

    assert payload is None
    assert reason == "recent_fast_digest"


def test_fast_theme_caps_apply():
    now_ts = datetime.now(timezone.utc)
    alerts = [
        Alert(
            tenant_id="tenant-1",
            alert_type="FAST_DISLOCATION",
            market_id="btc-above-90k",
            title="Will the price of Bitcoin be above $90,000 on January 3?",
            category="testing",
            move=0.1,
            market_p_yes=0.52,
            prev_market_p_yes=0.48,
            old_price=0.48,
            new_price=0.52,
            delta_pct=0.1,
            liquidity=30000.0,
            volume_24h=30000.0,
            strength="LOW",
            snapshot_bucket=now_ts,
            source_ts=now_ts,
            message="fast",
            triggered_at=now_ts,
            created_at=now_ts,
        ),
        Alert(
            tenant_id="tenant-1",
            alert_type="FAST_DISLOCATION",
            market_id="btc-88-90",
            title="Will the price of Bitcoin be between $88,000 and $90,000 on January 3?",
            category="testing",
            move=0.08,
            market_p_yes=0.5,
            prev_market_p_yes=0.47,
            old_price=0.47,
            new_price=0.5,
            delta_pct=0.08,
            liquidity=30000.0,
            volume_24h=30000.0,
            strength="LOW",
            snapshot_bucket=now_ts,
            source_ts=now_ts,
            message="fast",
            triggered_at=now_ts,
            created_at=now_ts,
        ),
        Alert(
            tenant_id="tenant-1",
            alert_type="FAST_DISLOCATION",
            market_id="btc-90-92",
            title="Will the price of Bitcoin be between $90,000 and $92,000 on January 3?",
            category="testing",
            move=0.07,
            market_p_yes=0.49,
            prev_market_p_yes=0.46,
            old_price=0.46,
            new_price=0.49,
            delta_pct=0.07,
            liquidity=30000.0,
            volume_24h=30000.0,
            strength="LOW",
            snapshot_bucket=now_ts,
            source_ts=now_ts,
            message="fast",
            triggered_at=now_ts,
            created_at=now_ts,
        ),
    ]

    original_themes = settings.FAST_MAX_THEMES_PER_DIGEST
    original_markets = settings.FAST_MAX_MARKETS_PER_THEME
    settings.FAST_MAX_THEMES_PER_DIGEST = 1
    settings.FAST_MAX_MARKETS_PER_THEME = 1
    try:
        text = _format_fast_digest_message(alerts, window_minutes=15)
    finally:
        settings.FAST_MAX_THEMES_PER_DIGEST = original_themes
        settings.FAST_MAX_MARKETS_PER_THEME = original_markets

    assert "PMD - FAST: 1 watchlist theme" in text
    assert text.count("\n- ") == 1
