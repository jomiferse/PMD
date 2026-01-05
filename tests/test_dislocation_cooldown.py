from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.core.dislocation import ALERT_TYPE, compute_dislocation_alerts
from app.models import Alert, MarketSnapshot
from app.core import defaults


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


def _snapshot_payload(market_id: str, title: str, category: str, price: float, now_ts: datetime) -> dict:
    return {
        "market_id": market_id,
        "title": title,
        "category": category,
        "market_p_yes": price,
        "liquidity": defaults.MEDIUM_MIN_LIQUIDITY + 100,
        "volume_24h": defaults.MEDIUM_MIN_VOLUME_24H + 100,
        "snapshot_bucket": now_ts,
        "source_ts": now_ts,
    }


def test_cooldown_respects_alert_cooldown_minutes(db_session):
    now_ts = datetime.now(timezone.utc)
    prev_bucket = now_ts - timedelta(minutes=10)
    market_id = "market-1"

    prev_snapshot = MarketSnapshot(
        market_id=market_id,
        title="Test Market",
        category="testing",
        market_p_yes=0.4,
        liquidity=defaults.MEDIUM_MIN_LIQUIDITY + 100,
        volume_24h=defaults.MEDIUM_MIN_VOLUME_24H + 100,
        volume_1w=0.0,
        best_ask=0.0,
        last_trade_price=0.0,
        model_p_yes=0.5,
        edge=0.1,
        source_ts=prev_bucket,
        snapshot_bucket=prev_bucket,
        asof_ts=prev_bucket,
    )
    db_session.add(prev_snapshot)

    recent_alert = Alert(
        tenant_id="tenant-1",
        alert_type=ALERT_TYPE,
        market_id=market_id,
        title="Test Market",
        category="testing",
        move=0.5,
        market_p_yes=0.6,
        prev_market_p_yes=0.4,
        old_price=0.4,
        new_price=0.6,
        delta_pct=0.5,
        liquidity=defaults.MEDIUM_MIN_LIQUIDITY + 100,
        volume_24h=defaults.MEDIUM_MIN_VOLUME_24H + 100,
        strength="MEDIUM",
        snapshot_bucket=prev_bucket,
        source_ts=prev_bucket,
        message="Recent alert",
        triggered_at=now_ts - timedelta(minutes=5),
        created_at=now_ts - timedelta(minutes=5),
    )
    db_session.add(recent_alert)
    db_session.commit()

    alerts = compute_dislocation_alerts(
        db=db_session,
        snapshots=[_snapshot_payload(market_id, "Test Market", "testing", 0.6, now_ts)],
        window_minutes=defaults.WINDOW_MINUTES,
        medium_move_threshold=defaults.MEDIUM_MOVE_THRESHOLD,
        min_price_threshold=defaults.MIN_PRICE_THRESHOLD,
        medium_abs_move_threshold=defaults.MEDIUM_ABS_MOVE_THRESHOLD,
        floor_price=defaults.FLOOR_PRICE,
        medium_min_liquidity=defaults.MEDIUM_MIN_LIQUIDITY,
        medium_min_volume_24h=defaults.MEDIUM_MIN_VOLUME_24H,
        strong_abs_move_threshold=defaults.STRONG_ABS_MOVE_THRESHOLD,
        strong_min_liquidity=defaults.STRONG_MIN_LIQUIDITY,
        strong_min_volume_24h=defaults.STRONG_MIN_VOLUME_24H,
        cooldown_minutes=defaults.ALERT_COOLDOWN_MINUTES,
        tenant_id="tenant-1",
        use_triggered_at=True,
    )

    assert alerts == []
