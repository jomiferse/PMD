from datetime import datetime, timezone

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from starlette.requests import Request

from app.auth import hash_api_key
from app.core.snapshots import backfill_market_p_no
from app.db import Base
from app.jobs.tasks import _build_snapshot_row
from app.models import Alert, ApiKey, MarketSnapshot
from app.polymarket.schemas import PolymarketMarket
from app.settings import settings
from app.api.routes.alerts import alert_history


@pytest.fixture()
def db_session():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)
    SessionLocal = sessionmaker(bind=engine)
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()


def _make_request(api_key: str) -> Request:
    scope = {"type": "http", "headers": [(b"x-api-key", api_key.encode())]}
    return Request(scope)


def test_backfill_sets_market_p_no_for_yesno(db_session):
    now_ts = datetime.now(timezone.utc)
    snap = MarketSnapshot(
        market_id="m1",
        title="Test market",
        category="test",
        market_p_yes=0.2,
        market_p_no=None,
        market_p_no_derived=None,
        is_yesno=True,
        model_p_yes=0.2,
        edge=0.0,
        snapshot_bucket=now_ts,
        asof_ts=now_ts,
    )
    snap_non_yesno = MarketSnapshot(
        market_id="m2",
        title="Multi market",
        category="test",
        market_p_yes=0.4,
        market_p_no=None,
        market_p_no_derived=None,
        is_yesno=False,
        model_p_yes=0.4,
        edge=0.0,
        snapshot_bucket=now_ts,
        asof_ts=now_ts,
    )
    db_session.add_all([snap, snap_non_yesno])
    db_session.commit()

    backfill_market_p_no(db_session)
    db_session.commit()

    db_session.refresh(snap)
    db_session.refresh(snap_non_yesno)
    assert snap.market_p_no == pytest.approx(0.8)
    assert snap.market_p_no_derived is True
    assert snap_non_yesno.market_p_no is None


def test_build_snapshot_row_sets_source_p_no():
    now_ts = datetime.now(timezone.utc)
    market = PolymarketMarket(
        market_id="m3",
        title="Yes/No market",
        category="test",
        p_primary=0.3,
        p_no=0.7,
        outcome_prices=[0.3, 0.7],
        primary_outcome_label="Yes",
        mapping_confidence="verified",
        market_kind="yesno",
        is_yesno=True,
        liquidity=1000.0,
        volume_24h=100.0,
        source_ts=now_ts,
    )

    key, row = _build_snapshot_row(market, now_ts)
    assert key is not None
    assert row["market_p_no"] == pytest.approx(0.7)
    assert row["market_p_no_derived"] is False


def test_build_snapshot_row_sets_derived_p_no():
    now_ts = datetime.now(timezone.utc)
    market = PolymarketMarket(
        market_id="m4",
        title="Derived market",
        category="test",
        p_primary=0.35,
        outcome_prices=[0.35],
        primary_outcome_label="Yes",
        mapping_confidence="verified",
        market_kind="yesno",
        is_yesno=True,
        liquidity=1000.0,
        volume_24h=100.0,
        source_ts=now_ts,
    )

    key, row = _build_snapshot_row(market, now_ts)
    assert key is not None
    assert row["market_p_no"] == pytest.approx(0.65)
    assert row["market_p_no_derived"] is True


def test_alert_history_includes_p_no(db_session):
    now_ts = datetime.now(timezone.utc)
    api_key_raw = "test-key"
    api_key = ApiKey(
        tenant_id=settings.DEFAULT_TENANT_ID,
        name="test",
        key_hash=hash_api_key(api_key_raw),
    )
    alert = Alert(
        tenant_id=settings.DEFAULT_TENANT_ID,
        alert_type="DISLOCATION",
        market_id="m5",
        title="History market",
        category="test",
        market_p_yes=0.2,
        prev_market_p_yes=0.2,
        is_yesno=True,
        market_kind="yesno",
        snapshot_bucket=now_ts,
    )
    snapshot = MarketSnapshot(
        market_id="m5",
        title="History market",
        category="test",
        market_p_yes=0.2,
        market_p_no=0.8,
        market_p_no_derived=False,
        is_yesno=True,
        model_p_yes=0.2,
        edge=0.0,
        snapshot_bucket=now_ts,
        asof_ts=now_ts,
    )
    db_session.add_all([api_key, alert, snapshot])
    db_session.commit()

    payload = alert_history(alert.id, _make_request(api_key_raw), db_session, range="1h")
    assert payload["points"][0]["p_no"] == pytest.approx(0.8)
