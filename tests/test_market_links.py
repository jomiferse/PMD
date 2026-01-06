from datetime import datetime, timezone
from types import SimpleNamespace

from sqlalchemy import create_engine, event
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import sessionmaker

from app.core.market_links import attach_market_slugs, market_url
from app.db import Base
from app.jobs.tasks import _build_snapshot_upsert_stmt
from app.models import MarketSnapshot


def _snapshot(market_id: str, slug: str | None) -> MarketSnapshot:
    now = datetime.now(timezone.utc)
    return MarketSnapshot(
        market_id=market_id,
        title="Test Market",
        category="testing",
        slug=slug,
        market_p_yes=0.5,
        primary_outcome_label="Yes",
        is_yesno=True,
        mapping_confidence="verified",
        market_kind="yesno",
        liquidity=1_000,
        volume_24h=2_000,
        volume_1w=3_000,
        best_ask=0.6,
        last_trade_price=0.55,
        model_p_yes=0.52,
        edge=0.02,
        source_ts=now,
        snapshot_bucket=now,
        asof_ts=now,
    )


def test_market_url_prefers_slug_when_available():
    url = market_url("900150", "race-2026")
    assert url.endswith("/race-2026")


def test_market_url_falls_back_when_slug_invalid():
    url = market_url("900150", "12345")
    assert url.endswith("/900150")


def test_snapshot_upsert_coalesces_slug():
    now = datetime.now(timezone.utc)
    row = {
        "market_id": "m1",
        "title": "t",
        "category": "c",
        "slug": "slug-1",
        "market_p_yes": 0.5,
        "primary_outcome_label": "Yes",
        "is_yesno": True,
        "mapping_confidence": "verified",
        "market_kind": "yesno",
        "liquidity": 1000.0,
        "volume_24h": 2000.0,
        "volume_1w": 3000.0,
        "best_ask": 0.6,
        "last_trade_price": 0.55,
        "model_p_yes": 0.52,
        "edge": 0.02,
        "source_ts": now,
        "snapshot_bucket": now,
        "asof_ts": now,
    }
    stmt = _build_snapshot_upsert_stmt([row], ["market_id", "snapshot_bucket"])
    compiled = stmt.compile(dialect=postgresql.dialect())
    sql = str(compiled).upper()
    assert "COALESCE" in sql
    assert "SLUG" in sql


def test_attach_market_slugs_batches_queries():
    engine = create_engine("sqlite:///:memory:")
    SessionLocal = sessionmaker(bind=engine)
    Base.metadata.create_all(engine)
    session = SessionLocal()
    session.add_all([_snapshot("m1", "slug-one"), _snapshot("m2", "slug-two")])
    session.commit()

    alerts = [SimpleNamespace(market_id="m1"), SimpleNamespace(market_id="m2")]
    counter = {"selects": 0}

    @event.listens_for(engine, "before_cursor_execute")
    def _count(_, __, statement, ___, ____, _____):
        if statement.lstrip().upper().startswith("SELECT"):
            counter["selects"] += 1

    counter["selects"] = 0
    attach_market_slugs(session, alerts)
    event.remove(engine, "before_cursor_execute", _count)

    assert counter["selects"] == 1
    assert alerts[0].market_slug == "slug-one"
    assert alerts[1].market_slug == "slug-two"
