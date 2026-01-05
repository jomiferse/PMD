from datetime import datetime, timezone
from uuid import uuid4

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.alerts.theme_key import extract_theme
from app.core.ai_copilot import _build_draft, create_ai_recommendation
from app.core.user_settings import get_effective_user_settings
from app.db import Base
from app.models import Alert, User


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


class FakeRedis:
    def __init__(self):
        self.store = {}

    def get(self, key):
        return self.store.get(key)

    def set(self, key, value, nx=False, ex=None):
        if nx and key in self.store:
            return None
        self.store[key] = value
        return True


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


def test_copilot_dedupe_skips_llm(db_session, monkeypatch):
    user = User(user_id=uuid4(), name="Trader", telegram_chat_id="123", copilot_enabled=True)
    alert = _make_alert(title="Will the price of Bitcoin be above $50 on Jan 5 2026?")
    db_session.add_all([user, alert])
    db_session.commit()

    fake_redis = FakeRedis()
    theme_key = extract_theme(alert.title, category=alert.category, slug=alert.market_id).theme_key
    fake_redis.store[f"copilot:sent:{user.user_id}:{theme_key}"] = "1"
    monkeypatch.setattr("app.core.ai_copilot.redis_conn", fake_redis)

    def _should_not_call(_payload):
        raise AssertionError("LLM should not be called when copilot is deduped")

    monkeypatch.setattr("app.core.ai_copilot.get_trade_recommendation", _should_not_call)
    monkeypatch.setattr("app.core.ai_copilot.send_telegram_message", lambda *args, **kwargs: None)

    result = create_ai_recommendation(db_session, user, alert)
    assert result is None


def test_build_draft_uses_pref_values(db_session, monkeypatch):
    user = User(
        user_id=uuid4(),
        name="Trader",
        telegram_chat_id="123",
        overrides_json={
            "risk_budget_usd_per_day": 100.0,
            "max_usd_per_trade": 50.0,
            "max_liquidity_fraction": 0.01,
        },
    )
    alert = _make_alert(best_ask=0.5, liquidity=10000.0)
    db_session.add_all([user, alert])
    db_session.commit()

    monkeypatch.setattr("app.core.ai_copilot.redis_conn", FakeRedis())
    effective = get_effective_user_settings(user)
    draft = _build_draft(effective, alert, user, "bitcoin-2026")
    assert draft.notional_usd == 50.0
    assert draft.size == 100.0
