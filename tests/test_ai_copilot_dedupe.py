from datetime import datetime, timezone
from uuid import uuid4

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.alerts.theme_key import extract_theme
from app.core import defaults
from app.core.ai_copilot import create_ai_recommendation
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
        self.expirations = {}
        self.now = 0

    def advance(self, seconds):
        self.now += seconds
        self._purge_expired()

    def _purge_expired(self):
        expired = [key for key, exp in self.expirations.items() if exp is not None and exp <= self.now]
        for key in expired:
            self.store.pop(key, None)
            self.expirations.pop(key, None)

    def get(self, key):
        self._purge_expired()
        return self.store.get(key)

    def set(self, key, value, nx=False, ex=None):
        self._purge_expired()
        if nx and key in self.store:
            return None
        self.store[key] = value
        if ex is not None:
            self.expirations[key] = self.now + ex
        else:
            self.expirations[key] = None
        return True

    def ttl(self, key):
        self._purge_expired()
        if key not in self.store:
            return -2
        expire_at = self.expirations.get(key)
        if expire_at is None:
            return -1
        return max(int(expire_at - self.now), 0)

    def expire(self, key, ttl):
        self._purge_expired()
        if key not in self.store:
            return False
        self.expirations[key] = self.now + ttl
        return True

    def delete(self, key):
        self.store.pop(key, None)
        self.expirations.pop(key, None)
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
    fake_redis.store[f"copilot:theme:{user.user_id}:{theme_key}"] = "1"
    monkeypatch.setattr("app.core.ai_copilot.redis_conn", fake_redis)

    def _should_not_call(_payload):
        raise AssertionError("LLM should not be called when copilot is deduped")

    monkeypatch.setattr("app.core.ai_copilot.get_trade_recommendation", _should_not_call)
    monkeypatch.setattr("app.core.ai_copilot.send_telegram_message", lambda *args, **kwargs: None)

    result = create_ai_recommendation(db_session, user, alert)
    assert result is None


class _StubClassification:
    signal_type = "REPRICING"
    confidence = "HIGH"
    suggested_action = "FOLLOW"


def _llm_response():
    return {
        "recommendation": "BUY",
        "confidence": "HIGH",
        "rationale": "go",
        "risks": "risk",
    }


def test_copilot_theme_dedupe_ttl_respected(db_session, monkeypatch):
    user = User(
        user_id=uuid4(),
        name="Trader",
        telegram_chat_id="123",
        copilot_enabled=True,
        overrides_json={
            "copilot_theme_ttl_minutes": 1,
        },
    )
    alert_one = _make_alert(
        title="Will the price of Bitcoin be above $50 on Jan 5 2026?",
        market_id="market-1",
    )
    alert_two = _make_alert(
        title="Will the price of Bitcoin be above $50 on Jan 5 2026?",
        market_id="market-2",
    )
    alert_three = _make_alert(
        title="Will the price of Bitcoin be above $50 on Jan 5 2026?",
        market_id="market-3",
    )
    db_session.add_all([user, alert_one, alert_two, alert_three])
    db_session.commit()

    fake_redis = FakeRedis()
    llm_calls = {"count": 0}
    monkeypatch.setattr("app.core.ai_copilot.redis_conn", fake_redis)
    monkeypatch.setattr("app.core.ai_copilot.classify_alert_with_snapshots", lambda *args, **kwargs: _StubClassification())

    def _llm(_payload):
        llm_calls["count"] += 1
        return _llm_response()

    monkeypatch.setattr("app.core.ai_copilot.get_trade_recommendation", _llm)
    monkeypatch.setattr(
        "app.core.ai_copilot.send_telegram_message",
        lambda *args, **kwargs: {"ok": True, "result": {"message_id": 1}},
    )

    first = create_ai_recommendation(db_session, user, alert_one)
    theme_key = extract_theme(alert_one.title, category=alert_one.category, slug=alert_one.market_id).theme_key
    dedupe_key = f"copilot:theme:{user.user_id}:{theme_key}"
    assert first is not None
    assert fake_redis.ttl(dedupe_key) == 60

    second = create_ai_recommendation(db_session, user, alert_two)
    assert second is None
    assert llm_calls["count"] == 1

    fake_redis.advance(61)
    third = create_ai_recommendation(db_session, user, alert_three)
    assert third is not None
    assert llm_calls["count"] == 2


def test_copilot_dedupe_shortens_on_send_failure(db_session, monkeypatch):
    user = User(
        user_id=uuid4(),
        name="Trader",
        telegram_chat_id="123",
        copilot_enabled=True,
        overrides_json={
            "copilot_theme_ttl_minutes": 1,
        },
    )
    alert = _make_alert(
        title="Will the price of Bitcoin be above $50 on Jan 5 2026?",
        market_id="market-1",
    )
    db_session.add_all([user, alert])
    db_session.commit()

    fake_redis = FakeRedis()
    monkeypatch.setattr("app.core.ai_copilot.redis_conn", fake_redis)
    monkeypatch.setattr("app.core.ai_copilot.classify_alert_with_snapshots", lambda *args, **kwargs: _StubClassification())
    monkeypatch.setattr("app.core.ai_copilot.get_trade_recommendation", lambda *_args, **_kwargs: _llm_response())
    monkeypatch.setattr("app.core.ai_copilot.send_telegram_message", lambda *args, **kwargs: None)

    rec = create_ai_recommendation(db_session, user, alert)
    theme_key = extract_theme(alert.title, category=alert.category, slug=alert.market_id).theme_key
    dedupe_key = f"copilot:theme:{user.user_id}:{theme_key}"
    assert rec is not None
    assert fake_redis.ttl(dedupe_key) == defaults.COPILOT_DEDUPE_FAILURE_TTL_SECONDS
