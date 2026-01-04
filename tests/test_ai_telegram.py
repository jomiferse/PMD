from datetime import datetime, timezone
from uuid import uuid4

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.core.ai_copilot import _format_ai_message, handle_telegram_callback
from app.db import Base
from app.models import AiRecommendation, AiRecommendationEvent, Alert, User, UserAlertPreference


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


@pytest.fixture(autouse=True)
def fake_redis(monkeypatch):


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


def test_ai_message_formatting_contains_sections():
    alert = _make_alert()
    rec = AiRecommendation(
        user_id=uuid4(),
        alert_id=1,
        recommendation="BUY",
        confidence="HIGH",
        rationale="Strong repricing; good liquidity",
        risks="Event risk",
        draft_side="YES",
        draft_price=0.5,
        draft_size=100.0,
        draft_notional_usd=50.0,
        status="PROPOSED",
        created_at=datetime.now(timezone.utc),
    )
    text, markup = _format_ai_message(alert, rec)
    assert "AI Copilot" in text
    assert "Rationale" in text
    assert "Risks" in text
    assert "Draft order" in text
    assert "token_id" in text
    assert markup["inline_keyboard"]


def test_callback_confirm_updates_status(db_session, monkeypatch):
    user = User(user_id=uuid4(), name="Trader", telegram_chat_id="123")
    alert = _make_alert()
    pref = UserAlertPreference(
        user_id=user.user_id,
        risk_budget_usd_per_day=100.0,
        max_usd_per_trade=50.0,
        max_liquidity_fraction=0.01,
    )
    db_session.add_all([user, alert])
    db_session.add(pref)
    db_session.commit()

    rec = AiRecommendation(
        user_id=user.user_id,
        alert_id=alert.id,
        recommendation="BUY",
        confidence="HIGH",
        rationale="test",
        risks="test",
        draft_side="YES",
        draft_price=0.5,
        draft_size=100.0,
        draft_notional_usd=50.0,
        status="PROPOSED",
        created_at=datetime.now(timezone.utc),
    )
    db_session.add(rec)
    db_session.commit()

    monkeypatch.setattr("app.core.ai_copilot.send_telegram_message", lambda *args, **kwargs: None)
    monkeypatch.setattr("app.core.ai_copilot.answer_callback_query", lambda *args, **kwargs: None)
    monkeypatch.setattr("app.core.ai_copilot._register_risk_spend", lambda *args, **kwargs: None)

    payload = {
        "callback_query": {
            "id": "1",
            "data": f"confirm:{rec.id}",
            "message": {"chat": {"id": "123"}, "message_id": "999"},
        }
    }
    result = handle_telegram_callback(db_session, payload)
    assert result["ok"] is True

    refreshed = db_session.query(AiRecommendation).filter(AiRecommendation.id == rec.id).one()
    assert refreshed.status == "CONFIRMED"


def test_confirm_payload_contains_draft(db_session, monkeypatch):
    user = User(user_id=uuid4(), name="Trader", telegram_chat_id="123")
    alert = _make_alert()
    pref = UserAlertPreference(
        user_id=user.user_id,
        risk_budget_usd_per_day=100.0,
        max_usd_per_trade=50.0,
        max_liquidity_fraction=0.01,
    )
    db_session.add_all([user, alert, pref])
    db_session.commit()

    rec = AiRecommendation(
        user_id=user.user_id,
        alert_id=alert.id,
        recommendation="BUY",
        confidence="HIGH",
        rationale="test",
        risks="test",
        draft_side="YES",
        draft_price=0.5,
        draft_size=100.0,
        draft_notional_usd=50.0,
        status="PROPOSED",
        created_at=datetime.now(timezone.utc),
    )
    db_session.add(rec)
    db_session.commit()

    sent = {}

    def _fake_send(chat_id, text, reply_markup=None):
        sent["text"] = text
        return None

    monkeypatch.setattr("app.core.ai_copilot.send_telegram_message", _fake_send)
    monkeypatch.setattr("app.core.ai_copilot.answer_callback_query", lambda *args, **kwargs: None)
    monkeypatch.setattr("app.core.ai_copilot._register_risk_spend", lambda *args, **kwargs: None)

    payload = {
        "callback_query": {
            "id": "2",
            "data": f"confirm:{rec.id}",
            "message": {"chat": {"id": "123"}, "message_id": "1000"},
        }
    }
    handle_telegram_callback(db_session, payload)
    assert "Draft order payload" in sent["text"]
    assert "side" in sent["text"]
    assert "price" in sent["text"]
    assert "size" in sent["text"]
    assert "notional_usd" in sent["text"]
    assert "null" not in sent["text"].lower()


def test_confirm_payload_missing_inputs_note(db_session, monkeypatch):
    user = User(user_id=uuid4(), name="Trader", telegram_chat_id="123")
    alert = _make_alert(liquidity=0.0, market_p_yes=0.0, new_price=0.0)
    pref = UserAlertPreference(
        user_id=user.user_id,
        risk_budget_usd_per_day=0.0,
        max_usd_per_trade=0.0,
        max_liquidity_fraction=0.0,
    )
    db_session.add_all([user, alert, pref])
    db_session.commit()

    rec = AiRecommendation(
        user_id=user.user_id,
        alert_id=alert.id,
        recommendation="BUY",
        confidence="HIGH",
        rationale="test",
        risks="test",
        draft_side=None,
        draft_price=None,
        draft_size=None,
        draft_notional_usd=None,
        status="PROPOSED",
        created_at=datetime.now(timezone.utc),
    )
    db_session.add(rec)
    db_session.commit()

    sent = {}

    def _fake_send(chat_id, text, reply_markup=None):
        sent["text"] = text
        return None

    monkeypatch.setattr("app.core.ai_copilot.send_telegram_message", _fake_send)
    monkeypatch.setattr("app.core.ai_copilot.answer_callback_query", lambda *args, **kwargs: None)
    monkeypatch.setattr("app.core.ai_copilot._register_risk_spend", lambda *args, **kwargs: None)

    payload = {
        "callback_query": {
            "id": "3",
            "data": f"confirm:{rec.id}",
            "message": {"chat": {"id": "123"}, "message_id": "1001"},
        }
    }
    handle_telegram_callback(db_session, payload)
    assert "Draft order unavailable" in sent["text"]
    assert "Missing inputs" in sent["text"]
    assert "null" not in sent["text"].lower()


def test_duplicate_callback_id_is_idempotent(db_session, monkeypatch):
    user = User(user_id=uuid4(), name="Trader", telegram_chat_id="123")
    alert = _make_alert()
    pref = UserAlertPreference(
        user_id=user.user_id,
        risk_budget_usd_per_day=100.0,
        max_usd_per_trade=50.0,
        max_liquidity_fraction=0.01,
    )
    db_session.add_all([user, alert, pref])
    db_session.commit()

    rec = AiRecommendation(
        user_id=user.user_id,
        alert_id=alert.id,
        recommendation="BUY",
        confidence="HIGH",
        rationale="test",
        risks="test",
        draft_side="YES",
        draft_price=0.5,
        draft_size=100.0,
        draft_notional_usd=50.0,
        status="PROPOSED",
        created_at=datetime.now(timezone.utc),
    )
    db_session.add(rec)
    db_session.commit()

    class FakeRedis:
        def __init__(self):
            self.store = {}

        def set(self, key, value, nx=False, ex=None):
            if nx and key in self.store:
                return None
            self.store[key] = value
            return True

        def get(self, key):
            return None

        def incrbyfloat(self, key, amount):
            return None

        def expire(self, key, ttl):
            return None

    monkeypatch.setattr("app.core.ai_copilot.redis_conn", FakeRedis())
    monkeypatch.setattr("app.core.ai_copilot.edit_message_reply_markup", lambda *args, **kwargs: None)
    monkeypatch.setattr("app.core.ai_copilot.send_telegram_message", lambda *args, **kwargs: None)
    monkeypatch.setattr("app.core.ai_copilot.answer_callback_query", lambda *args, **kwargs: None)
    monkeypatch.setattr("app.core.ai_copilot._register_risk_spend", lambda *args, **kwargs: None)

    payload = {
        "callback_query": {
            "id": "dup-1",
            "data": f"confirm:{rec.id}",
            "message": {"chat": {"id": "123"}, "message_id": "1002"},
        }
    }

    first = handle_telegram_callback(db_session, payload)
    second = handle_telegram_callback(db_session, payload)
    assert first["ok"] is True
    assert second["reason"] == "duplicate_callback"


def test_status_transitions_are_idempotent(db_session, monkeypatch):
    user = User(user_id=uuid4(), name="Trader", telegram_chat_id="123")
    alert = _make_alert()
    pref = UserAlertPreference(
        user_id=user.user_id,
        risk_budget_usd_per_day=100.0,
        max_usd_per_trade=50.0,
        max_liquidity_fraction=0.01,
    )
    db_session.add_all([user, alert, pref])
    db_session.commit()

    rec = AiRecommendation(
        user_id=user.user_id,
        alert_id=alert.id,
        recommendation="BUY",
        confidence="HIGH",
        rationale="test",
        risks="test",
        draft_side="YES",
        draft_price=0.5,
        draft_size=100.0,
        draft_notional_usd=50.0,
        status="PROPOSED",
        created_at=datetime.now(timezone.utc),
    )
    db_session.add(rec)
    db_session.commit()

    monkeypatch.setattr("app.core.ai_copilot.send_telegram_message", lambda *args, **kwargs: None)
    monkeypatch.setattr("app.core.ai_copilot.answer_callback_query", lambda *args, **kwargs: None)
    monkeypatch.setattr("app.core.ai_copilot._register_risk_spend", lambda *args, **kwargs: None)

    payload_confirm = {
        "callback_query": {
            "id": "confirm-1",
            "data": f"confirm:{rec.id}",
            "message": {"chat": {"id": "123"}, "message_id": "1003"},
        }
    }
    handle_telegram_callback(db_session, payload_confirm)

    payload_confirm_2 = {
        "callback_query": {
            "id": "confirm-2",
            "data": f"confirm:{rec.id}",
            "message": {"chat": {"id": "123"}, "message_id": "1003"},
        }
    }
    second = handle_telegram_callback(db_session, payload_confirm_2)
    assert second["message"] == "Already confirmed."

    events = db_session.query(AiRecommendationEvent).filter(AiRecommendationEvent.recommendation_id == rec.id).all()
    assert len(events) == 1
