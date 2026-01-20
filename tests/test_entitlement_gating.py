import asyncio
from datetime import datetime, timezone, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.core.alerts import send_user_digests
from app.core.ai_copilot import _send_recommendation_message
from app.db import Base
from app.models import Alert, AlertDelivery, AiRecommendation, Plan, Subscription, User
from app.settings import settings


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


def _seed_active_subscription(db_session, user):
    plan = Plan(name="pro")
    db_session.add(plan)
    db_session.flush()
    user.plan_id = plan.id
    subscription = Subscription(
        user_id=user.user_id,
        plan_id=plan.id,
        status="active",
        current_period_end=datetime.now(timezone.utc) + timedelta(days=1),
    )
    db_session.add(subscription)
    db_session.commit()


def test_scheduler_skips_unsubscribed_user(db_session, monkeypatch):
    user = User(
        user_id=uuid4(),
        name="Trader",
        telegram_chat_id=123,
        overrides_json={},
    )
    alert = _make_alert()
    db_session.add_all([user, alert])
    db_session.commit()

    original_token = settings.TELEGRAM_BOT_TOKEN
    settings.TELEGRAM_BOT_TOKEN = "test-token"
    try:
        calls = {"count": 0}

        async def _fake_send_user_digest(db, tenant_id, config, fast_section=None):
            calls["count"] += 1
            db.add(
                AlertDelivery(
                    alert_id=alert.id,
                    user_id=config.user_id,
                    delivery_status="sent",
                )
            )
            db.commit()
            return {"sent": True}

        monkeypatch.setattr("app.core.alerts._send_user_digest", _fake_send_user_digest)
        monkeypatch.setattr("app.core.alerts._prepare_fast_digest", lambda *args, **kwargs: (None, None))
        asyncio.run(send_user_digests(db_session, "tenant-1"))
    finally:
        settings.TELEGRAM_BOT_TOKEN = original_token

    assert calls["count"] == 0
    assert db_session.query(AlertDelivery).count() == 0


def test_scheduler_allows_subscribed_user(db_session, monkeypatch):
    user = User(
        user_id=uuid4(),
        name="Trader",
        telegram_chat_id=123,
        overrides_json={},
    )
    alert = _make_alert()
    db_session.add_all([user, alert])
    db_session.commit()
    _seed_active_subscription(db_session, user)

    original_token = settings.TELEGRAM_BOT_TOKEN
    settings.TELEGRAM_BOT_TOKEN = "test-token"
    try:
        calls = {"count": 0}

        async def _fake_send_user_digest(db, tenant_id, config, fast_section=None):
            calls["count"] += 1
            db.add(
                AlertDelivery(
                    alert_id=alert.id,
                    user_id=config.user_id,
                    delivery_status="sent",
                )
            )
            db.commit()
            return {"sent": True}

        monkeypatch.setattr("app.core.alerts._send_user_digest", _fake_send_user_digest)
        monkeypatch.setattr("app.core.alerts._prepare_fast_digest", lambda *args, **kwargs: (None, None))
        asyncio.run(send_user_digests(db_session, "tenant-1"))
    finally:
        settings.TELEGRAM_BOT_TOKEN = original_token

    assert calls["count"] == 1
    assert db_session.query(AlertDelivery).count() == 1


def test_telegram_send_skips_unsubscribed_user(db_session, monkeypatch):
    user = User(
        user_id=uuid4(),
        name="Trader",
        telegram_chat_id=123,
        overrides_json={},
    )
    alert = _make_alert()
    db_session.add_all([user, alert])
    db_session.commit()

    rec = AiRecommendation(
        user_id=user.user_id,
        alert_id=alert.id,
        recommendation="BUY",
        confidence="HIGH",
        rationale="test",
        risks="test",
        status="PROPOSED",
        created_at=datetime.now(timezone.utc),
    )
    db_session.add(rec)
    db_session.commit()

    called = {"count": 0}

    def _fake_send(*args, **kwargs):
        called["count"] += 1
        return {"ok": True, "result": {"message_id": 1}}

    monkeypatch.setattr("app.core.ai_copilot.send_telegram_message", _fake_send)

    sent = _send_recommendation_message(db_session, user, alert, rec, [])
    assert sent is False
    assert called["count"] == 0
