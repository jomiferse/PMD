import hashlib
import hmac
import json
import time
from datetime import datetime, timezone
from uuid import uuid4

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.db import Base, get_db
from app.main import app
from app.models import Plan, Subscription, User
from app.services.stripe_service import _create_checkout_session, _sync_subscription_from_stripe
from app.settings import settings


@pytest.fixture()
def db_session():
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
        future=True,
    )
    Base.metadata.create_all(bind=engine)
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()


@pytest.fixture()
def client(db_session):
    def _get_db():
        yield db_session

    app.dependency_overrides[get_db] = _get_db
    with TestClient(app) as test_client:
        yield test_client
    app.dependency_overrides.clear()


def _stripe_signature(payload: bytes, secret: str, timestamp: int) -> str:
    signed_payload = f"{timestamp}.{payload.decode('utf-8')}".encode("utf-8")
    signature = hmac.new(secret.encode("utf-8"), signed_payload, hashlib.sha256).hexdigest()
    return f"t={timestamp},v1={signature}"


def test_checkout_session_rejects_active_same_plan(db_session, monkeypatch):
    monkeypatch.setattr(settings, "STRIPE_ELITE_PRICE_ID", "price_elite")
    user = User(user_id=uuid4(), name="Test User", telegram_chat_id=None, overrides_json={})
    plan = Plan(name="elite", price_monthly=99, is_active=True)
    db_session.add_all([user, plan])
    db_session.commit()

    subscription = Subscription(user_id=user.user_id, status="active", plan_id=plan.id)
    db_session.add(subscription)
    db_session.commit()

    with pytest.raises(HTTPException) as excinfo:
        _create_checkout_session(db_session, user, None, "elite")
    assert excinfo.value.status_code == 409
    assert excinfo.value.detail == "already_subscribed"


def test_sync_subscription_sets_current_period_end(db_session, monkeypatch):
    monkeypatch.setattr(settings, "STRIPE_BASIC_PRICE_ID", "price_basic")
    user = User(user_id=uuid4(), name="Test User", telegram_chat_id=None, overrides_json={})
    plan = Plan(name="basic", price_monthly=10, is_active=True)
    db_session.add_all([user, plan])
    db_session.commit()

    period_end = int(datetime.now(tz=timezone.utc).timestamp())
    payload = {
        "id": "sub_test_1",
        "customer": "cus_test_1",
        "status": "active",
        "current_period_end": period_end,
        "items": {"data": [{"price": {"id": "price_basic"}}]},
    }

    subscription = _sync_subscription_from_stripe(db_session, payload, user.user_id)
    assert subscription.current_period_end is not None
    assert int(subscription.current_period_end.timestamp()) == period_end


def test_webhook_subscription_update_sets_current_period_end(client, db_session, monkeypatch):
    monkeypatch.setattr(settings, "STRIPE_WEBHOOK_SECRET", "whsec_test")
    monkeypatch.setattr(settings, "STRIPE_SECRET_KEY", "sk_test")
    monkeypatch.setattr(settings, "STRIPE_BASIC_PRICE_ID", "price_basic")

    user = User(user_id=uuid4(), name="Test User", telegram_chat_id=None, overrides_json={})
    plan = Plan(name="basic", price_monthly=10, is_active=True)
    db_session.add_all([user, plan])
    db_session.commit()

    period_end = int(datetime.now(tz=timezone.utc).timestamp())
    payload = {
        "id": "evt_test_123",
        "type": "customer.subscription.updated",
        "data": {
            "object": {
                "id": "sub_test_123",
                "customer": "cus_test_123",
                "status": "active",
                "current_period_end": period_end,
                "metadata": {"user_id": str(user.user_id)},
                "items": {"data": [{"price": {"id": "price_basic"}}]},
            }
        },
    }

    payload_bytes = json.dumps(payload, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
    timestamp = int(time.time())
    signature = _stripe_signature(payload_bytes, settings.STRIPE_WEBHOOK_SECRET, timestamp)

    response = client.post(
        "/webhooks/stripe",
        content=payload_bytes,
        headers={
            "stripe-signature": signature,
            "content-type": "application/json",
        },
    )
    assert response.status_code == 200
    assert response.json() == {"ok": True}

    stored = (
        db_session.query(Subscription)
        .filter(Subscription.user_id == user.user_id)
        .order_by(Subscription.created_at.desc())
        .first()
    )
    assert stored is not None
    assert stored.current_period_end is not None
