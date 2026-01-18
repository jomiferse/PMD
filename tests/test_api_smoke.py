import hashlib
import hmac
import json
import time
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.db import Base, get_db
from app.main import app
from app.models import Plan, User, UserSession
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
    previous_sessionmaker = getattr(app.state, "db_sessionmaker", None)
    app.state.db_sessionmaker = sessionmaker(
        bind=db_session.get_bind(),
        autoflush=False,
        autocommit=False,
    )
    with TestClient(app) as test_client:
        yield test_client
    app.dependency_overrides.clear()
    if previous_sessionmaker is None:
        if hasattr(app.state, "db_sessionmaker"):
            delattr(app.state, "db_sessionmaker")
    else:
        app.state.db_sessionmaker = previous_sessionmaker


def _stripe_signature(payload: bytes, secret: str, timestamp: int) -> str:
    signed_payload = f"{timestamp}.{payload.decode('utf-8')}".encode("utf-8")
    signature = hmac.new(secret.encode("utf-8"), signed_payload, hashlib.sha256).hexdigest()
    return f"t={timestamp},v1={signature}"


def test_health_ok(client):
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"ok": True}


def test_session_auth_for_dashboard_endpoints(client, db_session):
    user = User(user_id=uuid4(), name="Session User", telegram_chat_id=None, overrides_json={})
    session = UserSession(
        token="session-token",
        user_id=user.user_id,
        expires_at=datetime.now(timezone.utc) + timedelta(days=1),
    )
    db_session.add_all([user, session])
    db_session.commit()

    client.cookies.set(settings.SESSION_COOKIE_NAME, session.token)

    response = client.get("/me")
    assert response.status_code == 200

    response = client.get("/alerts/latest")
    assert response.status_code == 200

    response = client.get("/alerts/summary")
    assert response.status_code == 200

    response = client.get("/copilot/runs")
    assert response.status_code == 200


def test_checkout_session_returns_url(client, monkeypatch):
    monkeypatch.setattr(
        "app.api.routes.billing._require_session_user",
        lambda request, db: (object(), object()),
    )
    monkeypatch.setattr(
        "app.api.routes.billing._create_checkout_session",
        lambda db, user, auth, plan_id: "https://checkout.example/session",
    )

    response = client.post("/billing/checkout-session", json={"plan_id": "basic"})
    assert response.status_code == 200
    assert response.json() == {"checkout_url": "https://checkout.example/session"}


def test_stripe_webhook_rejects_missing_signature(client, monkeypatch):
    monkeypatch.setattr(settings, "STRIPE_WEBHOOK_SECRET", "whsec_test")
    monkeypatch.setattr(settings, "STRIPE_SECRET_KEY", "sk_test")

    response = client.post("/webhooks/stripe", json={})
    assert response.status_code == 400
    assert response.json()["detail"] == "missing_signature"


def test_stripe_webhook_accepts_valid_signature(client, db_session, monkeypatch):
    monkeypatch.setattr(settings, "STRIPE_WEBHOOK_SECRET", "whsec_test")
    monkeypatch.setattr(settings, "STRIPE_SECRET_KEY", "sk_test")
    monkeypatch.setattr(settings, "STRIPE_BASIC_PRICE_ID", "price_basic")

    user = User(user_id=uuid4(), name="Test User", telegram_chat_id=None, overrides_json={})
    plan = Plan(name="basic", price_monthly=0, is_active=True)
    db_session.add_all([user, plan])
    db_session.commit()

    payload = {
        "id": "evt_test_1",
        "type": "customer.subscription.created",
        "data": {
            "object": {
                "id": "sub_test_1",
                "customer": "cus_test_1",
                "status": "active",
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


def test_stripe_webhook_rejects_invalid_signature(client, monkeypatch):
    monkeypatch.setattr(settings, "STRIPE_WEBHOOK_SECRET", "whsec_test")
    monkeypatch.setattr(settings, "STRIPE_SECRET_KEY", "sk_test")

    payload_bytes = b'{"id":"evt_test_2","type":"customer.subscription.updated","data":{"object":{}}}'
    timestamp = int(time.time())
    signature = _stripe_signature(payload_bytes, "whsec_wrong", timestamp)

    response = client.post(
        "/webhooks/stripe",
        content=payload_bytes,
        headers={
            "stripe-signature": signature,
            "content-type": "application/json",
        },
    )
    assert response.status_code == 400
    assert response.json()["detail"] == "invalid_signature"
