from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.db import Base, get_db
from app.main import app
from app.models import Plan, User, UserAuth, UserSession, UserAlertPreference
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


def _create_plan(db_session, name="basic"):
    plan = Plan(
        name=name,
        price_monthly=10.0,
        is_active=True,
        copilot_enabled=False,
        max_alerts_per_digest=3,
        max_themes_per_digest=3,
        max_markets_per_theme=3,
        digest_window_minutes=60,
        min_liquidity=5000.0,
        min_volume_24h=5000.0,
        min_abs_move=0.01,
        p_min=0.15,
        p_max=0.85,
        allowed_strengths="STRONG",
        fast_signals_enabled=False,
        fast_window_minutes=15,
        fast_max_themes_per_digest=2,
        fast_max_markets_per_theme=2,
    )
    db_session.add(plan)
    db_session.commit()
    return plan


def _create_session(db_session, plan_id=None):
    user = User(user_id=uuid4(), name="Test User", telegram_chat_id=None, overrides_json={}, plan_id=plan_id)
    auth = UserAuth(user_id=user.user_id, email="test@example.com", password_hash="hash")
    token = "test_session_token"
    expires_at = datetime.now(timezone.utc) + timedelta(days=1)
    session = UserSession(token=token, user_id=user.user_id, expires_at=expires_at)
    db_session.add_all([user, auth, session])
    db_session.commit()
    return user, token


def test_settings_me_returns_payload(client, db_session):
    user, token = _create_session(db_session)
    client.cookies.set(settings.SESSION_COOKIE_NAME, token)

    response = client.get("/settings/me")
    assert response.status_code == 200
    payload = response.json()
    assert payload["user_id"] == str(user.user_id)
    assert "preferences" in payload
    assert "effective" in payload
    assert "baseline" in payload


def test_settings_update_persists_preferences(client, db_session):
    user, token = _create_session(db_session)
    client.cookies.set(settings.SESSION_COOKIE_NAME, token)

    response = client.patch("/settings/me", json={"min_liquidity": 5000, "p_min": 0.2, "p_max": 0.8})
    assert response.status_code == 200
    payload = response.json()
    assert payload["preferences"]["min_liquidity"] == 5000
    assert payload["preferences"]["p_min"] == 0.2
    assert payload["preferences"]["p_max"] == 0.8

    stored = db_session.query(UserAlertPreference).filter(UserAlertPreference.user_id == user.user_id).one_or_none()
    assert stored is not None
    assert stored.min_liquidity == 5000
    assert stored.p_min == 0.2
    assert stored.p_max == 0.8


def test_entitlements_me_returns_limits(client, db_session):
    plan = _create_plan(db_session)
    user, token = _create_session(db_session, plan_id=plan.id)
    client.cookies.set(settings.SESSION_COOKIE_NAME, token)

    response = client.get("/entitlements/me")
    assert response.status_code == 200
    payload = response.json()
    assert payload["plan"] == plan.name
    assert payload["limits"]["max_alerts_per_digest"]["max"] == plan.max_alerts_per_digest
    assert payload["limits"]["alert_strengths"]["allowed_values"] == ["STRONG"]


def test_settings_update_enforces_plan_caps(client, db_session):
    plan = _create_plan(db_session)
    user, token = _create_session(db_session, plan_id=plan.id)
    client.cookies.set(settings.SESSION_COOKIE_NAME, token)

    response = client.patch("/settings/me", json={"max_alerts_per_digest": 10})
    assert response.status_code == 422
    payload = response.json()
    assert "errors" in payload["detail"]
    assert "max_alerts_per_digest" in payload["detail"]["errors"]
