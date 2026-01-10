from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.db import Base, get_db
from app.main import app
from app.models import User, UserAuth, UserSession, UserAlertPreference
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


def _create_session(db_session):
    user = User(user_id=uuid4(), name="Test User", telegram_chat_id=None, overrides_json={})
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
