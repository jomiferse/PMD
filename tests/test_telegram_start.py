from datetime import datetime, timezone
from uuid import uuid4

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.core.ai_copilot import (
    START_MESSAGE_CONFLICT,
    START_MESSAGE_IDEMPOTENT,
    START_MESSAGE_INVALID,
    START_MESSAGE_PENDING,
    START_MESSAGE_SUCCESS,
    handle_telegram_update,
)
from app.db import Base
from app.models import PendingTelegramChat, User


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


def _install_fake_sender(monkeypatch):
    sent = {}

    def _fake_send(chat_id, text, reply_markup=None):
        sent["chat_id"] = chat_id
        sent["text"] = text
        return {"ok": True}

    monkeypatch.setattr("app.core.ai_copilot.send_telegram_message", _fake_send)
    return sent


def _payload(text: str, chat_id: int = 123) -> dict:
    return {"message": {"chat": {"id": chat_id}, "text": text}}


def test_start_no_payload_creates_pending(db_session, monkeypatch):
    sent = _install_fake_sender(monkeypatch)

    result = handle_telegram_update(db_session, _payload("/start"))

    pending = db_session.query(PendingTelegramChat).one()
    assert pending.telegram_chat_id == 123
    assert pending.first_seen_at is not None
    assert pending.last_seen_at is not None
    assert result["reason"] == "pending"
    assert sent["text"] == START_MESSAGE_PENDING


def test_start_invalid_payload_creates_pending(db_session, monkeypatch):
    sent = _install_fake_sender(monkeypatch)

    result = handle_telegram_update(db_session, _payload("/start bad_payload"))

    pending = db_session.query(PendingTelegramChat).one()
    assert pending.telegram_chat_id == 123
    assert result["reason"] == "invalid_payload"
    assert sent["text"] == START_MESSAGE_INVALID


def test_start_valid_payload_links_user(db_session, monkeypatch):
    sent = _install_fake_sender(monkeypatch)

    user = User(user_id=uuid4(), name="Trader", telegram_chat_id=None, created_at=datetime.now(timezone.utc))
    db_session.add(user)
    db_session.add(
        PendingTelegramChat(
            telegram_chat_id=123,
            first_seen_at=datetime.now(timezone.utc),
            last_seen_at=datetime.now(timezone.utc),
            status="pending",
        )
    )
    db_session.commit()

    payload = _payload(f"/start pmd_{user.user_id}")
    result = handle_telegram_update(db_session, payload)

    refreshed = db_session.query(User).filter(User.user_id == user.user_id).one()
    assert refreshed.telegram_chat_id == 123
    assert db_session.query(PendingTelegramChat).count() == 0
    assert result["reason"] == "link_success"
    assert sent["text"] == START_MESSAGE_SUCCESS


def test_start_valid_payload_idempotent(db_session, monkeypatch):
    sent = _install_fake_sender(monkeypatch)

    user = User(user_id=uuid4(), name="Trader", telegram_chat_id=123, created_at=datetime.now(timezone.utc))
    db_session.add(user)
    db_session.commit()

    payload = _payload(f"/start pmd_{user.user_id}")
    result = handle_telegram_update(db_session, payload)

    refreshed = db_session.query(User).filter(User.user_id == user.user_id).one()
    assert refreshed.telegram_chat_id == 123
    assert result["reason"] == "link_exists"
    assert sent["text"] == START_MESSAGE_IDEMPOTENT


def test_start_valid_payload_conflict_user_already_linked(db_session, monkeypatch):
    sent = _install_fake_sender(monkeypatch)

    user = User(user_id=uuid4(), name="Trader", telegram_chat_id=999, created_at=datetime.now(timezone.utc))
    db_session.add(user)
    db_session.commit()

    payload = _payload(f"/start pmd_{user.user_id}")
    result = handle_telegram_update(db_session, payload)

    refreshed = db_session.query(User).filter(User.user_id == user.user_id).one()
    assert refreshed.telegram_chat_id == 999
    assert result["reason"] == "link_conflict"
    assert sent["text"] == START_MESSAGE_CONFLICT


def test_start_valid_payload_conflict_chat_linked_elsewhere(db_session, monkeypatch):
    sent = _install_fake_sender(monkeypatch)

    linked = User(user_id=uuid4(), name="Linked", telegram_chat_id=123, created_at=datetime.now(timezone.utc))
    user = User(user_id=uuid4(), name="Other", telegram_chat_id=None, created_at=datetime.now(timezone.utc))
    db_session.add_all([linked, user])
    db_session.commit()

    payload = _payload(f"/start pmd_{user.user_id}")
    result = handle_telegram_update(db_session, payload)

    refreshed = db_session.query(User).filter(User.user_id == user.user_id).one()
    assert refreshed.telegram_chat_id is None
    assert result["reason"] == "link_conflict"
    assert sent["text"] == START_MESSAGE_CONFLICT
