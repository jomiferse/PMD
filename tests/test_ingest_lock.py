import asyncio
from datetime import datetime, timezone

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db import Base
from app.jobs import tasks


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

    def delete(self, key):
        self.store.pop(key, None)


def test_ingest_lock_skips_run(db_session, monkeypatch):
    fake_redis = FakeRedis()
    fake_redis.store[tasks.INGEST_LOCK_KEY] = "locked"
    monkeypatch.setattr(tasks, "redis_conn", fake_redis)

    called = {"client": False}

    class _FakeClient:
        def __init__(self):
            called["client"] = True

        async def fetch_markets_paginated(self):
            return []

    monkeypatch.setattr(tasks, "PolymarketClient", _FakeClient)

    result = asyncio.run(tasks.run_ingest_and_alert(db_session))
    assert result["reason"] == "ingest_locked"
    assert called["client"] is False
