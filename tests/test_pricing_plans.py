from datetime import datetime, timezone
from uuid import uuid4

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.core.alert_classification import AlertClassification
from app.core.alerts import _enqueue_ai_recommendations, _resolve_user_preferences
from app.core.plans import get_plan_seeds
from app.core.user_settings import get_effective_user_settings
from app.db import Base
from app.models import Alert, Plan, User


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


def _seed_plan(db_session, name: str) -> Plan:
    seed = next(plan for plan in get_plan_seeds() if plan.name == name)
    data = seed.as_dict()
    data["created_at"] = datetime.now(timezone.utc)
    plan = Plan(**data)
    db_session.add(plan)
    db_session.commit()
    db_session.refresh(plan)
    return plan


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


def test_basic_plan_disables_copilot(db_session, monkeypatch):
    basic_plan = _seed_plan(db_session, "basic")
    user = User(user_id=uuid4(), name="BasicUser", plan_id=basic_plan.id, copilot_enabled=True)
    alert = _make_alert()
    db_session.add_all([user, alert])
    db_session.commit()

    config = _resolve_user_preferences(user, None)
    assert config.ai_copilot_enabled is False

    enqueued = []
    monkeypatch.setattr("app.core.alerts.redis_conn", FakeRedis())
    monkeypatch.setattr(
        "app.core.alerts.classify_alert_with_snapshots",
        lambda *_args, **_kwargs: AlertClassification("REPRICING", "HIGH", "FOLLOW"),
    )
    monkeypatch.setattr("app.core.alerts.queue.enqueue", lambda *args, **kwargs: enqueued.append(args))

    _enqueue_ai_recommendations(db_session, config, [alert])
    assert not enqueued


def test_pro_plan_daily_cap_respected(db_session, monkeypatch):
    pro_plan = _seed_plan(db_session, "pro")
    user_id = uuid4()
    user = User(user_id=user_id, name="ProUser", plan_id=pro_plan.id, copilot_enabled=True)
    alert = _make_alert()
    db_session.add_all([user, alert])
    db_session.commit()

    config = _resolve_user_preferences(user, None)
    fake_redis = FakeRedis()
    date_key = datetime.now(timezone.utc).date().isoformat()
    fake_redis.store[f"copilot:count:{user_id}:{date_key}"] = "2"
    monkeypatch.setattr("app.core.alerts.redis_conn", fake_redis)
    monkeypatch.setattr(
        "app.core.alerts.classify_alert_with_snapshots",
        lambda *_args, **_kwargs: AlertClassification("REPRICING", "HIGH", "FOLLOW"),
    )
    enqueued = []
    monkeypatch.setattr("app.core.alerts.queue.enqueue", lambda *args, **kwargs: enqueued.append(args))

    _enqueue_ai_recommendations(db_session, config, [alert])
    assert len(enqueued) == 1

    enqueued.clear()
    fake_redis.store[f"copilot:count:{user_id}:{date_key}"] = "3"
    _enqueue_ai_recommendations(db_session, config, [alert])
    assert not enqueued


def test_elite_plan_increases_caps(db_session):
    pro_plan = _seed_plan(db_session, "pro")
    elite_plan = _seed_plan(db_session, "elite")

    pro_user = User(user_id=uuid4(), name="ProUser", plan_id=pro_plan.id, copilot_enabled=True)
    elite_user = User(user_id=uuid4(), name="EliteUser", plan_id=elite_plan.id, copilot_enabled=True)
    db_session.add_all([pro_user, elite_user])
    db_session.commit()

    pro_effective = get_effective_user_settings(pro_user)
    elite_effective = get_effective_user_settings(elite_user)

    assert elite_effective.max_themes_per_digest > pro_effective.max_themes_per_digest
    assert elite_effective.max_copilot_per_day > pro_effective.max_copilot_per_day


def test_cap_reached_message_includes_plan_and_limits(db_session, monkeypatch):
    pro_plan = _seed_plan(db_session, "pro")
    user_id = uuid4()
    user = User(user_id=user_id, name="ProUser", plan_id=pro_plan.id, copilot_enabled=True)
    alert = _make_alert()
    db_session.add_all([user, alert])
    db_session.commit()

    config = _resolve_user_preferences(user, None)
    fake_redis = FakeRedis()
    date_key = datetime.now(timezone.utc).date().isoformat()
    fake_redis.store[f"copilot:count:{user_id}:{date_key}"] = str(config.max_copilot_per_day)
    monkeypatch.setattr("app.core.alerts.redis_conn", fake_redis)
    monkeypatch.setattr(
        "app.core.alerts.classify_alert_with_snapshots",
        lambda *_args, **_kwargs: AlertClassification("REPRICING", "HIGH", "FOLLOW"),
    )
    monkeypatch.setattr("app.core.alerts.queue.enqueue", lambda *args, **kwargs: None)

    _enqueue_ai_recommendations(db_session, config, [alert])
    payload = fake_redis.store[f"copilot:last_eval:{user_id}"]
    assert "CAP_REACHED" in payload
    assert f"{config.max_copilot_per_day}/{config.max_copilot_per_day}" in payload
