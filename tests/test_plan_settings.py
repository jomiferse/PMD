from datetime import datetime, timezone
from uuid import uuid4

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.core.alert_classification import AlertClassification, AlertClass
from app.core.alerts import _enqueue_ai_recommendations, _resolve_user_preferences
from app.core.user_settings import get_effective_user_settings
from app.db import Base
from app.models import Alert, Plan, User, UserAlertPreference


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

    def get(self, key):
        return self.store.get(key)

    def set(self, key, value, nx=False, ex=None):
        if nx and key in self.store:
            return None
        self.store[key] = value
        if ex is not None:
            self.expirations[key] = ex
        return True

    def ttl(self, key):
        if key not in self.store:
            return -2
        return self.expirations.get(key, -1)

    def expire(self, key, ttl):
        if key not in self.store:
            return False
        self.expirations[key] = ttl
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


def test_users_with_different_plans_get_different_caps(db_session):
    starter = Plan(name="starter", max_copilot_per_day=1, max_copilot_per_digest=1)
    pro = Plan(name="pro", max_copilot_per_day=3, max_copilot_per_digest=2)
    db_session.add_all([starter, pro])
    db_session.commit()

    user_a = User(user_id=uuid4(), name="Starter", plan_id=starter.id, copilot_enabled=True)
    user_b = User(user_id=uuid4(), name="Pro", plan_id=pro.id, copilot_enabled=True)
    db_session.add_all([user_a, user_b])
    db_session.commit()

    config_a = _resolve_user_preferences(user_a, None, db_session)
    config_b = _resolve_user_preferences(user_b, None, db_session)

    assert config_a.max_copilot_per_day == 1
    assert config_b.max_copilot_per_day == 3


def test_cap_reached_uses_plan_limits(db_session, monkeypatch):
    plan = Plan(name="cap-plan", max_copilot_per_day=1, max_copilot_per_digest=1)
    db_session.add(plan)
    db_session.commit()

    user = User(user_id=uuid4(), name="CapUser", plan_id=plan.id, copilot_enabled=True)
    alert = _make_alert()
    db_session.add_all([user, alert])
    db_session.commit()

    fake_redis = FakeRedis()
    date_key = datetime.now(timezone.utc).date().isoformat()
    fake_redis.store[f"copilot:count:{user.user_id}:{date_key}"] = "1"
    monkeypatch.setattr("app.core.alerts.redis_conn", fake_redis)
    monkeypatch.setattr(
        "app.core.alerts.classify_alert_with_snapshots",
        lambda *_args, **_kwargs: AlertClassification("REPRICING", "HIGH", "FOLLOW"),
    )
    monkeypatch.setattr(
        "app.core.alerts._count_snapshot_points_bulk",
        lambda *_args, **_kwargs: {alert.market_id: 3},
    )
    monkeypatch.setattr("app.core.alerts._label_mapping_unknown", lambda *_args, **_kwargs: False)

    enqueued = []
    monkeypatch.setattr("app.core.alerts.queue.enqueue", lambda *args, **kwargs: enqueued.append(args))

    config = _resolve_user_preferences(user, None, db_session)
    _enqueue_ai_recommendations(
        db_session,
        config,
        [alert],
        classifier=lambda *_args, **_kwargs: AlertClassification(
            "REPRICING", "HIGH", "FOLLOW", alert_class=AlertClass.ACTIONABLE_STANDARD
        ),
    )

    assert not enqueued
    stored = fake_redis.store[f"copilot:last_eval:{user.user_id}"]
    assert "\"cap_reached\": \"daily\"" in stored


def test_overrides_json_takes_precedence(db_session):
    plan = Plan(name="base", max_copilot_per_day=5, max_copilot_per_digest=2)
    db_session.add(plan)
    db_session.commit()

    user = User(
        user_id=uuid4(),
        name="OverrideUser",
        plan_id=plan.id,
        overrides_json={"max_copilot_per_day": 2},
    )
    db_session.add(user)
    db_session.commit()

    effective = get_effective_user_settings(user)
    assert effective.max_copilot_per_day == 2


def test_effective_settings_merge_order(db_session):
    plan = Plan(name="tier", max_themes_per_digest=3)
    db_session.add(plan)
    db_session.commit()

    user = User(
        user_id=uuid4(),
        name="MergeUser",
        plan_id=plan.id,
        overrides_json={"max_themes_per_digest": 1},
    )
    pref = UserAlertPreference(user_id=user.user_id, max_themes_per_digest=2)
    db_session.add_all([user, pref])
    db_session.commit()

    effective = get_effective_user_settings(user, pref=pref)
    assert effective.max_themes_per_digest == 1
