import json
import time
from datetime import datetime, timezone
from uuid import uuid4

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.core import defaults
from app.core.alert_classification import AlertClassification
from app.core.ai_copilot import COPILOT_LAST_STATUS_KEY, _theme_key_for_alert, create_ai_recommendation
from app.alerts.theme_key import extract_theme
from app.core.alerts import CopilotSkipReason, UserDigestConfig, _enqueue_ai_recommendations
from app.db import Base
from app.models import Alert, User


class FakeRedis:
    def __init__(self):
        self.store = {}
        self.hash_store = {}
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

    def delete(self, key):
        self.store.pop(key, None)
        self.hash_store.pop(key, None)
        self.expirations.pop(key, None)
        return True

    def expire(self, key, ttl):
        if key in self.store or key in self.hash_store:
            self.expirations[key] = ttl
            return True
        return False

    def ttl(self, key):
        if key not in self.store and key not in self.hash_store:
            return -2
        return self.expirations.get(key, -1)

    def hset(self, key, mapping=None, **kwargs):
        if mapping is None:
            mapping = {}
        mapping.update(kwargs)
        bucket = self.hash_store.setdefault(key, {})
        for field, value in mapping.items():
            bucket[str(field)] = value
        return True

    def hget(self, key, field):
        bucket = self.hash_store.get(key, {})
        return bucket.get(str(field))

    def hgetall(self, key):
        return self.hash_store.get(key, {}).copy()

    def hsetnx(self, key, field, value):
        bucket = self.hash_store.setdefault(key, {})
        field = str(field)
        if field in bucket:
            return False
        bucket[field] = value
        return True

    def hincrby(self, key, field, amount):
        bucket = self.hash_store.setdefault(key, {})
        field = str(field)
        current = bucket.get(field, 0)
        try:
            current = int(current)
        except (TypeError, ValueError):
            current = 0
        current += int(amount)
        bucket[field] = current
        return current


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
def _patch_snapshot_stats(monkeypatch):
    def _stats(_db, themes, _window_start, _window_end):
        return {
            theme.representative.market_id: {"sustained": 3, "reversal": "none", "points": 3}
            for theme in themes
            if theme.representative.market_id
        }

    monkeypatch.setattr("app.core.alerts._build_theme_snapshot_stats", _stats)


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


def _make_config(user_id):
    return UserDigestConfig(
        user_id=user_id,
        name="Trader",
        telegram_chat_id=12345,
        min_liquidity=0.0,
        min_volume_24h=0.0,
        min_abs_price_move=0.0,
        alert_strengths={"STRONG", "MEDIUM"},
        digest_window_minutes=60,
        max_alerts_per_digest=10,
        ai_copilot_enabled=True,
        copilot_user_enabled=True,
        copilot_plan_enabled=True,
        fast_signals_enabled=False,
        fast_window_minutes=defaults.DEFAULT_FAST_WINDOW_MINUTES,
        fast_max_themes_per_digest=defaults.DEFAULT_FAST_MAX_THEMES_PER_DIGEST,
        fast_max_markets_per_theme=defaults.DEFAULT_FAST_MAX_MARKETS_PER_THEME,
        p_min=defaults.DEFAULT_P_MIN,
        p_max=defaults.DEFAULT_P_MAX,
        p_soft_min=defaults.DEFAULT_SOFT_P_MIN,
        p_soft_max=defaults.DEFAULT_SOFT_P_MAX,
        p_strict_min=defaults.DEFAULT_STRICT_P_MIN,
        p_strict_max=defaults.DEFAULT_STRICT_P_MAX,
        allow_info_alerts=True,
        allow_fast_alerts=True,
        plan_name="default",
        max_copilot_per_day=5,
        max_fast_copilot_per_day=5,
        max_copilot_per_hour=defaults.DEFAULT_MAX_COPILOT_PER_HOUR,
        max_copilot_per_digest=1,
        copilot_theme_ttl_minutes=360,
        max_themes_per_digest=5,
        max_markets_per_theme=defaults.DEFAULT_MAX_MARKETS_PER_THEME,
    )


def test_copilot_run_summary_plan_and_user_disabled(db_session, monkeypatch):
    alert = _make_alert()
    db_session.add(alert)
    db_session.commit()

    fake_redis = FakeRedis()
    monkeypatch.setattr("app.core.alerts.redis_conn", fake_redis)
    monkeypatch.setattr("app.core.ai_copilot.redis_conn", fake_redis)
    monkeypatch.setattr(
        "app.core.alerts.classify_alert_with_snapshots",
        lambda *_args, **_kwargs: AlertClassification("REPRICING", "HIGH", "FOLLOW"),
    )
    monkeypatch.setattr("app.core.alerts.queue.enqueue", lambda *args, **kwargs: None)

    user_id = uuid4()
    config = _make_config(user_id)
    config = config.__class__(
        **{
            **config.__dict__,
            "ai_copilot_enabled": False,
            "copilot_user_enabled": False,
            "copilot_plan_enabled": False,
        }
    )

    _enqueue_ai_recommendations(
        db_session,
        config,
        [alert],
        run_id="run-disabled",
        run_started_at=time.time(),
        digest_window_minutes=60,
    )

    key = COPILOT_LAST_STATUS_KEY.format(user_id=user_id)
    payload = json.loads(fake_redis.store[key])
    reasons = payload["summary"]["skipped_by_reason_counts"]
    assert CopilotSkipReason.USER_DISABLED.value in reasons
    assert CopilotSkipReason.PLAN_DISABLED.value in reasons


def test_copilot_run_summary_dedupe_active(db_session, monkeypatch):
    alert = _make_alert()
    db_session.add(alert)
    db_session.commit()

    fake_redis = FakeRedis()
    monkeypatch.setattr("app.core.alerts.redis_conn", fake_redis)
    monkeypatch.setattr("app.core.ai_copilot.redis_conn", fake_redis)
    monkeypatch.setattr(
        "app.core.alerts.classify_alert_with_snapshots",
        lambda *_args, **_kwargs: AlertClassification("REPRICING", "HIGH", "FOLLOW"),
    )
    monkeypatch.setattr("app.core.alerts.queue.enqueue", lambda *args, **kwargs: None)

    user_id = uuid4()
    config = _make_config(user_id)
    theme_key = extract_theme(alert.title, category=alert.category, slug=alert.market_id).theme_key
    dedupe_key = f"copilot:theme:{user_id}:{theme_key}"
    fake_redis.set(dedupe_key, "1", ex=300)

    _enqueue_ai_recommendations(
        db_session,
        config,
        [alert],
        run_id="run-dedupe",
        run_started_at=time.time(),
        digest_window_minutes=60,
    )

    key = COPILOT_LAST_STATUS_KEY.format(user_id=user_id)
    payload = json.loads(fake_redis.store[key])
    reasons = payload["summary"]["skipped_by_reason_counts"]
    assert CopilotSkipReason.DEDUPE_HIT.value in reasons
    assert fake_redis.ttl(dedupe_key) == 300


def test_copilot_run_summary_daily_cap_reached(db_session, monkeypatch):
    alert = _make_alert()
    db_session.add(alert)
    db_session.commit()

    fake_redis = FakeRedis()
    monkeypatch.setattr("app.core.alerts.redis_conn", fake_redis)
    monkeypatch.setattr("app.core.ai_copilot.redis_conn", fake_redis)
    monkeypatch.setattr(
        "app.core.alerts.classify_alert_with_snapshots",
        lambda *_args, **_kwargs: AlertClassification("REPRICING", "HIGH", "FOLLOW"),
    )
    monkeypatch.setattr("app.core.alerts.queue.enqueue", lambda *args, **kwargs: None)

    user_id = uuid4()
    config = _make_config(user_id)
    config = config.__class__(**{**config.__dict__, "max_copilot_per_day": 1})
    date_key = datetime.now(timezone.utc).date().isoformat()
    fake_redis.store[f"copilot:count:{user_id}:{date_key}"] = "1"

    _enqueue_ai_recommendations(
        db_session,
        config,
        [alert],
        run_id="run-cap",
        run_started_at=time.time(),
        digest_window_minutes=60,
    )

    key = COPILOT_LAST_STATUS_KEY.format(user_id=user_id)
    payload = json.loads(fake_redis.store[key])
    reasons = payload["summary"]["skipped_by_reason_counts"]
    assert CopilotSkipReason.CAP_REACHED.value in reasons
    assert payload["summary"]["daily_count"] == 1
    assert payload["summary"]["daily_limit"] == 1


def test_llm_failure_releases_dedupe(db_session, monkeypatch):
    user = User(user_id=uuid4(), name="Trader", telegram_chat_id=123, copilot_enabled=True)
    alert = _make_alert()
    db_session.add_all([user, alert])
    db_session.commit()

    fake_redis = FakeRedis()
    monkeypatch.setattr("app.core.ai_copilot.redis_conn", fake_redis)
    monkeypatch.setattr(
        "app.core.ai_copilot.classify_alert_with_snapshots",
        lambda *_args, **_kwargs: AlertClassification("REPRICING", "HIGH", "FOLLOW"),
    )
    def _raise_llm(*_args, **_kwargs):
        raise RuntimeError("LLM down")

    monkeypatch.setattr("app.core.ai_copilot.get_trade_recommendation", _raise_llm)

    rec = create_ai_recommendation(db_session, user, alert)
    assert rec is None

    theme_key = _theme_key_for_alert(alert)
    dedupe_key = f"copilot:theme:{user.user_id}:{theme_key}"
    assert fake_redis.ttl(dedupe_key) == defaults.COPILOT_DEDUPE_FAILURE_TTL_SECONDS


def test_telegram_failure_skips_daily_count_and_short_ttl(db_session, monkeypatch):
    user = User(user_id=uuid4(), name="Trader", telegram_chat_id=123, copilot_enabled=True)
    alert = _make_alert()
    db_session.add_all([user, alert])
    db_session.commit()

    fake_redis = FakeRedis()
    monkeypatch.setattr("app.core.ai_copilot.redis_conn", fake_redis)
    monkeypatch.setattr(
        "app.core.ai_copilot.classify_alert_with_snapshots",
        lambda *_args, **_kwargs: AlertClassification("REPRICING", "HIGH", "FOLLOW"),
    )
    monkeypatch.setattr(
        "app.core.ai_copilot.get_trade_recommendation",
        lambda *_args, **_kwargs: {
            "recommendation": "WAIT",
            "confidence": "LOW",
            "rationale": "test",
            "risks": "test",
        },
    )
    monkeypatch.setattr("app.core.ai_copilot.send_telegram_message", lambda *args, **kwargs: None)

    create_ai_recommendation(db_session, user, alert)

    date_key = datetime.now(timezone.utc).date().isoformat()
    count_key = f"copilot:count:{user.user_id}:{date_key}"
    assert count_key not in fake_redis.store
    theme_key = _theme_key_for_alert(alert)
    dedupe_key = f"copilot:theme:{user.user_id}:{theme_key}"
    assert fake_redis.ttl(dedupe_key) == defaults.COPILOT_DEDUPE_FAILURE_TTL_SECONDS
