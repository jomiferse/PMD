import asyncio
import json
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.core import defaults
from app.core.alert_classification import AlertClassification
from app.core.alerts import (
    CopilotIneligibilityReason,
    UserDigestConfig,
    _enqueue_ai_recommendations,
    _send_user_digest,
)
from app.alerts.theme_key import extract_theme
from app.db import Base
from app.models import AiThemeMute, Alert


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

    def expire(self, key, ttl):
        if key not in self.store:
            return False
        self.expirations[key] = ttl
        return True

    def ttl(self, key):
        if key not in self.store:
            return -2
        return self.expirations.get(key, -1)

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


def _make_config(user_id):
    return UserDigestConfig(
        user_id=user_id,
        name="Trader",
        telegram_chat_id="12345",
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
        max_copilot_per_digest=1,
        copilot_theme_ttl_minutes=360,
        max_themes_per_digest=5,
        max_markets_per_theme=defaults.DEFAULT_MAX_MARKETS_PER_THEME,
    )


def _patch_digest_helpers(monkeypatch):
    monkeypatch.setattr("app.core.alerts._digest_recently_sent", lambda *args, **kwargs: False)
    monkeypatch.setattr("app.core.alerts._claim_digest_fingerprint", lambda *args, **kwargs: True)
    monkeypatch.setattr("app.core.alerts._record_digest_sent", lambda *args, **kwargs: None)


def test_ai_enqueue_only_actionable_alerts(db_session, monkeypatch):
    _patch_digest_helpers(monkeypatch)
    user_id = uuid4()
    alert = _make_alert(market_id="market-wait")
    db_session.add(alert)
    db_session.commit()

    def _fake_classify(_, alert_arg):
        return AlertClassification("LIQUIDITY_SWEEP", "MEDIUM", "WAIT")

    monkeypatch.setattr("app.core.alerts.classify_alert_with_snapshots", _fake_classify)

    enqueued = []
    monkeypatch.setattr("app.core.alerts.queue.enqueue", lambda *args, **kwargs: enqueued.append(args))

    class _FakeResponse:
        is_success = True
        status_code = 200
        text = "ok"

    class _FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def post(self, url, json):
            return _FakeResponse()

    monkeypatch.setattr("app.core.alerts.httpx.AsyncClient", _FakeClient)

    result = asyncio.run(_send_user_digest(db_session, "tenant-1", _make_config(user_id)))
    assert result["sent"] is False
    assert not enqueued


def test_ai_enqueue_respects_daily_cap(db_session, monkeypatch):
    user_id = uuid4()
    alert = _make_alert(market_id="market-1")
    db_session.add(alert)
    db_session.commit()
    enqueued = []
    fake_redis = FakeRedis()
    date_key = datetime.now(timezone.utc).date().isoformat()
    fake_redis.store[f"copilot:count:{user_id}:{date_key}"] = "1"
    monkeypatch.setattr("app.core.alerts.redis_conn", fake_redis)
    monkeypatch.setattr(
        "app.core.alerts.classify_alert_with_snapshots",
        lambda *_args, **_kwargs: AlertClassification("REPRICING", "HIGH", "FOLLOW"),
    )
    monkeypatch.setattr("app.core.alerts.queue.enqueue", lambda *args, **kwargs: enqueued.append(args))
    config = _make_config(user_id)
    config = config.__class__(**{**config.__dict__, "max_copilot_per_day": 1})
    _enqueue_ai_recommendations(db_session, config, [alert])
    assert not enqueued


def test_ai_enqueue_respects_digest_cap(db_session, monkeypatch):
    user_id = uuid4()
    alerts = [
        _make_alert(market_id="market-1", liquidity=20000.0),
        _make_alert(market_id="market-2", liquidity=15000.0),
    ]
    db_session.add_all(alerts)
    db_session.commit()

    enqueued = []
    monkeypatch.setattr("app.core.alerts.redis_conn", FakeRedis())
    monkeypatch.setattr(
        "app.core.alerts.classify_alert_with_snapshots",
        lambda *_args, **_kwargs: AlertClassification("REPRICING", "HIGH", "FOLLOW"),
    )
    monkeypatch.setattr("app.core.alerts.queue.enqueue", lambda *args, **kwargs: enqueued.append(args))
    config = _make_config(user_id)
    config = config.__class__(**{**config.__dict__, "max_copilot_per_digest": 1})
    _enqueue_ai_recommendations(db_session, config, alerts)
    assert len(enqueued) == 1


def test_copilot_triggers_for_actionable_theme(db_session, monkeypatch):
    user_id = uuid4()
    alerts = [
        _make_alert(
            market_id="market-1",
            title="Will the price of Bitcoin be above $50 on Jan 5 2026?",
        ),
        _make_alert(
            market_id="market-2",
            title="Will the price of Bitcoin be above $55 on Jan 5 2026?",
        ),
    ]
    db_session.add_all(alerts)
    db_session.commit()

    enqueued = []
    fake_redis = FakeRedis()
    monkeypatch.setattr("app.core.alerts.redis_conn", fake_redis)
    monkeypatch.setattr(
        "app.core.alerts.classify_alert_with_snapshots",
        lambda *_args, **_kwargs: AlertClassification("REPRICING", "HIGH", "FOLLOW"),
    )
    monkeypatch.setattr("app.core.alerts.queue.enqueue", lambda *args, **kwargs: enqueued.append(args))

    _enqueue_ai_recommendations(db_session, _make_config(user_id), alerts)
    assert len(enqueued) == 1
    stored = fake_redis.store[f"copilot:last_eval:{user_id}"]
    payload = json.loads(stored)
    assert payload["themes_eligible_count"] == 1
    assert payload["themes"][0]["reasons"] == []


def test_copilot_skips_non_follow(db_session, monkeypatch):
    user_id = uuid4()
    alert = _make_alert(
        market_id="market-1",
        title="Will the price of Bitcoin be above $50 on Jan 5 2026?",
    )
    db_session.add(alert)
    db_session.commit()

    enqueued = []
    monkeypatch.setattr("app.core.alerts.redis_conn", FakeRedis())
    monkeypatch.setattr(
        "app.core.alerts.classify_alert_with_snapshots",
        lambda *_args, **_kwargs: AlertClassification("REPRICING", "HIGH", "WAIT"),
    )
    monkeypatch.setattr("app.core.alerts.queue.enqueue", lambda *args, **kwargs: enqueued.append(args))

    _enqueue_ai_recommendations(db_session, _make_config(user_id), [alert])
    assert not enqueued


def test_fast_actionable_skips_probability_band_for_copilot(db_session, monkeypatch):
    _patch_digest_helpers(monkeypatch)
    user_id = uuid4()
    alert = _make_alert(
        market_id="market-fast",
        market_p_yes=0.95,
        prev_market_p_yes=0.4,
        old_price=0.4,
        new_price=0.95,
        delta_pct=0.55,
        liquidity=15000.0,
        volume_24h=20000.0,
    )
    db_session.add(alert)
    db_session.commit()

    fake_redis = FakeRedis()
    monkeypatch.setattr("app.core.alerts.redis_conn", fake_redis)
    enqueued: list = []
    monkeypatch.setattr("app.core.alerts.queue.enqueue", lambda *args, **kwargs: enqueued.append(args))

    evaluations = _enqueue_ai_recommendations(
        db_session,
        _make_config(user_id),
        [alert],
        classifier=lambda *_args, **_kwargs: AlertClassification("REPRICING", "HIGH", "FOLLOW"),
    )
    assert evaluations and evaluations[0].reasons == []
    assert len(enqueued) == 1


def test_fast_copilot_skips_snapshot_requirement(db_session, monkeypatch):
    user_id = uuid4()
    alert = _make_alert(market_id="market-fast", move=0.05, delta_pct=0.05)
    db_session.add(alert)
    db_session.commit()

    fake_redis = FakeRedis()
    monkeypatch.setattr("app.core.alerts.redis_conn", fake_redis)
    monkeypatch.setattr(
        "app.core.alerts.classify_alert_with_snapshots",
        lambda *_args, **_kwargs: AlertClassification("REPRICING", "HIGH", "WAIT"),
    )
    monkeypatch.setattr("app.core.alerts._count_snapshot_points", lambda *_args, **_kwargs: 2)
    monkeypatch.setattr("app.core.alerts.queue.enqueue", lambda *args, **kwargs: None)

    config = _make_config(user_id)
    config = config.__class__(**{**config.__dict__, "digest_window_minutes": 15})
    evaluations = _enqueue_ai_recommendations(db_session, config, [alert], digest_window_minutes=15)

    assert evaluations
    reasons = evaluations[0].reasons
    assert CopilotIneligibilityReason.NOT_FOLLOW.value in reasons
    assert CopilotIneligibilityReason.INSUFFICIENT_SNAPSHOTS.value not in reasons


def test_copilot_dedupe_by_theme_key(db_session, monkeypatch):
    user_id = uuid4()
    alert = _make_alert(
        market_id="market-1",
        title="Will the price of Bitcoin be above $50 on Jan 5 2026?",
    )
    db_session.add(alert)
    db_session.commit()

    enqueued = []
    fake_redis = FakeRedis()
    monkeypatch.setattr("app.core.alerts.redis_conn", fake_redis)
    monkeypatch.setattr(
        "app.core.alerts.classify_alert_with_snapshots",
        lambda *_args, **_kwargs: AlertClassification("REPRICING", "HIGH", "FOLLOW"),
    )
    monkeypatch.setattr("app.core.alerts.queue.enqueue", lambda *args, **kwargs: enqueued.append(args))

    theme_key = extract_theme(alert.title, category=alert.category, slug=alert.market_id).theme_key
    dedupe_key = f"copilot:theme:{user_id}:{theme_key}"

    _enqueue_ai_recommendations(db_session, _make_config(user_id), [alert])
    fake_redis.set(dedupe_key, "1", ex=3600)
    _enqueue_ai_recommendations(db_session, _make_config(user_id), [alert])
    assert len(enqueued) == 1


def test_copilot_eval_includes_label_mapping_unknown(db_session, monkeypatch):
    user_id = uuid4()
    alert = _make_alert(
        market_id="market-1",
        mapping_confidence="unknown",
        title="Will the price of Bitcoin be above $50 on Jan 5 2026?",
    )
    db_session.add(alert)
    db_session.commit()

    fake_redis = FakeRedis()
    monkeypatch.setattr("app.core.alerts.redis_conn", fake_redis)
    monkeypatch.setattr(
        "app.core.alerts.classify_alert_with_snapshots",
        lambda *_args, **_kwargs: AlertClassification("REPRICING", "HIGH", "WAIT"),
    )
    monkeypatch.setattr("app.core.alerts.queue.enqueue", lambda *args, **kwargs: None)

    _enqueue_ai_recommendations(db_session, _make_config(user_id), [alert])
    stored = fake_redis.store[f"copilot:last_eval:{user_id}"]
    payload = json.loads(stored)
    reasons = payload["themes"][0]["reasons"]
    assert CopilotIneligibilityReason.LABEL_MAPPING_UNKNOWN.value in reasons


def test_copilot_eval_includes_dedupe_reason(db_session, monkeypatch):
    user_id = uuid4()
    alert = _make_alert(
        market_id="market-1",
        title="Will the price of Bitcoin be above $50 on Jan 5 2026?",
    )
    db_session.add(alert)
    db_session.commit()

    fake_redis = FakeRedis()
    theme_key = extract_theme(alert.title, category=alert.category, slug=alert.market_id).theme_key
    fake_redis.set(f"copilot:theme:{user_id}:{theme_key}", "1", ex=300)
    monkeypatch.setattr("app.core.alerts.redis_conn", fake_redis)
    monkeypatch.setattr(
        "app.core.alerts.classify_alert_with_snapshots",
        lambda *_args, **_kwargs: AlertClassification("REPRICING", "HIGH", "FOLLOW"),
    )
    monkeypatch.setattr("app.core.alerts.queue.enqueue", lambda *args, **kwargs: None)

    _enqueue_ai_recommendations(db_session, _make_config(user_id), [alert])
    stored = fake_redis.store[f"copilot:last_eval:{user_id}"]
    payload = json.loads(stored)
    reasons = payload["themes"][0]["reasons"]
    assert CopilotIneligibilityReason.COPILOT_DEDUPE_ACTIVE.value in reasons


def test_copilot_respects_theme_mute(db_session, monkeypatch):
    user_id = uuid4()
    alert = _make_alert(
        market_id="market-1",
        title="Will the price of Bitcoin be above $50 on Jan 5 2026?",
    )
    db_session.add(alert)
    db_session.commit()

    theme_key = extract_theme(alert.title, category=alert.category, slug=alert.market_id).theme_key
    mute = AiThemeMute(
        user_id=user_id,
        theme_key=theme_key,
        expires_at=datetime.now(timezone.utc) + timedelta(hours=6),
    )
    db_session.add(mute)
    db_session.commit()

    enqueued = []
    monkeypatch.setattr("app.core.alerts.redis_conn", FakeRedis())
    monkeypatch.setattr(
        "app.core.alerts.classify_alert_with_snapshots",
        lambda *_args, **_kwargs: AlertClassification("REPRICING", "HIGH", "FOLLOW"),
    )
    monkeypatch.setattr("app.core.alerts.queue.enqueue", lambda *args, **kwargs: enqueued.append(args))

    _enqueue_ai_recommendations(db_session, _make_config(user_id), [alert])
    assert not enqueued
