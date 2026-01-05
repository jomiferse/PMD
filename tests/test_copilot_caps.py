from datetime import datetime, timezone
from uuid import uuid4

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.core import defaults
from app.core.ai_copilot import _send_recommendation_message
from app.core.alert_classification import AlertClassification
from app.core.alerts import UserDigestConfig, _enqueue_ai_recommendations
from app.db import Base
from app.models import AiRecommendation, Alert, User
from app.settings import settings


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

    def incr(self, key):
        current = self.store.get(key)
        if current is None:
            value = 0
        else:
            value = int(current)
        value += 1
        self.store[key] = str(value)
        return value

    def expire(self, key, ttl):
        self.expirations[key] = ttl
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
        risk_budget_usd_per_day=100.0,
        max_usd_per_trade=20.0,
        max_liquidity_fraction=0.01,
        fast_signals_enabled=False,
        fast_window_minutes=defaults.DEFAULT_FAST_WINDOW_MINUTES,
        fast_max_themes_per_digest=defaults.DEFAULT_FAST_MAX_THEMES_PER_DIGEST,
        fast_max_markets_per_theme=defaults.DEFAULT_FAST_MAX_MARKETS_PER_THEME,
        p_min=defaults.DEFAULT_P_MIN,
        p_max=defaults.DEFAULT_P_MAX,
        plan_name="default",
        max_copilot_per_day=5,
        max_copilot_per_digest=1,
        copilot_theme_ttl_minutes=360,
        max_themes_per_digest=5,
        max_markets_per_theme=defaults.DEFAULT_MAX_MARKETS_PER_THEME,
    )


class _FakeResponse:
    def __init__(self, status_code):
        self.status_code = status_code
        self.text = "ok"

    @property
    def is_success(self):
        return 200 <= self.status_code < 300

    def json(self):
        return {"ok": True, "result": {"message_id": 1}}


class _FakeClient:
    def __init__(self, status_code):
        self.status_code = status_code

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def post(self, url, json):
        return _FakeResponse(self.status_code)


def test_copilot_daily_count_increments_on_success(db_session, monkeypatch):
    user = User(
        user_id=uuid4(),
        name="Trader",
        telegram_chat_id="123",
        overrides_json={
            "risk_budget_usd_per_day": 100.0,
            "max_usd_per_trade": 50.0,
            "max_liquidity_fraction": 0.01,
        },
    )
    alert = _make_alert()
    db_session.add_all([user, alert])
    db_session.commit()

    rec = AiRecommendation(
        user_id=user.user_id,
        alert_id=alert.id,
        recommendation="BUY",
        confidence="HIGH",
        rationale="test",
        risks="test",
        status="PROPOSED",
        created_at=datetime.now(timezone.utc),
    )
    db_session.add(rec)
    db_session.commit()

    fake_redis = FakeRedis()
    monkeypatch.setattr("app.core.ai_copilot.redis_conn", fake_redis)
    monkeypatch.setattr(
        "app.core.telegram.httpx.Client",
        lambda *args, **kwargs: _FakeClient(200),
    )

    original_token = settings.TELEGRAM_BOT_TOKEN
    settings.TELEGRAM_BOT_TOKEN = "test-token"
    try:
        _send_recommendation_message(db_session, user, alert, rec, [])
    finally:
        settings.TELEGRAM_BOT_TOKEN = original_token

    date_key = datetime.now(timezone.utc).date().isoformat()
    key = f"copilot:count:{user.user_id}:{date_key}"
    assert fake_redis.store[key] == "1"


def test_copilot_daily_count_skips_on_failure(db_session, monkeypatch):
    user = User(
        user_id=uuid4(),
        name="Trader",
        telegram_chat_id="123",
        overrides_json={
            "risk_budget_usd_per_day": 100.0,
            "max_usd_per_trade": 50.0,
            "max_liquidity_fraction": 0.01,
        },
    )
    alert = _make_alert()
    db_session.add_all([user, alert])
    db_session.commit()

    rec = AiRecommendation(
        user_id=user.user_id,
        alert_id=alert.id,
        recommendation="BUY",
        confidence="HIGH",
        rationale="test",
        risks="test",
        status="PROPOSED",
        created_at=datetime.now(timezone.utc),
    )
    db_session.add(rec)
    db_session.commit()

    fake_redis = FakeRedis()
    monkeypatch.setattr("app.core.ai_copilot.redis_conn", fake_redis)
    monkeypatch.setattr(
        "app.core.telegram.httpx.Client",
        lambda *args, **kwargs: _FakeClient(500),
    )

    original_token = settings.TELEGRAM_BOT_TOKEN
    settings.TELEGRAM_BOT_TOKEN = "test-token"
    try:
        _send_recommendation_message(db_session, user, alert, rec, [])
    finally:
        settings.TELEGRAM_BOT_TOKEN = original_token

    assert not fake_redis.store


def test_copilot_daily_cap_allows_below_limit(db_session, monkeypatch):
    user_id = uuid4()
    alert = _make_alert(market_id="market-1")
    db_session.add(alert)
    db_session.commit()

    fake_redis = FakeRedis()
    date_key = datetime.now(timezone.utc).date().isoformat()
    fake_redis.store[f"copilot:count:{user_id}:{date_key}"] = "1"
    monkeypatch.setattr("app.core.alerts.redis_conn", fake_redis)
    monkeypatch.setattr(
        "app.core.alerts.classify_alert_with_snapshots",
        lambda *_args, **_kwargs: AlertClassification("REPRICING", "HIGH", "FOLLOW"),
    )

    enqueued = []
    monkeypatch.setattr("app.core.alerts.queue.enqueue", lambda *args, **kwargs: enqueued.append(args))

    config = _make_config(user_id)
    config = config.__class__(**{**config.__dict__, "max_copilot_per_day": 2})
    _enqueue_ai_recommendations(db_session, config, [alert])

    assert len(enqueued) == 1


def test_copilot_daily_cap_blocks_at_limit(db_session, monkeypatch):
    user_id = uuid4()
    alert = _make_alert(market_id="market-1")
    db_session.add(alert)
    db_session.commit()

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

    config = _make_config(user_id)
    config = config.__class__(**{**config.__dict__, "max_copilot_per_day": 2})
    _enqueue_ai_recommendations(db_session, config, [alert])

    assert not enqueued


def test_copilot_digest_cap_resets_each_run(db_session, monkeypatch):
    user_id = uuid4()
    alerts_round_one = [
        _make_alert(market_id="market-1", title="Will BTC be above $50?"),
        _make_alert(market_id="market-2", title="Will ETH be above $5?"),
    ]
    alerts_round_two = [
        _make_alert(market_id="market-3", title="Will SOL be above $10?"),
        _make_alert(market_id="market-4", title="Will AVAX be above $12?"),
    ]
    db_session.add_all(alerts_round_one + alerts_round_two)
    db_session.commit()

    fake_redis = FakeRedis()
    monkeypatch.setattr("app.core.alerts.redis_conn", fake_redis)
    monkeypatch.setattr(
        "app.core.alerts.classify_alert_with_snapshots",
        lambda *_args, **_kwargs: AlertClassification("REPRICING", "HIGH", "FOLLOW"),
    )

    enqueued = []
    monkeypatch.setattr("app.core.alerts.queue.enqueue", lambda *args, **kwargs: enqueued.append(args))

    config = _make_config(user_id)
    config = config.__class__(
        **{**config.__dict__, "max_copilot_per_day": 5, "max_copilot_per_digest": 1}
    )
    _enqueue_ai_recommendations(db_session, config, alerts_round_one)
    _enqueue_ai_recommendations(db_session, config, alerts_round_two)

    assert len(enqueued) == 2


def test_copilot_daily_key_is_date_scoped(db_session, monkeypatch):
    user = User(
        user_id=uuid4(),
        name="Trader",
        telegram_chat_id="123",
        overrides_json={
            "risk_budget_usd_per_day": 100.0,
            "max_usd_per_trade": 50.0,
            "max_liquidity_fraction": 0.01,
        },
    )
    alert = _make_alert()
    db_session.add_all([user, alert])
    db_session.commit()

    rec = AiRecommendation(
        user_id=user.user_id,
        alert_id=alert.id,
        recommendation="BUY",
        confidence="HIGH",
        rationale="test",
        risks="test",
        status="PROPOSED",
        created_at=datetime.now(timezone.utc),
    )
    db_session.add(rec)
    db_session.commit()

    class _FixedDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2026, 1, 5, 12, 0, 0, tzinfo=timezone.utc)

    fake_redis = FakeRedis()
    monkeypatch.setattr("app.core.ai_copilot.redis_conn", fake_redis)
    monkeypatch.setattr("app.core.ai_copilot.datetime", _FixedDatetime)
    monkeypatch.setattr(
        "app.core.telegram.httpx.Client",
        lambda *args, **kwargs: _FakeClient(200),
    )

    original_token = settings.TELEGRAM_BOT_TOKEN
    settings.TELEGRAM_BOT_TOKEN = "test-token"
    try:
        _send_recommendation_message(db_session, user, alert, rec, [])
    finally:
        settings.TELEGRAM_BOT_TOKEN = original_token

    key = f"copilot:count:{user.user_id}:2026-01-05"
    assert fake_redis.store[key] == "1"
