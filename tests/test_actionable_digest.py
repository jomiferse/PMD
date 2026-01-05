import asyncio
from datetime import datetime, timezone
from uuid import uuid4

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.core.alert_classification import AlertClassification
from app.core.alerts import UserDigestConfig, _send_user_digest
from app.models import Alert, AlertDelivery
from app.core import defaults


@pytest.fixture()
def db_session():
    engine = create_engine("sqlite:///:memory:", future=True)
    Alert.__table__.create(bind=engine)
    AlertDelivery.__table__.create(bind=engine)
    SessionLocal = sessionmaker(bind=engine)
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()


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


def _make_config(**overrides):
    data = dict(
        user_id=uuid4(),
        name="Trader",
        telegram_chat_id="12345",
        min_liquidity=0.0,
        min_volume_24h=0.0,
        min_abs_price_move=0.0,
        alert_strengths={"STRONG", "MEDIUM"},
        digest_window_minutes=60,
        max_alerts_per_digest=10,
        ai_copilot_enabled=False,
        risk_budget_usd_per_day=0.0,
        max_usd_per_trade=0.0,
        max_liquidity_fraction=0.01,
        fast_signals_enabled=False,
        fast_window_minutes=defaults.DEFAULT_FAST_WINDOW_MINUTES,
        fast_max_themes_per_digest=defaults.DEFAULT_FAST_MAX_THEMES_PER_DIGEST,
        fast_max_markets_per_theme=defaults.DEFAULT_FAST_MAX_MARKETS_PER_THEME,
        p_min=defaults.DEFAULT_P_MIN,
        p_max=defaults.DEFAULT_P_MAX,
        plan_name="default",
        max_copilot_per_day=0,
        max_copilot_per_digest=1,
        copilot_theme_ttl_minutes=360,
        max_themes_per_digest=5,
        max_markets_per_theme=defaults.DEFAULT_MAX_MARKETS_PER_THEME,
    )
    data.update(overrides)
    return UserDigestConfig(**data)


def _patch_digest_helpers(monkeypatch):
    monkeypatch.setattr("app.core.alerts._digest_recently_sent", lambda *args, **kwargs: False)
    monkeypatch.setattr("app.core.alerts._claim_digest_fingerprint", lambda *args, **kwargs: True)
    monkeypatch.setattr("app.core.alerts._record_digest_sent", lambda *args, **kwargs: None)


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


def test_wait_alerts_do_not_trigger_digest(db_session, monkeypatch):
    _patch_digest_helpers(monkeypatch)

    alert = _make_alert(market_id="market-wait")
    db_session.add(alert)
    db_session.commit()

    def _fake_classify(_, alert_arg):
        return AlertClassification("LIQUIDITY_SWEEP", "MEDIUM", "WAIT")

    monkeypatch.setattr("app.core.alerts.classify_alert_with_snapshots", _fake_classify)

    result = asyncio.run(_send_user_digest(db_session, "tenant-1", _make_config()))
    assert result["sent"] is False
    assert result["reason"] == "no_actionable_alerts"


def test_pyes_bounds_filter_excludes_alerts(db_session, monkeypatch):
    _patch_digest_helpers(monkeypatch)

    alert = _make_alert(market_id="market-low", market_p_yes=0.05)
    db_session.add(alert)
    db_session.commit()

    def _fake_classify(_, alert_arg):
        return AlertClassification("REPRICING", "HIGH", "FOLLOW")

    monkeypatch.setattr("app.core.alerts.classify_alert_with_snapshots", _fake_classify)

    config = _make_config(p_min=0.15, p_max=0.85)
    result = asyncio.run(_send_user_digest(db_session, "tenant-1", config))
    assert result["sent"] is False
    assert result["reason"] == "no_actionable_alerts"


def test_digest_sends_only_with_actionable_alerts(db_session, monkeypatch):
    _patch_digest_helpers(monkeypatch)

    actionable = _make_alert(market_id="market-actionable")
    wait_alert = _make_alert(market_id="market-wait")
    db_session.add_all([actionable, wait_alert])
    db_session.commit()

    def _fake_classify(_, alert_arg):
        if alert_arg.market_id == "market-actionable":
            return AlertClassification("REPRICING", "HIGH", "FOLLOW")
        return AlertClassification("LIQUIDITY_SWEEP", "MEDIUM", "WAIT")

    monkeypatch.setattr("app.core.alerts.classify_alert_with_snapshots", _fake_classify)

    payloads = []

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
            payloads.append(json)
            return _FakeResponse()

    monkeypatch.setattr("app.core.alerts.httpx.AsyncClient", _FakeClient)

    result = asyncio.run(_send_user_digest(db_session, "tenant-1", _make_config()))
    assert result["sent"] is True
    assert payloads


def test_digest_caps_actionable_items(db_session, monkeypatch):
    _patch_digest_helpers(monkeypatch)

    original_cap = defaults.MAX_ACTIONABLE_PER_DIGEST
    defaults.MAX_ACTIONABLE_PER_DIGEST = 2
    try:
        alerts = [
            _make_alert(market_id="market-1", title="Market One", move=0.2, old_price=0.4, new_price=0.6),
            _make_alert(market_id="market-2", title="Market Two", move=0.18, old_price=0.4, new_price=0.58),
            _make_alert(market_id="market-3", title="Market Three", move=0.16, old_price=0.4, new_price=0.56),
        ]
        db_session.add_all(alerts)
        db_session.commit()

        def _fake_classify(_, alert_arg):
            return AlertClassification("REPRICING", "HIGH", "FOLLOW")

        monkeypatch.setattr("app.core.alerts.classify_alert_with_snapshots", _fake_classify)

        payloads = []

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
                payloads.append(json)
                return _FakeResponse()

        monkeypatch.setattr("app.core.alerts.httpx.AsyncClient", _FakeClient)

        result = asyncio.run(_send_user_digest(db_session, "tenant-1", _make_config()))
        assert result["sent"] is True
        assert payloads
        text = payloads[0]["text"]
        assert text.count("THEME") == 2
    finally:
        defaults.MAX_ACTIONABLE_PER_DIGEST = original_cap


def test_digest_header_format(db_session, monkeypatch):
    _patch_digest_helpers(monkeypatch)

    alert = _make_alert(market_id="market-1")
    db_session.add(alert)
    db_session.commit()

    def _fake_classify(_, alert_arg):
        return AlertClassification("REPRICING", "HIGH", "FOLLOW")

    monkeypatch.setattr("app.core.alerts.classify_alert_with_snapshots", _fake_classify)

    payloads = []

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
            payloads.append(json)
            return _FakeResponse()

    monkeypatch.setattr("app.core.alerts.httpx.AsyncClient", _FakeClient)

    result = asyncio.run(_send_user_digest(db_session, "tenant-1", _make_config()))
    assert result["sent"] is True
    assert payloads
    assert "PMD - 1 theme (60m)" in payloads[0]["text"]


def test_digest_dedupe_skips_duplicate_send(db_session, monkeypatch):
    monkeypatch.setattr("app.core.alerts._digest_recently_sent", lambda *args, **kwargs: False)
    monkeypatch.setattr("app.core.alerts._record_digest_sent", lambda *args, **kwargs: None)
    monkeypatch.setattr("app.core.alerts.redis_conn", FakeRedis())

    alert = _make_alert(market_id="market-1")
    db_session.add(alert)
    db_session.commit()

    monkeypatch.setattr(
        "app.core.alerts.classify_alert_with_snapshots",
        lambda *_args, **_kwargs: AlertClassification("REPRICING", "HIGH", "FOLLOW"),
    )

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

    first = asyncio.run(_send_user_digest(db_session, "tenant-1", _make_config()))
    second = asyncio.run(_send_user_digest(db_session, "tenant-1", _make_config()))
    assert first["sent"] is True
    assert second["sent"] is False
    assert second["reason"] == "digest_dedupe"
