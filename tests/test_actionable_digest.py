import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import uuid4

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.core.alert_classification import AlertClassification
from app.core.alerts import (
    FilterReason,
    UserDigestConfig,
    _is_within_actionable_pyes,
    _normalize_allowed_strengths,
    _send_user_digest,
)
from app.core.effective_settings import _parse_strengths
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
        copilot_user_enabled=False,
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
        max_copilot_per_day=0,
        max_fast_copilot_per_day=0,
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
    monkeypatch.setattr("app.core.alerts._enqueue_ai_recommendations", lambda *args, **kwargs: None)


def _install_fake_telegram(monkeypatch, payloads):
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


def test_wait_alerts_do_not_trigger_digest(db_session, monkeypatch):
    _patch_digest_helpers(monkeypatch)

    alert = _make_alert(market_id="market-wait")
    db_session.add(alert)
    db_session.commit()

    def _fake_classify(_, alert_arg):
        return AlertClassification("LIQUIDITY_SWEEP", "MEDIUM", "WAIT")

    monkeypatch.setattr("app.core.alerts.classify_alert_with_snapshots", _fake_classify)
    payloads = []
    _install_fake_telegram(monkeypatch, payloads)

    result = asyncio.run(_send_user_digest(db_session, "tenant-1", _make_config()))
    assert result["sent"] is False
    assert result["reason"] == "no_selected_alerts"
    assert not payloads


def test_pyes_bounds_filter_excludes_alerts(db_session, monkeypatch):
    _patch_digest_helpers(monkeypatch)

    alert = _make_alert(market_id="market-low", market_p_yes=0.05)
    db_session.add(alert)
    db_session.commit()

    def _fake_classify(_, alert_arg):
        return AlertClassification("REPRICING", "MEDIUM", "FOLLOW")

    monkeypatch.setattr("app.core.alerts.classify_alert_with_snapshots", _fake_classify)

    config = _make_config(p_min=0.15, p_max=0.85)
    result = asyncio.run(_send_user_digest(db_session, "tenant-1", config))
    assert result["sent"] is False
    assert result["reason"] == "no_selected_alerts"


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
    monkeypatch.setattr("app.core.alerts._enqueue_ai_recommendations", lambda *args, **kwargs: None)

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

    config = _make_config()
    first = asyncio.run(_send_user_digest(db_session, "tenant-1", config))
    second = asyncio.run(_send_user_digest(db_session, "tenant-1", config))
    assert first["sent"] is True
    assert second["sent"] is False
    assert second["reason"] == "digest_dedupe"


def test_strong_alert_delivered_without_filters(db_session, monkeypatch):
    _patch_digest_helpers(monkeypatch)

    alert = _make_alert(
        strength="STRONG",
        liquidity=20000.0,
        volume_24h=25000.0,
    )
    db_session.add(alert)
    db_session.commit()

    monkeypatch.setattr(
        "app.core.alerts.classify_alert_with_snapshots",
        lambda *_args, **_kwargs: AlertClassification("REPRICING", "HIGH", "FOLLOW"),
    )

    payloads = []
    _install_fake_telegram(monkeypatch, payloads)

    result = asyncio.run(_send_user_digest(db_session, "tenant-1", _make_config()))
    deliveries = db_session.query(AlertDelivery).all()

    assert result["sent"] is True
    assert payloads
    assert len(deliveries) == 1
    assert deliveries[0].delivery_status == "sent"
    assert deliveries[0].filter_reasons in ([], None)


def test_medium_filtered_when_only_strong_allowed(db_session, monkeypatch):
    _patch_digest_helpers(monkeypatch)

    alert = _make_alert(
        strength="MEDIUM",
        liquidity=15000.0,
        volume_24h=18000.0,
    )
    db_session.add(alert)
    db_session.commit()

    monkeypatch.setattr(
        "app.core.alerts.classify_alert_with_snapshots",
        lambda *_args, **_kwargs: AlertClassification("REPRICING", "HIGH", "FOLLOW"),
    )

    payloads = []
    _install_fake_telegram(monkeypatch, payloads)

    config = _make_config(
        alert_strengths=_normalize_allowed_strengths(_parse_strengths('["STRONG"]')),
    )
    result = asyncio.run(_send_user_digest(db_session, "tenant-1", config))
    deliveries = db_session.query(AlertDelivery).all()

    assert result["sent"] is False
    assert deliveries and deliveries[0].delivery_status == "filtered"
    assert deliveries[0].filter_reasons == [FilterReason.STRENGTH_NOT_ALLOWED.value]


def test_probability_band_prefers_primary_for_multi_outcome():
    multi_outcome = SimpleNamespace(
        market_p_yes=0.95,
        is_yesno=False,
        market_p_primary=0.55,
    )
    yes_no = SimpleNamespace(
        market_p_yes=0.95,
        is_yesno=True,
    )

    assert _is_within_actionable_pyes(multi_outcome, 0.15, 0.85) is True
    assert _is_within_actionable_pyes(yes_no, 0.15, 0.85) is False


def test_theme_grouping_keeps_strong_delivery(db_session, monkeypatch):
    _patch_digest_helpers(monkeypatch)

    strong_alert = _make_alert(
        market_id="market-strong",
        title="Election winner projection",
        strength="STRONG",
        liquidity=12000.0,
        volume_24h=15000.0,
    )
    medium_alert = _make_alert(
        market_id="market-medium",
        title="Election winner projection update",
        strength="MEDIUM",
        liquidity=12000.0,
        volume_24h=15000.0,
    )
    db_session.add_all([strong_alert, medium_alert])
    db_session.commit()

    monkeypatch.setattr(
        "app.core.alerts.classify_alert_with_snapshots",
        lambda *_args, **_kwargs: AlertClassification("REPRICING", "HIGH", "FOLLOW"),
    )

    payloads = []
    _install_fake_telegram(monkeypatch, payloads)

    config = _make_config(
        alert_strengths=_normalize_allowed_strengths(_parse_strengths('["STRONG"]')),
    )
    result = asyncio.run(_send_user_digest(db_session, "tenant-1", config))
    deliveries = db_session.query(AlertDelivery).order_by(AlertDelivery.alert_id.asc()).all()

    assert result["sent"] is True
    assert payloads
    assert len(deliveries) == 2
    assert any(delivery.delivery_status == "sent" for delivery in deliveries)
    medium_delivery = next((d for d in deliveries if d.delivery_status == "filtered"), None)
    assert medium_delivery is not None
    assert medium_delivery.filter_reasons == [FilterReason.STRENGTH_NOT_ALLOWED.value]


def test_fast_repricings_skip_probability_band(db_session, monkeypatch):
    _patch_digest_helpers(monkeypatch)

    alert = _make_alert(
        market_id="market-fast",
        market_p_yes=0.95,
        prev_market_p_yes=0.4,
        old_price=0.4,
        new_price=0.95,
        delta_pct=0.55,
    )
    db_session.add(alert)
    db_session.commit()

    monkeypatch.setattr(
        "app.core.alerts.classify_alert_with_snapshots",
        lambda *_args, **_kwargs: AlertClassification("REPRICING", "HIGH", "FOLLOW"),
    )

    payloads = []
    _install_fake_telegram(monkeypatch, payloads)

    result = asyncio.run(
        _send_user_digest(
            db_session,
            "tenant-1",
            _make_config(allow_fast_alerts=True, allow_info_alerts=False),
        )
    )
    deliveries = db_session.query(AlertDelivery).all()

    assert result["sent"] is True
    assert payloads
    assert deliveries and deliveries[0].filter_reasons in (None, [])


def test_info_only_blocked_on_free_plan(db_session, monkeypatch):
    _patch_digest_helpers(monkeypatch)

    alert = _make_alert(market_id="market-info")
    db_session.add(alert)
    db_session.commit()

    monkeypatch.setattr(
        "app.core.alerts.classify_alert_with_snapshots",
        lambda *_args, **_kwargs: AlertClassification("NOISY", "LOW", "IGNORE"),
    )

    config = _make_config(allow_info_alerts=False, allow_fast_alerts=False)
    result = asyncio.run(_send_user_digest(db_session, "tenant-1", config))
    deliveries = db_session.query(AlertDelivery).all()

    assert result["sent"] is False
    assert deliveries
    assert deliveries[0].filter_reasons == [FilterReason.INFO_ONLY_BLOCKED.value]


def test_info_only_delivered_on_pro_plan(db_session, monkeypatch):
    _patch_digest_helpers(monkeypatch)

    alert = _make_alert(market_id="market-info")
    db_session.add(alert)
    db_session.commit()

    monkeypatch.setattr(
        "app.core.alerts.classify_alert_with_snapshots",
        lambda *_args, **_kwargs: AlertClassification("NOISY", "LOW", "IGNORE"),
    )
    payloads = []
    _install_fake_telegram(monkeypatch, payloads)

    config = _make_config(allow_info_alerts=True, allow_fast_alerts=False, p_strict_min=0.2, p_strict_max=0.8)
    result = asyncio.run(_send_user_digest(db_session, "tenant-1", config))
    deliveries = db_session.query(AlertDelivery).all()

    assert result["sent"] is True
    assert payloads
    assert deliveries and deliveries[0].delivery_status == "sent"


def test_band_differs_by_classification(db_session, monkeypatch):
    _patch_digest_helpers(monkeypatch)

    standard_alert = _make_alert(market_id="market-standard", market_p_yes=0.95, prev_market_p_yes=0.9)
    info_alert = _make_alert(market_id="market-info", market_p_yes=0.95, prev_market_p_yes=0.9)
    db_session.add_all([standard_alert, info_alert])
    db_session.commit()

    def _fake_classify(_, alert_arg):
        if alert_arg.market_id == "market-standard":
            return AlertClassification("LIQUIDITY_SWEEP", "MEDIUM", "WAIT")
        return AlertClassification("NOISY", "LOW", "IGNORE")

    monkeypatch.setattr("app.core.alerts.classify_alert_with_snapshots", _fake_classify)

    config = _make_config(allow_info_alerts=True, allow_fast_alerts=False)
    result = asyncio.run(_send_user_digest(db_session, "tenant-1", config))
    deliveries = {
        delivery.alert_id: delivery
        for delivery in db_session.query(AlertDelivery).all()
    }

    assert result["sent"] is False
    standard_delivery = deliveries.get(standard_alert.id)
    info_delivery = deliveries.get(info_alert.id)
    assert standard_delivery is not None
    assert info_delivery is not None
    assert FilterReason.NON_ACTIONABLE.value in standard_delivery.filter_reasons
    assert FilterReason.STRICT_BAND_BLOCKED.value in info_delivery.filter_reasons
