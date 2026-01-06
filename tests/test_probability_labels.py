from datetime import datetime, timezone

from app.core import alerts as alerts_module
from app.core import ai_copilot as copilot_module
from app.models import Alert


def _make_alert(**overrides) -> Alert:
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
        primary_outcome_label="YES",
        is_yesno=True,
        mapping_confidence="verified",
        market_kind="yesno",
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


def test_probability_label_yesno():
    alert = _make_alert(market_kind="yesno", is_yesno=True)
    assert alerts_module._format_probability_label(alert) == "p_yes"
    assert copilot_module._format_probability_label(alert) == "p_yes"


def test_probability_label_verified_outcome():
    alert = _make_alert(
        market_kind="multi",
        is_yesno=False,
        mapping_confidence="verified",
        primary_outcome_label="Lakers",
    )
    assert alerts_module._format_probability_label(alert) == "p_LAKERS"
    assert copilot_module._format_probability_label(alert) == "p_LAKERS"


def test_probability_label_unknown_mapping():
    alert = _make_alert(
        market_kind="multi",
        is_yesno=False,
        mapping_confidence="unknown",
        primary_outcome_label="Lakers",
    )
    assert alerts_module._format_probability_label(alert) == "p_outcome0"
    assert copilot_module._format_probability_label(alert) == "p_outcome0"


def test_probability_label_over_under_requires_ou():
    alert = _make_alert(
        market_kind="multi",
        is_yesno=False,
        mapping_confidence="verified",
        primary_outcome_label="Over",
    )
    assert alerts_module._format_probability_label(alert) == "p_outcome0"
    assert copilot_module._format_probability_label(alert) == "p_outcome0"

