from datetime import datetime, timezone
from app.core.ai_copilot import _apply_fast_recommendation_rules
from app.core.signal_speed import SIGNAL_SPEED_FAST
from app.models import Alert


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


def test_fast_low_confidence_forces_watch_recommendation():
    alert = _make_alert()
    llm_result = {
        "recommendation": "BUY",
        "confidence": "LOW",
        "rationale": "Strong move",
        "risks": "Volatility",
    }
    evidence = ["Observed across 2 snapshots (~10m) within context window"]
    result = _apply_fast_recommendation_rules(llm_result, alert, evidence, SIGNAL_SPEED_FAST)
    assert result["recommendation"] == "WAIT"
