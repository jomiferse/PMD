from app.trading.sizing import compute_draft_size, MIN_NOTIONAL_USD, MAX_SIZE_SHARES


def test_sizing_requires_risk_limits():
    assert compute_draft_size(0.0, 10.0, 0.01, 10.0, 1000.0, 0.5) is None
    assert compute_draft_size(10.0, 0.0, 0.01, 10.0, 1000.0, 0.5) is None


def test_sizing_rejects_bad_price():
    assert compute_draft_size(10.0, 10.0, 0.01, 10.0, 1000.0, 0.0) is None


def test_sizing_respects_caps_and_min_notional():
    result = compute_draft_size(100.0, 50.0, 0.01, 100.0, 10000.0, 0.5)
    assert result is not None
    assert result.notional_usd == 50.0
    assert result.size_shares == 100.0

    too_small = compute_draft_size(100.0, 2.0, 0.01, 100.0, 10000.0, 1.0)
    assert too_small is None

    capped = compute_draft_size(1000000.0, 1000000.0, 1.0, 1000000.0, 1000000.0, 0.0001)
    assert capped is not None
    assert capped.size_shares <= MAX_SIZE_SHARES
    assert capped.notional_usd >= MIN_NOTIONAL_USD
