from dataclasses import dataclass


@dataclass(frozen=True)
class DraftSize:
    notional_usd: float
    size_shares: float


MIN_NOTIONAL_USD = 5.0
MIN_SIZE_SHARES = 1.0
MAX_SIZE_SHARES = 100000.0


def compute_draft_size(
    risk_budget_usd_per_day: float,
    max_usd_per_trade: float,
    max_liquidity_fraction: float,
    risk_budget_remaining: float,
    liquidity: float,
    price: float,
) -> DraftSize | None:
    if max_usd_per_trade <= 0 or risk_budget_usd_per_day <= 0:
        return None
    if price <= 0:
        return None

    max_liquidity_usd = max(liquidity, 0.0) * max(max_liquidity_fraction, 0.0)
    notional = min(max_usd_per_trade, risk_budget_remaining, max_liquidity_usd)
    if notional < MIN_NOTIONAL_USD:
        return None

    size = notional / price
    if size < MIN_SIZE_SHARES:
        return None
    size = min(size, MAX_SIZE_SHARES)
    notional = size * price
    return DraftSize(notional_usd=notional, size_shares=size)
