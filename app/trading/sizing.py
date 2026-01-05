from dataclasses import dataclass


@dataclass(frozen=True)
class DraftSize:
    notional_usd: float
    size_shares: float


@dataclass(frozen=True)
class DraftUnavailable:
    reasons: list[str]


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
) -> DraftSize | DraftUnavailable:
    reasons: list[str] = []
    if max_usd_per_trade <= 0:
        reasons.append("max_usd_per_trade is 0 (or missing)")
    if risk_budget_usd_per_day <= 0:
        reasons.append("risk_budget_usd_per_day is 0 (or missing)")
    if risk_budget_remaining <= 0:
        reasons.append("daily_budget_remaining is 0")
    if price <= 0:
        reasons.append("missing price")
    if liquidity <= 0:
        reasons.append("missing liquidity")
    if reasons:
        return DraftUnavailable(reasons=reasons)

    max_liquidity_usd = max(liquidity, 0.0) * max(max_liquidity_fraction, 0.0)
    notional = min(max_usd_per_trade, risk_budget_remaining, max_liquidity_usd)
    if notional < MIN_NOTIONAL_USD:
        return DraftUnavailable(reasons=["draft_notional_below_min"])

    size = notional / price
    if size < MIN_SIZE_SHARES:
        return DraftUnavailable(reasons=["draft_size_below_min"])
    size = min(size, MAX_SIZE_SHARES)
    notional = size * price
    return DraftSize(notional_usd=notional, size_shares=size)
