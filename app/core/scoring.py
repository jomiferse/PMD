from dataclasses import dataclass

@dataclass
class ScoredMarket:
    market_id: str
    title: str
    category: str
    market_p_yes: float
    model_p_yes: float
    liquidity: float
    edge: float

def clamp01(x: float) -> float:
    return max(0.0, min(1.0, x))

def simple_fair_probability(title: str, category: str) -> float:
    """
    MVP heurístico:
    - Empieza simple y reemplázalo por modelo real (encuestas, bookmakers, etc.)
    - Por ahora: prior neutral 0.5 + pequeños sesgos por categoría si quieres.
    """
    base = 0.5
    cat = (category or "").lower()
    if "sports" in cat:
        base = 0.5
    if "politics" in cat:
        base = 0.5
    return clamp01(base)

def score_market(
    market_id: str,
    title: str,
    category: str,
    market_p_primary: float,
    liquidity: float,
) -> ScoredMarket:
    model_p = simple_fair_probability(title, category)
    edge = model_p - market_p_primary
    return ScoredMarket(
        market_id=market_id,
        title=title,
        category=category or "unknown",
        market_p_yes=market_p_primary,
        model_p_yes=model_p,
        liquidity=liquidity,
        edge=edge,
    )
