from datetime import datetime
from pydantic import BaseModel, Field


class PolymarketMarket(BaseModel):
    market_id: str
    title: str
    category: str | None = None
    p_primary: float
    outcome_prices: list[float] = Field(default_factory=list)
    primary_outcome_label: str = "OUTCOME_0"
    mapping_confidence: str = "unknown"
    market_kind: str = "multi"
    is_yesno: bool = False
    liquidity: float = 0.0
    volume_24h: float = 0.0
    volume_1w: float = 0.0
    best_ask: float = 0.0
    last_trade_price: float = 0.0
    source_ts: datetime | None = None
