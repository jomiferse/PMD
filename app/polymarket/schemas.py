from datetime import datetime
from pydantic import BaseModel

class PolymarketMarket(BaseModel):
    market_id: str
    title: str
    category: str | None = None
    p_yes: float
    liquidity: float = 0.0
    volume_24h: float = 0.0
    volume_1w: float = 0.0
    best_ask: float = 0.0
    last_trade_price: float = 0.0
    source_ts: datetime | None = None
