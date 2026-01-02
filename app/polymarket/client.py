import json
from datetime import datetime, timezone
import httpx
from tenacity import retry, stop_after_attempt, wait_exponential
from .schemas import PolymarketMarket
from ..settings import settings


class PolymarketClient:
    def __init__(self):
        self.base_url = settings.POLYMARKET_BASE_URL.rstrip("/")

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=0.5, min=0.5, max=4))
    async def fetch_markets(self, limit: int | None = None) -> list[PolymarketMarket]:
        """
        Fetch markets from Polymarket Gamma API:
        - GET /events?active=true&closed=false&limit=N
        - Each event contains a list of 'markets'
        - Each market has outcomePrices as a JSON-string array: '["0.12","0.88"]'
        """
        url = f"{self.base_url}/events"
        params = {
            "active": "true",
            "closed": "false",
            "limit": str(limit or settings.POLY_LIMIT),
        }

        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.get(url, params=params)
            r.raise_for_status()
            events = r.json()

        markets: list[PolymarketMarket] = []

        for ev in events:
            event_title = (ev.get("title") or "").strip()
            event_slug = ev.get("slug") or ev.get("ticker") or "unknown"

            for m in ev.get("markets", []) or []:
                # Skip if market not tradable
                if m.get("active") is False or m.get("closed") is True:
                    continue

                # Parse YES price from outcomePrices (stringified JSON array)
                raw_prices = m.get("outcomePrices", "[]")
                try:
                    prices = json.loads(raw_prices) if isinstance(raw_prices, str) else raw_prices
                except json.JSONDecodeError:
                    continue

                if not prices:
                    continue

                try:
                    p_yes = float(prices[0])
                except (TypeError, ValueError, IndexError):
                    continue

                # Liquidity: prefer numeric fields if present
                liq = m.get("liquidityNum")
                if liq is None:
                    liq = m.get("liquidity")
                try:
                    liquidity = float(liq or 0.0)
                except (TypeError, ValueError):
                    liquidity = 0.0

                title = (m.get("question") or "").strip()
                if not title:
                    continue

                source_ts = _parse_ts(m.get("lastUpdated") or m.get("updatedAt") or m.get("timestamp"))
                volume_24h = _parse_float(m.get("volume24hr") or m.get("volume24h"))
                volume_1w = _parse_float(m.get("volume1w") or m.get("volume7d"))
                best_ask = _parse_float(m.get("bestAsk"))
                last_trade_price = _parse_float(m.get("lastTradePrice") or m.get("lastTradePriceNum"))

                market_id = str(m.get("id") or m.get("slug") or "")
                if not market_id:
                    continue

                markets.append(
                    PolymarketMarket(
                        market_id=market_id,
                        title=title,
                        category=event_title or str(event_slug),
                        p_yes=p_yes,
                        liquidity=liquidity,
                        volume_24h=volume_24h,
                        volume_1w=volume_1w,
                        best_ask=best_ask,
                        last_trade_price=last_trade_price,
                        source_ts=source_ts,
                    )
                )

        return markets


def _parse_float(value) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _parse_ts(value) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(float(value), tz=timezone.utc)
    if isinstance(value, str):
        v = value.strip()
        if not v:
            return None
        if v.endswith("Z"):
            v = v[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(v)
        except ValueError:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    return None
