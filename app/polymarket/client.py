import json
import logging
from datetime import datetime, timezone
import httpx
from tenacity import retry, stop_after_attempt, wait_exponential
from .schemas import PolymarketMarket
from ..settings import settings

logger = logging.getLogger(__name__)


class PolymarketClient:
    def __init__(self):
        self.base_url = settings.POLYMARKET_BASE_URL.rstrip("/")

    async def fetch_markets(self, limit: int | None = None) -> list[PolymarketMarket]:
        return await self.fetch_markets_paginated(limit=limit)

    async def fetch_markets_paginated(
        self,
        limit: int | None = None,
        max_events: int | None = None,
        start_offset: int | None = None,
        max_pages: int | None = None,
        order: str | None = None,
        ascending: bool | None = None,
    ) -> list[PolymarketMarket]:
        """
        Fetch markets from Polymarket Gamma API using offset pagination:
        - GET /events?active=true&closed=false&limit=N&offset=K
        - Each event contains a list of 'markets'
        - Each market has outcomePrices as a JSON-string array: '["0.12","0.88"]'
        - If order/ascending are set, they are passed through to Gamma; otherwise API order applies
        """
        page_limit = _coerce_non_negative_int(limit or settings.POLY_PAGE_LIMIT)
        max_events = _coerce_optional_non_negative_int(
            settings.POLY_MAX_EVENTS if max_events is None else max_events
        )
        max_pages = _coerce_optional_non_negative_int(
            settings.POLY_MAX_PAGES if max_pages is None else max_pages
        )
        offset = _coerce_non_negative_int(
            settings.POLY_START_OFFSET if start_offset is None else start_offset
        )
        order = settings.POLY_ORDER if order is None else order
        ascending = settings.POLY_ASCENDING if ascending is None else ascending
        if page_limit == 0:
            return []
        if max_events is not None and (max_events == 0 or offset >= max_events):
            return []

        markets: list[PolymarketMarket] = []
        url = f"{self.base_url}/events"
        fetched_events = 0
        page_count = 0

        async with httpx.AsyncClient(timeout=15) as client:
            while True:
                if max_events is not None and fetched_events >= max_events:
                    break
                if max_pages is not None and page_count >= max_pages:
                    logger.warning(
                        "polymarket_pagination_max_pages_reached max_pages=%s fetched_events=%s offset=%s",
                        max_pages,
                        fetched_events,
                        offset,
                    )
                    break

                params = _build_events_params(
                    limit=page_limit, offset=offset, order=order, ascending=ascending
                )
                events = await self._fetch_events_page(client, url, params)
                page_count += 1

                if not events:
                    break

                fetched_events += len(events)
                markets.extend(_parse_markets(events))

                if max_events is not None and fetched_events >= max_events:
                    break

                if len(events) < page_limit:
                    break

                offset += page_limit

        return markets

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=0.5, min=0.5, max=4))
    async def _fetch_events_page(
        self, client: httpx.AsyncClient, url: str, params: dict[str, str]
    ) -> list[dict]:
        r = await client.get(url, params=params)
        r.raise_for_status()
        events = r.json()
        return events if isinstance(events, list) else []


def _parse_float(value) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _coerce_non_negative_int(value: int | None) -> int:
    try:
        return max(int(value or 0), 0)
    except (TypeError, ValueError):
        return 0


def _coerce_optional_non_negative_int(value: int | None) -> int | None:
    if value is None:
        return None
    try:
        return max(int(value), 0)
    except (TypeError, ValueError):
        return None


def _build_events_params(
    limit: int, offset: int, order: str | None, ascending: bool | None
) -> dict[str, str]:
    params: dict[str, str] = {
        "active": "true",
        "closed": "false",
        "limit": str(_coerce_non_negative_int(limit)),
        "offset": str(_coerce_non_negative_int(offset)),
    }
    if order:
        params["order"] = order
    if ascending is not None:
        params["ascending"] = "true" if ascending else "false"
    if settings.GLOBAL_MIN_LIQUIDITY > 0:
        params["liquidity_min"] = str(settings.GLOBAL_MIN_LIQUIDITY)
    if settings.GLOBAL_MIN_VOLUME_24H > 0:
        params["volume_min"] = str(settings.GLOBAL_MIN_VOLUME_24H)
    return params


def _parse_markets(events: list[dict]) -> list[PolymarketMarket]:
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

            volume_24h = _parse_float(m.get("volume24hr") or m.get("volume24h"))
            if settings.GLOBAL_MIN_LIQUIDITY > 0 and liquidity < settings.GLOBAL_MIN_LIQUIDITY:
                continue
            if settings.GLOBAL_MIN_VOLUME_24H > 0 and volume_24h < settings.GLOBAL_MIN_VOLUME_24H:
                continue

            title = (m.get("question") or "").strip()
            if not title:
                continue

            source_ts = _parse_ts(m.get("lastUpdated") or m.get("updatedAt") or m.get("timestamp"))
            volume_1w = _parse_float(m.get("volume1w") or m.get("volume7d"))
            best_ask = _parse_float(m.get("bestAsk"))
            last_trade_price = _parse_float(m.get("lastTradePrice") or m.get("lastTradePriceNum"))

            market_id = str(m.get("slug") or m.get("id") or "")
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


def _parse_ts(value) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(float(value), tz=timezone.utc)
    if isinstance(value, str):
        v = value.strip()
        if not v:
            return None
        if " " in v:
            v = v.split()[0]
        try:
            num = float(v)
        except ValueError:
            num = None
        if num is not None:
            if num > 1e14:
                num = num / 1e9
            elif num > 1e11:
                num = num / 1e3
            return datetime.fromtimestamp(num, tz=timezone.utc)
        if v.endswith("Z"):
            v = v[:-1] + "+00:00"
        v = _trim_iso_fraction(v)
        try:
            dt = datetime.fromisoformat(v)
        except ValueError:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    return None


def _trim_iso_fraction(value: str) -> str:
    if "." not in value:
        return value
    tz_pos = None
    t_pos = value.find("T")
    for i in range(len(value) - 1, -1, -1):
        ch = value[i]
        if ch in "+-" and (t_pos == -1 or i > t_pos):
            tz_pos = i
            break
    if tz_pos is None:
        main = value
        tz = ""
    else:
        main = value[:tz_pos]
        tz = value[tz_pos:]
    if "." not in main:
        return value
    pre, frac = main.split(".", 1)
    digits = "".join(ch for ch in frac if ch.isdigit())
    if not digits:
        return pre + tz
    if len(digits) > 6:
        digits = digits[:6]
    return pre + "." + digits + tz
