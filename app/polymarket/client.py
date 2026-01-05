import json
import logging
from datetime import datetime, timezone
import httpx
from tenacity import retry, stop_after_attempt, wait_exponential
from .schemas import PolymarketMarket
from ..settings import settings
from ..core import defaults

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
        effective_liquidity_min, effective_volume_min = _effective_market_minimums()
        server_filters_enabled = settings.POLY_USE_SERVER_FILTERS
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
            _log_ingestion_summary(
                events_fetched=0,
                markets_parsed=0,
                markets_kept=0,
                liquidity_min=effective_liquidity_min,
                volume_min=effective_volume_min,
                server_filters_enabled=server_filters_enabled,
            )
            return []
        if max_events is not None and (max_events == 0 or offset >= max_events):
            _log_ingestion_summary(
                events_fetched=0,
                markets_parsed=0,
                markets_kept=0,
                liquidity_min=effective_liquidity_min,
                volume_min=effective_volume_min,
                server_filters_enabled=server_filters_enabled,
            )
            return []

        markets: list[PolymarketMarket] = []
        url = f"{self.base_url}/events"
        fetched_events = 0
        page_count = 0
        parsed_markets = 0
        max_events_reached = False

        async with httpx.AsyncClient(timeout=15) as client:
            while True:
                if max_events is not None and fetched_events >= max_events:
                    max_events_reached = True
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
                    limit=page_limit,
                    offset=offset,
                    order=order,
                    ascending=ascending,
                    liquidity_min=effective_liquidity_min,
                    volume_min=effective_volume_min,
                    server_filters_enabled=server_filters_enabled,
                )
                events = await self._fetch_events_page(client, url, params)
                page_count += 1

                if not events:
                    break

                fetched_events += len(events)
                page_markets, page_parsed = _parse_markets(
                    events,
                    liquidity_min=effective_liquidity_min,
                    volume_min=effective_volume_min,
                )
                parsed_markets += page_parsed
                markets.extend(page_markets)

                if max_events is not None and fetched_events >= max_events:
                    max_events_reached = True
                    break

                if len(events) < page_limit:
                    break

                offset += page_limit

        if max_events_reached:
            logger.warning(
                "polymarket_pagination_max_events_reached max_events=%s fetched_events=%s",
                max_events,
                fetched_events,
            )

        _log_ingestion_summary(
            events_fetched=fetched_events,
            markets_parsed=parsed_markets,
            markets_kept=len(markets),
            liquidity_min=effective_liquidity_min,
            volume_min=effective_volume_min,
            server_filters_enabled=server_filters_enabled,
        )
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
    limit: int,
    offset: int,
    order: str | None,
    ascending: bool | None,
    liquidity_min: float | None,
    volume_min: float | None,
    server_filters_enabled: bool,
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
    if server_filters_enabled:
        if liquidity_min is not None:
            params["liquidity_min"] = str(liquidity_min)
        if volume_min is not None:
            params["volume_min"] = str(volume_min)
    return params


def _parse_markets(
    events: list[dict],
    liquidity_min: float | None,
    volume_min: float | None,
) -> tuple[list[PolymarketMarket], int]:
    markets: list[PolymarketMarket] = []
    parsed_count = 0

    for ev in events:
        event_title = (ev.get("title") or "").strip()
        event_slug = ev.get("slug") or ev.get("ticker") or "unknown"

        for m in ev.get("markets", []) or []:
            # Skip if market not tradable
            if m.get("active") is False or m.get("closed") is True:
                continue

            # Parse primary outcome price from outcomePrices (stringified JSON array)
            raw_prices = m.get("outcomePrices", "[]")
            try:
                prices = json.loads(raw_prices) if isinstance(raw_prices, str) else raw_prices
            except json.JSONDecodeError:
                continue

            if not isinstance(prices, list) or not prices:
                continue

            outcome_prices = [_parse_float(price) for price in prices]
            if not outcome_prices:
                continue

            p_primary = outcome_prices[0]

            outcome_labels, label_fields = _extract_outcome_labels(m)
            mapping_confidence = _mapping_confidence(outcome_labels, outcome_prices)
            market_kind = _market_kind_from_labels(outcome_labels)
            primary_outcome_label = (
                outcome_labels[0].strip() if mapping_confidence == "verified" and outcome_labels else "OUTCOME_0"
            )
            is_yesno = market_kind == "yesno"

            # Liquidity: prefer numeric fields if present
            liq = m.get("liquidityNum")
            if liq is None:
                liq = m.get("liquidity")
            try:
                liquidity = float(liq or 0.0)
            except (TypeError, ValueError):
                liquidity = 0.0

            volume_24h = _parse_float(m.get("volume24hr") or m.get("volume24h"))

            title = (m.get("question") or "").strip()
            if not title:
                continue

            source_ts = _parse_ts(m.get("lastUpdated") or m.get("updatedAt") or m.get("timestamp"))
            volume_1w = _parse_float(m.get("volume1wk") or m.get("volume1w") or m.get("volume7d"))
            best_ask = _parse_float(m.get("bestAsk"))
            last_trade_price = _parse_float(m.get("lastTradePrice") or m.get("lastTradePriceNum"))

            market_id = str(m.get("slug") or m.get("id") or "")
            if not market_id:
                continue

            parsed_count += 1
            if liquidity_min is not None and liquidity < liquidity_min:
                continue
            if volume_min is not None and volume_24h < volume_min:
                continue

            if mapping_confidence != "verified":
                logger.warning(
                    "polymarket_outcome_label_mapping_unknown market_id=%s slug=%s title=%s "
                    "label_fields=%s prices_len=%s labels_len=%s",
                    market_id,
                    m.get("slug") or "",
                    title,
                    label_fields,
                    len(outcome_prices),
                    len(outcome_labels),
                )

            markets.append(
                PolymarketMarket(
                    market_id=market_id,
                    title=title,
                    category=event_title or str(event_slug),
                    p_primary=p_primary,
                    outcome_prices=outcome_prices,
                    primary_outcome_label=primary_outcome_label,
                    mapping_confidence=mapping_confidence,
                    market_kind=market_kind,
                    is_yesno=is_yesno,
                    liquidity=liquidity,
                    volume_24h=volume_24h,
                    volume_1w=volume_1w,
                    best_ask=best_ask,
                    last_trade_price=last_trade_price,
                    source_ts=source_ts,
                )
            )

    return markets, parsed_count


def _parse_outcome_labels(raw) -> list[str]:
    if raw is None:
        return []
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except json.JSONDecodeError:
            return []
    if isinstance(raw, list):
        labels: list[str] = []
        for item in raw:
            if isinstance(item, str):
                if item.strip():
                    labels.append(item)
                continue
            if isinstance(item, dict):
                for key in ("outcome", "label", "name", "title"):
                    value = item.get(key)
                    if isinstance(value, str) and value.strip():
                        labels.append(value)
                        break
        return labels
    return []


def _extract_outcome_labels(market: dict) -> tuple[list[str], dict[str, str | list[str]]]:
    candidates = [
        "outcomeLabels",
        "outcomeNames",
        "outcomes",
        "outcomeTokenNames",
        "outcomeTokens",
        "tokens",
    ]
    label_fields: dict[str, str | list[str]] = {}
    picked: list[str] = []
    for key in candidates:
        if key not in market:
            continue
        raw = market.get(key)
        parsed = _parse_outcome_labels(raw)
        if parsed:
            label_fields[key] = parsed
            if not picked:
                picked = parsed
        else:
            label_fields[key] = _summarize_label_field(raw)
    return picked, label_fields


def _summarize_label_field(raw) -> str:
    if raw is None:
        return "none"
    if isinstance(raw, str):
        value = raw.strip()
        return value[:200] if value else "empty_string"
    if isinstance(raw, list):
        return f"list(len={len(raw)})"
    if isinstance(raw, dict):
        keys = ",".join(list(raw.keys())[:5])
        return f"dict(keys={keys})"
    return f"type={type(raw).__name__}"


def _mapping_confidence(labels: list[str], prices: list[float]) -> str:
    if labels and len(labels) == len(prices):
        return "verified"
    return "unknown"


def _is_yesno_outcomes(labels: list[str]) -> bool:
    if len(labels) != 2:
        return False
    normalized = {label.strip().lower() for label in labels if isinstance(label, str)}
    return normalized == {"yes", "no"}


def _is_ou_outcomes(labels: list[str]) -> bool:
    if len(labels) != 2:
        return False
    normalized = {label.strip().lower() for label in labels if isinstance(label, str)}
    return normalized == {"over", "under"}


def _market_kind_from_labels(labels: list[str]) -> str:
    if _is_yesno_outcomes(labels):
        return "yesno"
    if _is_ou_outcomes(labels):
        return "ou"
    return "multi"


def _coerce_optional_positive_float(value: float | None) -> float | None:
    if value is None:
        return None
    try:
        value = float(value)
    except (TypeError, ValueError):
        return None
    if value <= 0:
        return None
    return value


def _effective_market_minimums() -> tuple[float | None, float | None]:
    if settings.POLY_USE_GLOBAL_MINIMUMS:
        liquidity_min = (
            settings.POLY_LIQUIDITY_MIN
            if settings.POLY_LIQUIDITY_MIN is not None
            else defaults.GLOBAL_MIN_LIQUIDITY
        )
        volume_min = (
            settings.POLY_VOLUME_MIN
            if settings.POLY_VOLUME_MIN is not None
            else defaults.GLOBAL_MIN_VOLUME_24H
        )
    else:
        liquidity_min = settings.POLY_LIQUIDITY_MIN
        volume_min = settings.POLY_VOLUME_MIN
    return (
        _coerce_optional_positive_float(liquidity_min),
        _coerce_optional_positive_float(volume_min),
    )


def _log_ingestion_summary(
    events_fetched: int,
    markets_parsed: int,
    markets_kept: int,
    liquidity_min: float | None,
    volume_min: float | None,
    server_filters_enabled: bool,
) -> None:
    logger.info(
        "polymarket_ingestion_summary events_fetched=%s markets_parsed=%s markets_kept=%s "
        "liquidity_min=%s volume_min=%s server_filters_enabled=%s",
        events_fetched,
        markets_parsed,
        markets_kept,
        liquidity_min,
        volume_min,
        server_filters_enabled,
    )


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
