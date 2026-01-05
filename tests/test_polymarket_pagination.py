import asyncio
import logging

import httpx

from app.polymarket.client import PolymarketClient
from app.core import defaults
from app.settings import settings


def _mock_async_client(responses, calls):
    class MockAsyncClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def get(self, url, params=None):
            calls.append(params or {})
            offset = int((params or {}).get("offset", 0))
            data = responses.get(offset, [])
            request = httpx.Request("GET", url, params=params)
            return httpx.Response(200, json=data, request=request)

    return MockAsyncClient


def _event(event_id: int, market_id: str) -> dict:
    return {
        "title": f"Event {event_id}",
        "slug": f"event-{event_id}",
        "markets": [
            {
                "id": market_id,
                "question": f"Question {market_id}",
                "outcomePrices": '["0.25","0.75"]',
                "liquidityNum": 2000,
                "volume24hr": 2000,
            }
        ],
    }


def test_paginated_offsets_and_flattening(monkeypatch):
    responses = {
        0: [_event(1, "m1"), _event(2, "m2")],
        2: [_event(3, "m3"), _event(4, "m4")],
        4: [],
    }
    calls = []
    monkeypatch.setattr(httpx, "AsyncClient", _mock_async_client(responses, calls))
    monkeypatch.setattr(defaults, "GLOBAL_MIN_LIQUIDITY", 0.0)
    monkeypatch.setattr(defaults, "GLOBAL_MIN_VOLUME_24H", 0.0)

    async def _run():
        client = PolymarketClient()
        markets = await client.fetch_markets_paginated(limit=2, max_events=10, start_offset=0)
        return markets

    markets = asyncio.run(_run())

    assert [int(c["offset"]) for c in calls] == [0, 2, 4]
    assert [m.market_id for m in markets] == ["m1", "m2", "m3", "m4"]


def test_paginated_stops_on_short_page(monkeypatch):
    responses = {
        0: [_event(1, "m1"), _event(2, "m2")],
        2: [_event(3, "m3")],
        4: [_event(4, "m4")],
    }
    calls = []
    monkeypatch.setattr(httpx, "AsyncClient", _mock_async_client(responses, calls))
    monkeypatch.setattr(defaults, "GLOBAL_MIN_LIQUIDITY", 0.0)
    monkeypatch.setattr(defaults, "GLOBAL_MIN_VOLUME_24H", 0.0)

    async def _run():
        client = PolymarketClient()
        markets = await client.fetch_markets_paginated(limit=2, max_events=10, start_offset=0)
        return markets

    markets = asyncio.run(_run())

    assert [int(c["offset"]) for c in calls] == [0, 2]
    assert [m.market_id for m in markets] == ["m1", "m2", "m3"]


def test_paginated_respects_max_events(monkeypatch):
    responses = {
        0: [_event(1, "m1"), _event(2, "m2")],
        2: [_event(3, "m3")],
        3: [_event(4, "m4")],
    }
    calls = []
    monkeypatch.setattr(httpx, "AsyncClient", _mock_async_client(responses, calls))
    monkeypatch.setattr(defaults, "GLOBAL_MIN_LIQUIDITY", 0.0)
    monkeypatch.setattr(defaults, "GLOBAL_MIN_VOLUME_24H", 0.0)

    async def _run():
        client = PolymarketClient()
        markets = await client.fetch_markets_paginated(limit=2, max_events=3, start_offset=0)
        return markets

    markets = asyncio.run(_run())

    assert [int(c["limit"]) for c in calls] == [2, 2]
    assert [int(c["offset"]) for c in calls] == [0, 2]
    assert [m.market_id for m in markets] == ["m1", "m2", "m3"]


def test_paginated_stops_on_max_pages_with_warning(monkeypatch, caplog):
    responses = {
        0: [_event(1, "m1"), _event(2, "m2")],
        2: [_event(3, "m3"), _event(4, "m4")],
        4: [_event(5, "m5"), _event(6, "m6")],
    }
    calls = []
    monkeypatch.setattr(httpx, "AsyncClient", _mock_async_client(responses, calls))
    monkeypatch.setattr(defaults, "GLOBAL_MIN_LIQUIDITY", 0.0)
    monkeypatch.setattr(defaults, "GLOBAL_MIN_VOLUME_24H", 0.0)
    monkeypatch.setattr(settings, "POLY_MAX_PAGES", 2)

    async def _run():
        client = PolymarketClient()
        markets = await client.fetch_markets_paginated(limit=2, max_events=None, start_offset=0)
        return markets

    with caplog.at_level(logging.WARNING):
        markets = asyncio.run(_run())

    assert [int(c["offset"]) for c in calls] == [0, 2]
    assert [m.market_id for m in markets] == ["m1", "m2", "m3", "m4"]
    assert "polymarket_pagination_max_pages_reached" in caplog.text


def test_paginated_ignores_none_max_events(monkeypatch):
    responses = {
        0: [_event(1, "m1"), _event(2, "m2")],
        2: [_event(3, "m3"), _event(4, "m4")],
        4: [_event(5, "m5"), _event(6, "m6")],
        6: [],
    }
    calls = []
    monkeypatch.setattr(httpx, "AsyncClient", _mock_async_client(responses, calls))
    monkeypatch.setattr(defaults, "GLOBAL_MIN_LIQUIDITY", 0.0)
    monkeypatch.setattr(defaults, "GLOBAL_MIN_VOLUME_24H", 0.0)
    monkeypatch.setattr(settings, "POLY_MAX_EVENTS", None)
    monkeypatch.setattr(settings, "POLY_MAX_PAGES", None)

    async def _run():
        client = PolymarketClient()
        markets = await client.fetch_markets_paginated(limit=2, max_events=None, start_offset=0)
        return markets

    markets = asyncio.run(_run())

    assert [int(c["offset"]) for c in calls] == [0, 2, 4, 6]
    assert [m.market_id for m in markets] == ["m1", "m2", "m3", "m4", "m5", "m6"]
