import asyncio

import httpx

from app.polymarket.client import PolymarketClient
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


def _market(market_id: str, liquidity: float, volume_24h: float) -> dict:
    return {
        "id": market_id,
        "question": f"Question {market_id}",
        "outcomePrices": '["0.25","0.75"]',
        "liquidityNum": liquidity,
        "volume24hr": volume_24h,
    }


def _event(event_id: int, markets: list[dict]) -> dict:
    return {
        "title": f"Event {event_id}",
        "slug": f"event-{event_id}",
        "markets": markets,
    }


def test_server_filters_include_effective_minimums(monkeypatch):
    responses = {
        0: [_event(1, [_market("m1", 2000, 2000)])],
        1: [],
    }
    calls = []
    monkeypatch.setattr(httpx, "AsyncClient", _mock_async_client(responses, calls))
    monkeypatch.setattr(settings, "POLY_USE_SERVER_FILTERS", True)
    monkeypatch.setattr(settings, "POLY_USE_GLOBAL_MINIMUMS", True)
    monkeypatch.setattr(settings, "POLY_LIQUIDITY_MIN", None)
    monkeypatch.setattr(settings, "POLY_VOLUME_MIN", None)
    monkeypatch.setattr(settings, "GLOBAL_MIN_LIQUIDITY", 123.0)
    monkeypatch.setattr(settings, "GLOBAL_MIN_VOLUME_24H", 456.0)

    async def _run():
        client = PolymarketClient()
        return await client.fetch_markets_paginated(limit=1, start_offset=0)

    asyncio.run(_run())

    assert calls[0]["liquidity_min"] == "123.0"
    assert calls[0]["volume_min"] == "456.0"


def test_local_filtering_applies_to_low_quality_markets(monkeypatch):
    responses = {
        0: [_event(1, [_market("low", 10, 10), _market("high", 2000, 2000)])],
        1: [],
    }
    calls = []
    monkeypatch.setattr(httpx, "AsyncClient", _mock_async_client(responses, calls))
    monkeypatch.setattr(settings, "POLY_USE_SERVER_FILTERS", False)
    monkeypatch.setattr(settings, "POLY_USE_GLOBAL_MINIMUMS", True)
    monkeypatch.setattr(settings, "POLY_LIQUIDITY_MIN", None)
    monkeypatch.setattr(settings, "POLY_VOLUME_MIN", None)
    monkeypatch.setattr(settings, "GLOBAL_MIN_LIQUIDITY", 1000.0)
    monkeypatch.setattr(settings, "GLOBAL_MIN_VOLUME_24H", 1000.0)

    async def _run():
        client = PolymarketClient()
        return await client.fetch_markets_paginated(limit=1, start_offset=0)

    markets = asyncio.run(_run())

    assert [m.market_id for m in markets] == ["high"]


def test_no_effective_minimums_sends_no_filters_and_keeps_markets(monkeypatch):
    responses = {
        0: [_event(1, [_market("low", 10, 10)])],
        1: [],
    }
    calls = []
    monkeypatch.setattr(httpx, "AsyncClient", _mock_async_client(responses, calls))
    monkeypatch.setattr(settings, "POLY_USE_SERVER_FILTERS", True)
    monkeypatch.setattr(settings, "POLY_USE_GLOBAL_MINIMUMS", False)
    monkeypatch.setattr(settings, "POLY_LIQUIDITY_MIN", None)
    monkeypatch.setattr(settings, "POLY_VOLUME_MIN", None)
    monkeypatch.setattr(settings, "GLOBAL_MIN_LIQUIDITY", 1000.0)
    monkeypatch.setattr(settings, "GLOBAL_MIN_VOLUME_24H", 1000.0)

    async def _run():
        client = PolymarketClient()
        return await client.fetch_markets_paginated(limit=1, start_offset=0)

    markets = asyncio.run(_run())

    assert "liquidity_min" not in calls[0]
    assert "volume_min" not in calls[0]
    assert [m.market_id for m in markets] == ["low"]
