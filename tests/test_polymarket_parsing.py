from app.polymarket.client import _parse_markets


def _event_with_market(market: dict) -> dict:
    return {
        "title": "Event 1",
        "slug": "event-1",
        "markets": [market],
    }


def test_parse_yesno_outcomes_sets_is_yesno():
    market = {
        "id": "m1",
        "question": "Will it rain?",
        "outcomePrices": '["0.25","0.75"]',
        "outcomes": '["Yes","No"]',
        "liquidityNum": 2000,
        "volume24hr": 2000,
    }
    markets, parsed = _parse_markets([_event_with_market(market)], None, None)
    assert parsed == 1
    assert len(markets) == 1
    parsed_market = markets[0]
    assert parsed_market.p_primary == 0.25
    assert parsed_market.primary_outcome_label == "Yes"
    assert parsed_market.is_yesno is True


def test_parse_non_yesno_outcomes_sets_label_and_is_yesno_false():
    market = {
        "id": "m2",
        "question": "Who wins?",
        "outcomePrices": '["0.37","0.63"]',
        "outcomes": '["CAR","TB"]',
        "liquidityNum": 2000,
        "volume24hr": 2000,
    }
    markets, parsed = _parse_markets([_event_with_market(market)], None, None)
    assert parsed == 1
    assert len(markets) == 1
    parsed_market = markets[0]
    assert parsed_market.p_primary == 0.37
    assert parsed_market.primary_outcome_label == "CAR"
    assert parsed_market.is_yesno is False
