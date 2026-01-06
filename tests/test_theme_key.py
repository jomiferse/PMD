from app.alerts.theme_key import extract_theme


def test_price_range_grouping():
    title = "Will the price of Bitcoin be between $88,000 and $90,000 on January 3?"
    extracted = extract_theme(title)
    assert extracted.kind == "price_band"
    assert extracted.theme_key.startswith("bitcoin|january-3|price-band")
    assert extracted.short_title in {"88-90k range", "88–90k range"}


def test_price_range_same_day_same_theme_key_prefix():
    title = "Will the price of Bitcoin be between $90,000 and $92,000 on January 3?"
    extracted = extract_theme(title)
    assert extracted.theme_key.startswith("bitcoin|january-3|price-band")


def test_above_market():
    title = "Will the price of Bitcoin be above $90,000 on January 3?"
    extracted = extract_theme(title)
    assert extracted.kind == "price_band"
    assert extracted.theme_key.startswith("bitcoin|january-3|price-band")
    assert extracted.short_title == "Above 90k"


def test_matchup_market():
    title = "Hawks vs. Knicks"
    extracted = extract_theme(title)
    assert extracted.kind == "matchup"
    assert extracted.theme_key == "hawks_knicks|unknown-date|matchup"
    assert extracted.short_title == "Hawks vs Knicks"


def test_robustness_with_punctuation_and_spacing():
    title = "Will  the price of   Bitcoin be, between $88,000  and $90,000, on January 3 ?"
    extracted = extract_theme(title)
    assert extracted.kind == "price_band"
    assert extracted.theme_key.startswith("bitcoin|january-3|price-band")


def test_extract_theme_is_deterministic():
    title = "Will the price of Bitcoin be above $90,000 on January 3?"
    first = extract_theme(title)
    second = extract_theme(title)
    assert first == second


def test_will_question_label_is_concise():
    title = "Will Avatar: Fire and Ash 3rd weekend be less than $40m?"
    extracted = extract_theme(title)
    assert "Will" not in extracted.short_title
    assert "<" in extracted.short_title
    assert "40" in extracted.short_title
    assert len(extracted.short_title) <= 40


def test_matchup_total_label():
    title = "Seahawks vs Rams total 47.5"
    extracted = extract_theme(title)
    assert extracted.kind == "matchup"
    assert "Total 47.5" in extracted.short_title
    assert "Total 47.5" in extracted.theme_label


def test_matchup_spread_label():
    title = "Seahawks vs Rams spread -7.5"
    extracted = extract_theme(title)
    assert extracted.kind == "matchup"
    assert "Spread -7.5" in extracted.short_title
    assert "Spread -7.5" in extracted.theme_label


def test_theme_key_differs_for_distinct_matchups_and_assets():
    matchup_a = extract_theme("Lakers vs Celtics").theme_key
    matchup_b = extract_theme("Heat vs Celtics").theme_key
    assert matchup_a != matchup_b

    btc_theme = extract_theme("Will Bitcoin be above $40,000 on January 3?").theme_key
    eth_theme = extract_theme("Will Ethereum be above $3,000 on January 3?").theme_key
    assert btc_theme != eth_theme
