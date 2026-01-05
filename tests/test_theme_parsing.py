from app.alerts.theme_key import extract_theme


def test_theme_label_removes_will_prefix():
    title = "Will the price of Bitcoin be above $50,000 on Jan 5 2026?"
    extracted = extract_theme(title, category="crypto", slug="bitcoin-2026")
    assert extracted.theme_label == "BTC 50k band (Jan 5 2026)"


def test_theme_label_includes_total_for_matchups():
    title = "Lakers vs Celtics total 210.5"
    extracted = extract_theme(title, category="nba", slug="lakers-celtics")
    assert extracted.theme_label == "Lakers vs Celtics - Total 210.5"


def test_theme_label_includes_spread_for_matchups():
    title = "Giants vs Jets spread -3.5"
    extracted = extract_theme(title, category="nfl", slug="giants-jets")
    assert extracted.theme_label == "Giants vs Jets - Spread -3.5"
