from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from typing import Literal

_STOPWORDS = {
    "will",
    "the",
    "a",
    "an",
    "be",
    "of",
    "price",
    "prices",
    "on",
    "by",
    "in",
    "at",
    "for",
    "to",
    "is",
    "are",
    "between",
    "and",
    "above",
    "below",
    "over",
    "under",
    "vs",
    "versus",
}

_LEAGUE_TOKENS = {"nba", "nfl", "nhl", "mlb", "mls", "ncaa", "ufc"}

_MONTHS = {
    "jan": "january",
    "feb": "february",
    "mar": "march",
    "apr": "april",
    "may": "may",
    "jun": "june",
    "jul": "july",
    "aug": "august",
    "sep": "september",
    "oct": "october",
    "nov": "november",
    "dec": "december",
}

_MONTHS_SHORT = {value: key.title() for key, value in _MONTHS.items()}

_ASSET_ALIASES = {
    "bitcoin": "bitcoin",
    "btc": "bitcoin",
    "ethereum": "ethereum",
    "eth": "ethereum",
    "solana": "solana",
    "sol": "solana",
    "dogecoin": "dogecoin",
    "doge": "dogecoin",
    "xrp": "xrp",
    "ripple": "xrp",
    "cardano": "cardano",
    "ada": "cardano",
    "litecoin": "litecoin",
    "ltc": "litecoin",
}

_ASSET_DISPLAY = {
    "bitcoin": "BTC",
    "ethereum": "ETH",
    "solana": "SOL",
    "dogecoin": "DOGE",
    "xrp": "XRP",
    "cardano": "ADA",
    "litecoin": "LTC",
}


@dataclass(frozen=True)
class ThemeExtract:
    theme_key: str
    theme_label: str
    short_title: str
    kind: Literal["price_band", "intraday_direction", "matchup", "generic"]
    underlying: str | None = None
    date_key: str | None = None
    range_low: int | None = None
    range_high: int | None = None
    strike: int | None = None
    time_key: str | None = None
    team_a: str | None = None
    team_b: str | None = None


def normalize_text(text: str) -> str:
    text = text.lower()
    text = text.translate(
        str.maketrans(
            {
                "\u2018": "'",
                "\u2019": "'",
                "\u201c": '"',
                "\u201d": '"',
                "\u2013": "-",
                "\u2014": "-",
            }
        )
    )
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def strip_stopwords(tokens: list[str]) -> list[str]:
    return [token for token in tokens if token and token not in _STOPWORDS]


def parse_int_money(raw: str) -> int:
    cleaned = re.sub(r"[,\s$]", "", raw)
    if not cleaned:
        return 0
    try:
        return int(float(cleaned))
    except ValueError:
        return 0


def parse_date_like(title: str) -> str:
    text = normalize_text(title)
    match = re.search(
        r"\b(?:on\s+)?(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|"
        r"jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:tember)?|oct(?:ober)?|"
        r"nov(?:ember)?|dec(?:ember)?)\s+(\d{1,2})(?:\s*,?\s*(\d{4}))?\b",
        text,
    )
    if not match:
        return "unknown-date"
    month = _MONTHS.get(match.group(1)[:3], match.group(1))
    day = match.group(2)
    year = match.group(3)
    if year:
        return f"{month}-{day}-{year}"
    return f"{month}-{day}"


def format_k(value: int) -> str:
    if value >= 1_000_000:
        return _format_compact(value, 1_000_000, "m")
    if value >= 1_000:
        return _format_compact(value, 1_000, "k")
    return str(value)


def format_range(low: int, high: int) -> str:
    return f"{format_k(low)}-{format_k(high)}"


def extract_theme(title: str, *, category: str | None = None, slug: str | None = None) -> ThemeExtract:
    title = title or ""
    normalized = normalize_text(title)
    tokens = normalized.split()
    tokens_no_stop = strip_stopwords(tokens)

    date_key = parse_date_like(title)
    underlying = _detect_underlying(tokens_no_stop)

    range_low, range_high = _extract_price_range(title)
    if range_low is not None and range_high is not None and "between" in tokens and "and" in tokens:
        underlying = underlying or _fallback_underlying(tokens_no_stop)
        theme_key = f"{underlying}|{date_key}|price-band"
        label = _build_price_band_label(underlying, date_key, None, is_range=True)
        short_title = f"{format_range(range_low, range_high)} range"
        return ThemeExtract(
            theme_key=theme_key,
            theme_label=label,
            short_title=short_title,
            kind="price_band",
            underlying=underlying,
            date_key=date_key,
            range_low=range_low,
            range_high=range_high,
        )

    direction, strike = _extract_directional_strike(title)
    if direction and strike is not None:
        underlying = underlying or _fallback_underlying(tokens_no_stop)
        time_key, time_label = _extract_time_key(title)
        if time_key:
            theme_key = f"{underlying}|{date_key}|{time_key}|intraday"
            label = _build_intraday_label(underlying, date_key, time_label)
            kind: Literal["intraday_direction", "price_band"] = "intraday_direction"
        else:
            theme_key = f"{underlying}|{date_key}|price-band"
            label = _build_price_band_label(underlying, date_key, strike, is_range=False)
            kind = "price_band"
        short_title = f"{direction.title()} {format_k(strike)}"
        return ThemeExtract(
            theme_key=theme_key,
            theme_label=label,
            short_title=short_title,
            kind=kind,
            underlying=underlying,
            date_key=date_key,
            strike=strike,
            time_key=time_key,
        )

    matchup = _extract_matchup(tokens, normalized)
    if matchup:
        team_a, team_b = matchup
        date_key = _matchup_date(title, category, slug)
        team_a_key = team_a.replace(" ", "-")
        team_b_key = team_b.replace(" ", "-")
        ordered = "_".join(sorted([team_a_key, team_b_key]))
        theme_key = f"{ordered}|{date_key}|matchup"
        label, short_title = _build_matchup_labels(title, team_a, team_b, date_key)
        return ThemeExtract(
            theme_key=theme_key,
            theme_label=label,
            short_title=short_title,
            kind="matchup",
            date_key=date_key,
            team_a=team_a,
            team_b=team_b,
        )

    fingerprint = tokens_no_stop[:6]
    fingerprint_key = "_".join(fingerprint) if fingerprint else "unknown"
    if len(fingerprint_key) > 80:
        fingerprint_key = hashlib.sha1(fingerprint_key.encode("utf-8")).hexdigest()[:16]
    theme_key = f"generic|{fingerprint_key}"
    label = _build_generic_label(normalized)
    if normalized.startswith("will "):
        will_label = _build_will_label(title)
        if will_label:
            label = will_label
    return ThemeExtract(
        theme_key=theme_key,
        theme_label=label,
        short_title=label,
        kind="generic",
    )


def _format_compact(value: int, scale: int, suffix: str) -> str:
    scaled = value / scale
    if scaled.is_integer():
        return f"{int(scaled)}{suffix}"
    text = f"{scaled:.1f}".rstrip("0").rstrip(".")
    return f"{text}{suffix}"


def _detect_underlying(tokens: list[str]) -> str | None:
    for token in tokens:
        alias = _ASSET_ALIASES.get(token)
        if alias:
            return alias
    return None


def _fallback_underlying(tokens: list[str]) -> str:
    for token in tokens:
        if token.isdigit():
            continue
        return token
    return "market"


def _extract_price_range(title: str) -> tuple[int | None, int | None]:
    match = re.search(
        r"\bbetween\s+\$?([\d,]+(?:\.\d+)?)\s+and\s+\$?([\d,]+(?:\.\d+)?)\b",
        title,
        flags=re.IGNORECASE,
    )
    if not match:
        return None, None
    low = parse_int_money(match.group(1))
    high = parse_int_money(match.group(2))
    if low > high:
        low, high = high, low
    return low, high


def _extract_directional_strike(title: str) -> tuple[str | None, int | None]:
    match = re.search(
        r"\b(above|over|below|under)\s+\$?([\d,]+(?:\.\d+)?)\b",
        title,
        flags=re.IGNORECASE,
    )
    if not match:
        return None, None
    keyword = match.group(1).lower()
    direction = "above" if keyword in {"above", "over"} else "below"
    return direction, parse_int_money(match.group(2))


def _extract_time_key(title: str) -> tuple[str | None, str | None]:
    match = re.search(r"\b(\d{1,2})(?::(\d{2}))?\s*(am|pm)\s*(et|ct|mt|pt)?\b", title, re.IGNORECASE)
    if not match:
        return None, None
    hour = match.group(1)
    minute = match.group(2) or ""
    ampm = match.group(3).lower()
    tz = match.group(4).lower() if match.group(4) else None
    minute_key = f"{minute}" if minute else ""
    key = f"{hour}{minute_key}{ampm}"
    label = f"{hour}{':' + minute if minute else ''}{ampm.upper()}"
    if tz:
        key = f"{key}-{tz}"
        label = f"{label} {tz.upper()}"
    return key, label


def _extract_matchup(tokens: list[str], normalized: str) -> tuple[str, str] | None:
    if "vs" not in tokens and "versus" not in tokens and "v" not in tokens:
        return None
    parts = re.split(r"\b(?:vs|versus|v)\b", normalized, maxsplit=1)
    if len(parts) < 2:
        return None
    left_tokens = strip_stopwords(parts[0].split())
    right_tokens = strip_stopwords(parts[1].split())
    team_a = _compact_team_token(left_tokens)
    team_b = _compact_team_token(right_tokens)
    if not team_a or not team_b:
        return None
    return team_a, team_b


def _compact_team_token(tokens: list[str]) -> str:
    filtered = [token for token in tokens if _token_is_team_word(token)]
    if not filtered:
        return ""
    tail = filtered[-3:]
    return " ".join(tail).strip()


def _token_is_team_word(token: str) -> bool:
    if not token:
        return False
    if token in _LEAGUE_TOKENS:
        return False
    if token.isdigit():
        return False
    if token in {"u", "o", "ou", "total", "spread", "line", "over", "under"}:
        return False
    if any(ch.isdigit() for ch in token):
        return False
    return True


def _matchup_date(title: str, category: str | None, slug: str | None) -> str:
    date_key = parse_date_like(title)
    if date_key != "unknown-date":
        return date_key
    for source in (category, slug):
        if not source:
            continue
        match = re.search(r"\b\d{4}-\d{2}-\d{2}\b", source)
        if match:
            return match.group(0)
    return "unknown-date"


def _build_price_band_label(underlying: str, date_key: str, strike: int | None, *, is_range: bool) -> str:
    display = _display_underlying(underlying)
    if is_range:
        base = f"{display} price band"
    elif strike is not None:
        base = f"{display} {format_k(strike)} band"
    else:
        base = f"{display} price band"
    date_label = _format_date_label(date_key)
    return f"{base} ({date_label})" if date_label else base


def _build_intraday_label(underlying: str, date_key: str, time_label: str | None) -> str:
    display = _display_underlying(underlying)
    date_label = _format_date_label(date_key)
    parts = [part for part in [date_label, time_label] if part]
    suffix = f" ({' '.join(parts)})" if parts else ""
    return f"{display} intraday{suffix}"


def _build_matchup_label(team_a: str, team_b: str, date_key: str) -> str:
    base = f"{team_a.title()} vs {team_b.title()}"
    date_label = _format_date_label(date_key)
    return f"{base} ({date_label})" if date_label else base


def _build_generic_label(normalized: str) -> str:
    tokens = normalized.split()
    label_tokens = tokens[:7]
    if not label_tokens:
        return "Market theme"
    return " ".join(label_tokens).title()


def _build_matchup_labels(
    title: str,
    team_a: str,
    team_b: str,
    date_key: str,
) -> tuple[str, str]:
    base = f"{team_a.title()} vs {team_b.title()}"
    detail = _extract_matchup_detail(title)
    if detail:
        label = f"{base} - {detail}"
        date_label = _format_date_label(date_key)
        if date_label:
            label = f"{label} ({date_label})"
        return label, f"{base} - {detail}"
    return _build_matchup_label(team_a, team_b, date_key), base


def _extract_matchup_detail(title: str) -> str | None:
    total_match = re.search(r"\btotal\s+(\d+(?:\.\d+)?)\b", title, flags=re.IGNORECASE)
    if total_match:
        return f"Total {total_match.group(1)}"
    ou_match = re.search(
        r"\b(?:over under|over/under|o/u|ou|o u)\s+(\d+(?:\.\d+)?)\b",
        title,
        flags=re.IGNORECASE,
    )
    if ou_match:
        return f"Total {ou_match.group(1)}"
    short_ou_match = re.search(r"\b[ou]\s+(\d+(?:\.\d+)?)\b", title, flags=re.IGNORECASE)
    if short_ou_match:
        return f"Total {short_ou_match.group(1)}"
    spread_match = re.search(
        r"\b(?:spread|line)\s*([+-]?\d+(?:\.\d+)?)\b",
        title,
        flags=re.IGNORECASE,
    )
    if spread_match:
        return f"Spread {spread_match.group(1)}"
    return None


def _build_will_label(title: str) -> str | None:
    cleaned = re.sub(r"^\s*will\s+", "", title, flags=re.IGNORECASE).strip()
    cleaned = cleaned.rstrip(" ?")
    if not cleaned:
        return None
    normalized = normalize_text(cleaned)
    comparator_map = {
        "less than": "<",
        "under": "<",
        "below": "<",
        "over": ">",
        "above": ">",
        "more than": ">",
        "greater than": ">",
        "at least": ">=",
        "at most": "<=",
    }
    comparator = None
    amount = None
    matched_phrase = None
    for phrase, symbol in comparator_map.items():
        match = re.search(rf"\b{re.escape(phrase)}\s+\$?(\d+(?:\.\d+)?)([kmb])?\b", normalized)
        if match:
            comparator = symbol
            amount = match.group(1)
            suffix = match.group(2) or ""
            matched_phrase = phrase
            break
    label = cleaned
    if comparator and amount:
        prefix = cleaned
        if matched_phrase and matched_phrase in normalized:
            prefix = re.sub(
                rf"\b{re.escape(matched_phrase)}\s+\$?\d+(?:\.\d+)?[kmb]?\b",
                "",
                cleaned,
                flags=re.IGNORECASE,
            )
        prefix = prefix.strip(" ,;-")
        prefix = re.sub(r"\b(be|reach|gross|earn|hit)\b\s*$", "", prefix, flags=re.IGNORECASE).strip()
        value = f"${amount}{suffix}" if "$" in cleaned or "usd" in normalized or "dollar" in normalized else f"{amount}{suffix}"
        label = f"{prefix} {comparator} {value}".strip()
    return _truncate_label(_title_case_label(label), 40)


def _title_case_label(text: str) -> str:
    tokens = text.split()
    return " ".join(token if token.isupper() else token.capitalize() for token in tokens)


def _truncate_label(text: str, limit: int) -> str:
    if len(text) <= limit:
        return text
    return text[: limit - 3].rstrip() + "..."


def _format_date_label(date_key: str) -> str | None:
    if not date_key or date_key == "unknown-date":
        return None
    iso_match = re.match(r"(\d{4})-(\d{2})-(\d{2})", date_key)
    if iso_match:
        year, month, day = iso_match.groups()
        month_name = _month_from_number(month)
        if month_name:
            return f"{month_name} {int(day)} {year}"
        return date_key
    match = re.match(r"([a-z]+)-(\d{1,2})(?:-(\d{4}))?", date_key)
    if not match:
        return date_key
    month = match.group(1)
    day = match.group(2)
    year = match.group(3)
    month_short = _MONTHS_SHORT.get(month, month.title())
    if year:
        return f"{month_short} {int(day)} {year}"
    return f"{month_short} {int(day)}"


def _month_from_number(month: str) -> str | None:
    months = [
        "Jan",
        "Feb",
        "Mar",
        "Apr",
        "May",
        "Jun",
        "Jul",
        "Aug",
        "Sep",
        "Oct",
        "Nov",
        "Dec",
    ]
    try:
        idx = int(month) - 1
    except ValueError:
        return None
    if 0 <= idx < 12:
        return months[idx]
    return None


def _display_underlying(underlying: str) -> str:
    return _ASSET_DISPLAY.get(underlying, underlying.title())
