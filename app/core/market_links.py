from collections.abc import Iterable
from typing import Any
from urllib.parse import quote

from sqlalchemy import inspect
from sqlalchemy.orm import Session
from sqlalchemy.exc import ProgrammingError

from ..models import MarketSnapshot

_HAS_MARKET_SLUG_COLUMN: dict[str, bool] = {}


def normalize_slug(raw: Any) -> str | None:
    """
    Normalize a potential slug. Reject empty or purely numeric values so we
    don't treat opaque ids as human-friendly slugs.
    """
    if raw is None:
        return None
    slug = str(raw).strip()
    if not slug:
        return None
    if slug.isdigit():
        return None
    if not any(ch.isalpha() for ch in slug) and "-" not in slug:
        return None
    return slug


def market_url(market_id: str, slug: str | None) -> str:
    target = normalize_slug(slug) or str(market_id or "").strip()
    safe = quote(target)
    return f"https://polymarket.com/market/{safe}"


def _has_market_slug_column(db: Session) -> bool:
    # Cache result per database bind to avoid cross-test contamination.
    bind = db.get_bind()
    cache_key = f"{getattr(bind, 'url', '')}:{id(bind)}"
    cached = _HAS_MARKET_SLUG_COLUMN.get(cache_key)
    if cached is not None:
        return cached
    try:
        inspector = inspect(bind)
        cols = inspector.get_columns("market_snapshots")
        _HAS_MARKET_SLUG_COLUMN[cache_key] = any(col.get("name") == "slug" for col in cols)
    except Exception:
        _HAS_MARKET_SLUG_COLUMN[cache_key] = False
    return _HAS_MARKET_SLUG_COLUMN[cache_key]


def fetch_market_slugs(db: Session, market_ids: set[str]) -> dict[str, str | None]:
    if not market_ids:
        return {}
    if not _has_market_slug_column(db):
        return {}
    try:
        rows = (
            db.query(MarketSnapshot.market_id, MarketSnapshot.slug, MarketSnapshot.snapshot_bucket)
            .filter(MarketSnapshot.market_id.in_(market_ids))
            .order_by(MarketSnapshot.market_id.asc(), MarketSnapshot.snapshot_bucket.desc())
            .all()
        )
    except ProgrammingError:
        # Column is missing; mark cache and skip slug attachment.
        bind = db.get_bind()
        cache_key = f"{getattr(bind, 'url', '')}:{id(bind)}"
        _HAS_MARKET_SLUG_COLUMN[cache_key] = False
        return {}
    slugs: dict[str, str | None] = {}
    for market_id, slug, _ in rows:
        normalized = normalize_slug(slug)
        if market_id not in slugs or (normalized and not slugs.get(market_id)):
            slugs[market_id] = normalized
    return slugs


def attach_market_slugs(db: Session, alerts: Iterable[Any]) -> dict[str, str | None]:
    alert_list = [alert for alert in alerts if getattr(alert, "market_id", None)]
    market_ids = {str(alert.market_id) for alert in alert_list if getattr(alert, "market_id", None)}
    slug_map = fetch_market_slugs(db, market_ids)
    for alert in alert_list:
        try:
            setattr(alert, "market_slug", slug_map.get(str(alert.market_id)))
        except Exception:
            continue
    return slug_map
