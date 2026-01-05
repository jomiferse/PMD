from datetime import datetime, timedelta, timezone

from sqlalchemy.orm import Session

from ..models import Alert, MarketSnapshot
from .alert_strength import AlertStrength

ALERT_TYPE = "DISLOCATION"


def compute_dislocation_alerts(
    db: Session,
    snapshots: list[dict],
    window_minutes: int,
    medium_move_threshold: float,
    min_price_threshold: float,
    medium_abs_move_threshold: float,
    floor_price: float,
    medium_min_liquidity: float,
    medium_min_volume_24h: float,
    strong_abs_move_threshold: float,
    strong_min_liquidity: float,
    strong_min_volume_24h: float,
    cooldown_minutes: int,
    tenant_id: str,
    use_triggered_at: bool = True,
) -> list[Alert]:
    if not snapshots:
        return []

    window_start = datetime.now(timezone.utc) - timedelta(minutes=window_minutes)
    alerts: list[Alert] = []

    now_ts = datetime.now(timezone.utc)
    cooldown_start = now_ts - timedelta(minutes=cooldown_minutes)
    seen_market_ids: set[str] = set()
    market_ids = {snap.get("market_id") for snap in snapshots if snap.get("market_id")}
    prev_snapshots = _load_prev_snapshots(db, market_ids, window_start)
    cooldown_field = Alert.triggered_at if use_triggered_at else Alert.created_at
    recent_markets = _load_recent_alert_markets(
        db,
        market_ids,
        tenant_id,
        cooldown_field,
        cooldown_start,
    )

    for snap in snapshots:
        # Skip illiquid markets to avoid noisy, low-signal moves.
        if snap["liquidity"] < medium_min_liquidity:
            continue
        # Require minimum trading activity for meaningful alerts.
        if snap["volume_24h"] < medium_min_volume_24h:
            continue
        if snap["market_id"] in seen_market_ids:
            continue

        prev = _pick_prev_snapshot(
            prev_snapshots,
            snap["market_id"],
            snap["snapshot_bucket"],
        )
        if not prev:
            continue

        prev_bucket, prev_price = prev
        if prev_price <= 0:
            continue

        old_price = prev_price
        new_price = snap["market_p_yes"]
        # Skip unchanged prices so % math can't create phantom moves.
        if old_price == new_price:
            continue
        # Avoid micro-priced markets where % moves are misleading.
        if old_price < min_price_threshold and new_price < min_price_threshold:
            continue

        abs_move = abs(new_price - old_price)
        # Require a real absolute move to filter out tiny jitter.
        if abs_move < medium_abs_move_threshold:
            continue

        # Use a floor to prevent % explosions on tiny bases.
        delta_pct = abs_move / max(old_price, floor_price)
        if delta_pct < medium_move_threshold:
            continue

        if snap["market_id"] in recent_markets:
            continue

        strength = AlertStrength.MEDIUM
        if (
            abs_move >= strong_abs_move_threshold
            and snap["liquidity"] >= strong_min_liquidity
            and snap["volume_24h"] >= strong_min_volume_24h
        ):
            strength = AlertStrength.STRONG

        message = f"Dislocation {delta_pct * 100:.1f}% over {window_minutes}m"
        alerts.append(
            Alert(
                tenant_id=tenant_id,
                alert_type=ALERT_TYPE,
                market_id=snap["market_id"],
                title=snap["title"],
                category=snap["category"],
                move=delta_pct,
                market_p_yes=new_price,
                prev_market_p_yes=old_price,
                primary_outcome_label=snap.get("primary_outcome_label"),
                is_yesno=snap.get("is_yesno"),
                mapping_confidence=snap.get("mapping_confidence"),
                market_kind=snap.get("market_kind"),
                old_price=old_price,
                new_price=new_price,
                delta_pct=delta_pct,
                liquidity=snap["liquidity"],
                volume_24h=snap["volume_24h"],
                strength=strength.value,
                snapshot_bucket=snap["snapshot_bucket"],
                source_ts=snap["source_ts"],
                message=message,
                triggered_at=now_ts,
                created_at=now_ts,
            )
        )
        seen_market_ids.add(snap["market_id"])

    return alerts


def _load_prev_snapshots(
    db: Session,
    market_ids: set[str],
    window_start: datetime,
) -> dict[str, list[tuple[datetime, float]]]:
    if not market_ids:
        return {}
    rows = (
        db.query(
            MarketSnapshot.market_id,
            MarketSnapshot.snapshot_bucket,
            MarketSnapshot.market_p_yes,
        )
        .filter(
            MarketSnapshot.market_id.in_(market_ids),
            MarketSnapshot.snapshot_bucket >= window_start,
        )
        .order_by(MarketSnapshot.market_id.asc(), MarketSnapshot.snapshot_bucket.asc())
        .all()
    )
    snapshots_by_market: dict[str, list[tuple[datetime, float]]] = {}
    for market_id, bucket, price in rows:
        if price is None:
            continue
        snapshots_by_market.setdefault(market_id, []).append((bucket, price))
    return snapshots_by_market


def _pick_prev_snapshot(
    snapshots_by_market: dict[str, list[tuple[datetime, float]]],
    market_id: str,
    current_bucket: datetime,
) -> tuple[datetime, float] | None:
    current_bucket = _ensure_aware_utc(current_bucket)
    for bucket, price in snapshots_by_market.get(market_id, []):
        bucket = _ensure_aware_utc(bucket)
        if bucket < current_bucket:
            return bucket, price
    return None


def _ensure_aware_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.tzinfo.utcoffset(value) is None:
        return value.replace(tzinfo=timezone.utc)
    return value


def _load_recent_alert_markets(
    db: Session,
    market_ids: set[str],
    tenant_id: str,
    cooldown_field,
    cooldown_start: datetime,
) -> set[str]:
    if not market_ids:
        return set()
    rows = (
        db.query(Alert.market_id)
        .filter(
            Alert.tenant_id == tenant_id,
            Alert.alert_type == ALERT_TYPE,
            Alert.market_id.in_(market_ids),
            cooldown_field >= cooldown_start,
        )
        .distinct()
        .all()
    )
    return {row[0] for row in rows}
