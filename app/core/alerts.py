import html
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from urllib.parse import quote
from uuid import UUID

import httpx
import redis
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from ..models import Alert, AlertDelivery, User, UserAlertPreference
from ..settings import settings
from .alert_strength import AlertStrength
from .alert_classification import AlertClassification, classify_alert, classify_alert_with_snapshots

logger = logging.getLogger(__name__)
redis_conn = redis.from_url(settings.REDIS_URL)

USER_DIGEST_LAST_SENT_KEY = "alerts:digest:last_sent:user:{user_id}"
USER_DIGEST_LAST_PAYLOAD_KEY = "alerts:last_digest:user:{user_id}"
TENANT_DIGEST_LAST_PAYLOAD_KEY = "alerts:last_digest:{tenant_id}"

DELIVERY_STATUS_SENT = "sent"
DELIVERY_STATUS_SKIPPED = "skipped"
DELIVERY_STATUS_FILTERED = "filtered"


@dataclass(frozen=True)
class UserDigestConfig:
    user_id: UUID
    name: str
    telegram_chat_id: str | None
    min_liquidity: float
    min_volume_24h: float
    min_abs_price_move: float
    alert_strengths: set[str]
    digest_window_minutes: int
    max_alerts_per_digest: int


async def send_user_digests(db: Session, tenant_id: str) -> dict | None:
    if not settings.TELEGRAM_BOT_TOKEN:
        return {"ok": False, "reason": "telegram_disabled"}

    users = db.query(User).filter(User.is_active.is_(True)).all()
    if not users:
        return {"ok": True, "users": 0, "sent": 0}

    prefs = _load_user_preferences(db, users)
    results = []
    sent_count = 0
    for user in users:
        config = _resolve_user_preferences(user, prefs.get(user.user_id))
        result = await _send_user_digest(db, tenant_id, config)
        results.append(result)
        if result.get("sent"):
            sent_count += 1

    return {"ok": True, "users": len(users), "sent": sent_count, "results": results}


def _load_user_preferences(
    db: Session,
    users: list[User],
) -> dict[UUID, UserAlertPreference]:
    user_ids = [user.user_id for user in users]
    if not user_ids:
        return {}
    rows = (
        db.query(UserAlertPreference)
        .filter(UserAlertPreference.user_id.in_(user_ids))
        .all()
    )
    return {row.user_id: row for row in rows}


def _resolve_user_preferences(
    user: User,
    pref: UserAlertPreference | None,
) -> UserDigestConfig:
    min_liquidity = pref.min_liquidity if pref and pref.min_liquidity is not None else settings.GLOBAL_MIN_LIQUIDITY
    min_volume_24h = (
        pref.min_volume_24h if pref and pref.min_volume_24h is not None else settings.GLOBAL_MIN_VOLUME_24H
    )
    min_abs_price_move = (
        pref.min_abs_price_move if pref and pref.min_abs_price_move is not None else settings.MIN_ABS_MOVE
    )
    alert_strengths = _parse_alert_strengths(pref.alert_strengths if pref else None)
    digest_window_minutes = (
        pref.digest_window_minutes if pref and pref.digest_window_minutes is not None else settings.GLOBAL_DIGEST_WINDOW
    )
    max_alerts_per_digest = (
        pref.max_alerts_per_digest if pref and pref.max_alerts_per_digest is not None else settings.GLOBAL_MAX_ALERTS
    )
    return UserDigestConfig(
        user_id=user.user_id,
        name=user.name,
        telegram_chat_id=user.telegram_chat_id,
        min_liquidity=min_liquidity,
        min_volume_24h=min_volume_24h,
        min_abs_price_move=min_abs_price_move,
        alert_strengths=alert_strengths,
        digest_window_minutes=max(int(digest_window_minutes), 1),
        max_alerts_per_digest=max(int(max_alerts_per_digest), 1),
    )


def _parse_alert_strengths(raw: str | None) -> set[str]:
    allowed = {AlertStrength.STRONG.value, AlertStrength.MEDIUM.value}
    if not raw:
        return allowed
    parts = {part.strip().upper() for part in raw.split(",") if part.strip()}
    parsed = {part for part in parts if part in allowed}
    return parsed or {AlertStrength.STRONG.value}


async def _send_user_digest(db: Session, tenant_id: str, config: UserDigestConfig) -> dict:
    window_minutes = max(config.digest_window_minutes, 1)
    now_ts = datetime.now(timezone.utc)

    if _digest_recently_sent(config.user_id, now_ts, window_minutes):
        return {"user_id": str(config.user_id), "sent": False, "reason": "recent_digest"}

    window_start = now_ts - timedelta(minutes=window_minutes)
    rows = (
        db.query(Alert)
        .filter(Alert.tenant_id == tenant_id, Alert.created_at >= window_start)
        .all()
    )
    if not rows:
        return {"user_id": str(config.user_id), "sent": False, "reason": "no_alerts"}

    included_alerts, filtered_out = _filter_alerts_for_user(rows, config)

    pyes_filtered: list[Alert] = []
    pyes_candidates: list[Alert] = []
    for alert in included_alerts:
        if not _is_within_actionable_pyes(alert):
            pyes_filtered.append(alert)
            continue
        pyes_candidates.append(alert)

    if not config.telegram_chat_id:
        _record_alert_deliveries(
            db,
            filtered_out + pyes_filtered,
            pyes_candidates,
            config.user_id,
            now_ts,
            sent_alert_ids=set(),
            skip_reason="missing_chat_id",
        )
        return {"user_id": str(config.user_id), "sent": False, "reason": "missing_chat_id"}

    classification_cache: dict[int, AlertClassification] = {}

    def classifier(alert: Alert) -> AlertClassification:
        key = alert.id if alert.id is not None else id(alert)
        cached = classification_cache.get(key)
        if cached:
            return cached
        classification = classify_alert_with_snapshots(db, alert)
        classification_cache[key] = classification
        return classification

    actionable_alerts: list[Alert] = []
    non_actionable_alerts: list[Alert] = []
    for alert in pyes_candidates:
        classification = classifier(alert)
        if _is_actionable_classification(classification):
            actionable_alerts.append(alert)
        else:
            non_actionable_alerts.append(alert)

    actionable_ranked = _dedupe_by_market_id(_rank_alerts(actionable_alerts))
    total_actionable = len(actionable_ranked)
    filtered_for_delivery = filtered_out + pyes_filtered
    if settings.DIGEST_ACTIONABLE_ONLY:
        filtered_for_delivery += non_actionable_alerts
    included_for_delivery = actionable_alerts if settings.DIGEST_ACTIONABLE_ONLY else pyes_candidates

    if total_actionable < 1:
        _record_alert_deliveries(
            db,
            filtered_for_delivery,
            included_for_delivery,
            config.user_id,
            now_ts,
            sent_alert_ids=set(),
            skip_reason="no_actionable_alerts",
        )
        return {"user_id": str(config.user_id), "sent": False, "reason": "no_actionable_alerts"}

    max_alerts = min(
        max(config.max_alerts_per_digest, 1),
        max(settings.MAX_ACTIONABLE_PER_DIGEST, 1),
    )

    selected_actionable = actionable_ranked[:max_alerts]
    selected_alerts = selected_actionable
    if not settings.DIGEST_ACTIONABLE_ONLY:
        strong_alerts = [a for a in pyes_candidates if a.strength == AlertStrength.STRONG.value]
        medium_alerts = [a for a in pyes_candidates if a.strength == AlertStrength.MEDIUM.value]

        strong_ranked = _dedupe_by_market_id(_rank_alerts(strong_alerts))
        medium_ranked = _dedupe_by_market_id(
            _rank_alerts(medium_alerts),
            exclude_market_ids={alert.market_id for alert in strong_ranked},
        )
        selected_strong = strong_ranked[:max_alerts]
        remaining = max(max_alerts - len(selected_strong), 0)
        selected_medium = medium_ranked[:remaining] if remaining > 0 else []
        selected_alerts = selected_strong + selected_medium

    if not selected_alerts:
        _record_alert_deliveries(
            db,
            filtered_for_delivery,
            included_for_delivery,
            config.user_id,
            now_ts,
            sent_alert_ids=set(),
            skip_reason="no_selected_alerts",
        )
        return {"user_id": str(config.user_id), "sent": False, "reason": "no_selected_alerts"}

    counts = {"REPRICING": 0, "LIQUIDITY_SWEEP": 0, "NOISY": 0}
    for alert in selected_alerts:
        classification = classifier(alert)
        counts[classification.signal_type] = counts.get(classification.signal_type, 0) + 1
    logger.info(
        "alert_classification_summary user_id=%s repricing=%s liquidity_sweep=%s noisy=%s total=%s",
        config.user_id,
        counts.get("REPRICING", 0),
        counts.get("LIQUIDITY_SWEEP", 0),
        counts.get("NOISY", 0),
        len(selected_alerts),
    )

    text = _format_digest_message(
        selected_alerts,
        window_minutes,
        total_actionable=total_actionable,
        user_name=config.name,
        classifier=classifier,
    )
    if not text:
        _record_alert_deliveries(
            db,
            filtered_for_delivery,
            included_for_delivery,
            config.user_id,
            now_ts,
            sent_alert_ids=set(),
            skip_reason="empty_message",
        )
        return {"user_id": str(config.user_id), "sent": False, "reason": "empty_message"}

    url = f"https://api.telegram.org/bot{settings.TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": config.telegram_chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            response = await client.post(url, json=payload)
        if response.is_success:
            _record_digest_sent(config.user_id, tenant_id, now_ts, window_minutes, actionable_alerts, [])
            _record_alert_deliveries(
                db,
                filtered_for_delivery,
                included_for_delivery,
                config.user_id,
                now_ts,
                sent_alert_ids={alert.id for alert in selected_alerts if alert.id is not None},
            )
            logger.info(
                "digest_sent user_id=%s window_minutes=%s actionable=%s",
                config.user_id,
                window_minutes,
                len(actionable_alerts),
            )
            return {"user_id": str(config.user_id), "sent": True, "status_code": response.status_code}

        logger.error(
            "telegram_send_failed user_id=%s status=%s body=%s",
            config.user_id,
            response.status_code,
            response.text[:500],
        )
        _record_alert_deliveries(
            db,
            filtered_for_delivery,
            included_for_delivery,
            config.user_id,
            now_ts,
            sent_alert_ids=set(),
            skip_reason="telegram_failed",
        )
        return {
            "user_id": str(config.user_id),
            "sent": False,
            "status_code": response.status_code,
            "text": response.text[:200],
        }
    except Exception:
        logger.exception("telegram_send_failed user_id=%s", config.user_id)
        _record_alert_deliveries(
            db,
            filtered_for_delivery,
            included_for_delivery,
            config.user_id,
            now_ts,
            sent_alert_ids=set(),
            skip_reason="telegram_exception",
        )
        return {"user_id": str(config.user_id), "sent": False, "status_code": 0, "text": ""}


def _filter_alerts_for_user(
    alerts: list[Alert],
    config: UserDigestConfig,
) -> tuple[list[Alert], list[Alert]]:
    included: list[Alert] = []
    filtered: list[Alert] = []

    for alert in alerts:
        if alert.liquidity < config.min_liquidity:
            filtered.append(alert)
            continue
        if alert.volume_24h < config.min_volume_24h:
            filtered.append(alert)
            continue
        if _alert_abs_move(alert) < config.min_abs_price_move:
            filtered.append(alert)
            continue
        if alert.strength not in config.alert_strengths:
            filtered.append(alert)
            continue
        included.append(alert)

    return included, filtered


def _alert_abs_move(alert: Alert) -> float:
    if alert.old_price is not None and alert.new_price is not None:
        return abs(alert.new_price - alert.old_price)
    if alert.delta_pct is not None:
        return abs(alert.delta_pct)
    return abs(alert.move or 0.0)


def _digest_recently_sent(user_id: UUID, now_ts: datetime, window_minutes: int) -> bool:
    key = USER_DIGEST_LAST_SENT_KEY.format(user_id=user_id)
    try:
        last_sent_raw = redis_conn.get(key)
        if not last_sent_raw:
            return False
        last_sent = datetime.fromisoformat(last_sent_raw.decode())
        return (now_ts - last_sent).total_seconds() < window_minutes * 60
    except Exception:
        logger.exception("digest_last_sent_lookup_failed user_id=%s", user_id)
        return False


def _record_digest_sent(
    user_id: UUID,
    tenant_id: str,
    sent_at: datetime,
    window_minutes: int,
    strong_alerts: list[Alert],
    medium_alerts: list[Alert],
) -> None:
    payload = {
        "sent_at": sent_at.isoformat(),
        "window_minutes": window_minutes,
        "strong": len(strong_alerts),
        "medium": len(medium_alerts),
    }
    last_sent_key = USER_DIGEST_LAST_SENT_KEY.format(user_id=user_id)
    last_payload_key = USER_DIGEST_LAST_PAYLOAD_KEY.format(user_id=user_id)
    tenant_payload_key = TENANT_DIGEST_LAST_PAYLOAD_KEY.format(tenant_id=tenant_id)
    try:
        redis_conn.set(last_sent_key, sent_at.isoformat())
        redis_conn.set(last_payload_key, json.dumps(payload, ensure_ascii=True))
        redis_conn.set(tenant_payload_key, json.dumps(payload, ensure_ascii=True))
    except Exception:
        logger.exception("digest_state_write_failed user_id=%s", user_id)


def _record_alert_deliveries(
    db: Session,
    filtered_alerts: list[Alert],
    included_alerts: list[Alert],
    user_id: UUID,
    delivered_at: datetime,
    sent_alert_ids: set[int],
    skip_reason: str | None = None,
) -> None:
    rows: list[dict] = []
    for alert in filtered_alerts:
        if alert.id is None:
            continue
        rows.append(
            {
                "alert_id": alert.id,
                "user_id": user_id,
                "delivered_at": delivered_at,
                "delivery_status": DELIVERY_STATUS_FILTERED,
            }
        )

    for alert in included_alerts:
        if alert.id is None:
            continue
        status = DELIVERY_STATUS_SENT if alert.id in sent_alert_ids else DELIVERY_STATUS_SKIPPED
        rows.append(
            {
                "alert_id": alert.id,
                "user_id": user_id,
                "delivered_at": delivered_at,
                "delivery_status": status,
            }
        )

    if not rows:
        return

    if skip_reason:
        logger.info("digest_delivery_skipped user_id=%s reason=%s", user_id, skip_reason)

    stmt = pg_insert(AlertDelivery).values(rows)
    stmt = stmt.on_conflict_do_update(
        index_elements=["alert_id", "user_id"],
        set_={
            "delivered_at": delivered_at,
            "delivery_status": stmt.excluded.delivery_status,
        },
    )
    db.execute(stmt)
    db.commit()


def _rank_alerts(alerts: list[Alert]) -> list[Alert]:
    return sorted(
        alerts,
        key=lambda alert: (
            abs(alert.new_price - alert.old_price),
            alert.liquidity,
            alert.volume_24h,
        ),
        reverse=True,
    )


def _dedupe_by_market_id(
    alerts: list[Alert],
    exclude_market_ids: set[str] | None = None,
) -> list[Alert]:
    seen: set[str] = set(exclude_market_ids or set())
    deduped: list[Alert] = []
    for alert in alerts:
        if alert.market_id in seen:
            continue
        seen.add(alert.market_id)
        deduped.append(alert)
    return deduped


def _format_digest_message(
    alerts: list[Alert],
    window_minutes: int,
    total_actionable: int,
    user_name: str | None = None,
    classifier=None,
) -> str:
    if total_actionable < 1:
        return ""

    header = f"<b>PMD — {total_actionable} actionable repricings ({window_minutes}m)</b>"
    lines = [header, ""]

    actionable_displayed = 0
    for idx, alert in enumerate(alerts, start=1):
        classification = classifier(alert) if classifier else classify_alert(alert)
        if _is_actionable_classification(classification):
            actionable_displayed += 1
        lines.extend(_format_digest_alert(alert, idx, window_minutes, classifier, classification=classification))
        lines.append("")

    remaining_actionable = total_actionable - actionable_displayed
    if remaining_actionable > 0:
        lines.extend([f"+{remaining_actionable} more actionable repricings not shown", ""])

    lines.append("<i>Read-only analytics • Not financial advice</i>")
    return "\n".join(lines).strip()


def _format_digest_alert(
    alert: Alert,
    idx: int,
    window_minutes: int,
    classifier=None,
    classification: AlertClassification | None = None,
) -> list[str]:
    title = html.escape(alert.title[:120])
    classification = classification or (classifier(alert) if classifier else classify_alert(alert))
    rank_label = "STRONG" if _is_actionable_classification(classification) else "NOTABLE"
    move_text = _format_move(alert)
    p_yes_text = _format_p_yes(alert)
    liquidity_text = _format_liquidity_volume(alert)
    return [
        f"<b>#{idx} {rank_label} - {classification.signal_type} ({classification.confidence})</b>",
        title,
        f"Move: {move_text} | {p_yes_text}",
        liquidity_text,
        f"Suggested action: {classification.suggested_action}",
        _format_market_link(alert.market_id),
    ]


def _format_compact_usd(value: float) -> str:
    abs_value = abs(value)
    if abs_value >= 1_000_000_000:
        return f"${value / 1_000_000_000:.2f}B"
    if abs_value >= 1_000_000:
        return f"${value / 1_000_000:.2f}M"
    if abs_value >= 1_000:
        return f"${value / 1_000:.1f}k"
    return f"${value:,.0f}"


def _rank_digest_alerts(alerts: list[Alert]) -> list[Alert]:
    def _strength_weight(alert: Alert) -> int:
        return 1 if alert.strength == AlertStrength.STRONG.value else 0

    return sorted(
        alerts,
        key=lambda alert: (
            _strength_weight(alert),
            abs(_signed_price_delta(alert)),
            alert.liquidity,
            alert.volume_24h,
        ),
        reverse=True,
    )


def _is_actionable_classification(classification: AlertClassification) -> bool:
    return (
        classification.signal_type == "REPRICING"
        and classification.confidence == "HIGH"
        and classification.suggested_action == "FOLLOW"
    )


def _is_within_actionable_pyes(alert: Alert) -> bool:
    if alert.market_p_yes is None:
        return True
    return settings.PYES_ACTIONABLE_MIN <= alert.market_p_yes <= settings.PYES_ACTIONABLE_MAX


def _signed_price_delta(alert: Alert) -> float:
    if alert.new_price is not None and alert.old_price is not None:
        return alert.new_price - alert.old_price
    return alert.move or 0.0


def _format_move(alert: Alert) -> str:
    signed_delta = _signed_price_delta(alert)
    abs_move = abs(signed_delta)
    raw_delta_pct = alert.delta_pct if alert.delta_pct is not None else alert.move
    delta_pct = abs((raw_delta_pct or 0.0) * 100)
    sign = "+" if signed_delta >= 0 else "-"
    return f"{sign}{abs_move:.3f} ({sign}{delta_pct:.1f}%)"


def _format_p_yes(alert: Alert) -> str:
    new_value = alert.market_p_yes
    if new_value is None:
        return "p_yes now: n/a"
    new_text = f"{new_value * 100:.1f}%"
    if alert.prev_market_p_yes is not None:
        prev_text = f"{alert.prev_market_p_yes * 100:.1f}%"
        return f"p_yes: {prev_text} -> {new_text}"
    return f"p_yes now: {new_text}"


def _format_liquidity_volume(alert: Alert) -> str:
    liq_descriptor = _descriptor_from_thresholds(
        alert.liquidity,
        settings.STRONG_MIN_LIQUIDITY,
        settings.GLOBAL_MIN_LIQUIDITY,
    )
    vol_descriptor = _descriptor_from_thresholds(
        alert.volume_24h,
        settings.STRONG_MIN_VOLUME_24H,
        settings.GLOBAL_MIN_VOLUME_24H,
    )
    return f"Liquidity: {liq_descriptor} | Volume: {vol_descriptor}"


def _descriptor_from_thresholds(value: float, high: float, moderate: float) -> str:
    if value >= high:
        return "High"
    if value >= moderate:
        return "Moderate"
    return "Light"


def _format_market_link(market_id: str) -> str:
    safe_id = quote(str(market_id).strip())
    return f"https://polymarket.com/market/{safe_id}"
