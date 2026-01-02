import html
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from uuid import UUID

import httpx
import redis
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from ..models import Alert, AlertDelivery, User, UserAlertPreference
from ..settings import settings
from .alert_strength import AlertStrength

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
    strong_alerts = [a for a in included_alerts if a.strength == AlertStrength.STRONG.value]
    medium_alerts = [a for a in included_alerts if a.strength == AlertStrength.MEDIUM.value]

    strong_ranked = _dedupe_by_market_id(_rank_alerts(strong_alerts))
    medium_ranked = _dedupe_by_market_id(
        _rank_alerts(medium_alerts),
        exclude_market_ids={alert.market_id for alert in strong_ranked},
    )

    if not strong_ranked:
        _record_alert_deliveries(
            db,
            filtered_out,
            included_alerts,
            config.user_id,
            now_ts,
            sent_alert_ids=set(),
            skip_reason="no_strong_alerts",
        )
        return {"user_id": str(config.user_id), "sent": False, "reason": "no_strong_alerts"}

    selected_strong = strong_ranked[: config.max_alerts_per_digest]
    remaining = max(config.max_alerts_per_digest - len(selected_strong), 0)
    selected_medium = medium_ranked[:remaining] if remaining > 0 else []
    selected_alerts = selected_strong + selected_medium

    if not selected_alerts:
        _record_alert_deliveries(
            db,
            filtered_out,
            included_alerts,
            config.user_id,
            now_ts,
            sent_alert_ids=set(),
            skip_reason="no_selected_alerts",
        )
        return {"user_id": str(config.user_id), "sent": False, "reason": "no_selected_alerts"}

    if not config.telegram_chat_id:
        _record_alert_deliveries(
            db,
            filtered_out,
            included_alerts,
            config.user_id,
            now_ts,
            sent_alert_ids=set(),
            skip_reason="missing_chat_id",
        )
        return {"user_id": str(config.user_id), "sent": False, "reason": "missing_chat_id"}

    text = _format_digest_message(
        selected_strong,
        selected_medium,
        window_minutes,
        total_strong=len(strong_alerts),
        total_medium=len(medium_alerts),
        user_name=config.name,
    )
    if not text:
        _record_alert_deliveries(
            db,
            filtered_out,
            included_alerts,
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
            _record_digest_sent(config.user_id, tenant_id, now_ts, window_minutes, strong_alerts, medium_alerts)
            _record_alert_deliveries(
                db,
                filtered_out,
                included_alerts,
                config.user_id,
                now_ts,
                sent_alert_ids={alert.id for alert in selected_alerts if alert.id is not None},
            )
            logger.info(
                "digest_sent user_id=%s window_minutes=%s strong=%s medium=%s",
                config.user_id,
                window_minutes,
                len(strong_alerts),
                len(medium_alerts),
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
            filtered_out,
            included_alerts,
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
            filtered_out,
            included_alerts,
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
    strong_alerts: list[Alert],
    medium_alerts: list[Alert],
    window_minutes: int,
    total_strong: int,
    total_medium: int,
    user_name: str | None = None,
) -> str:
    if not strong_alerts:
        return ""

    name_suffix = f" for {html.escape(user_name)}" if user_name else ""
    header = f"<b>PMD Digest{name_suffix} - last {window_minutes} minutes</b>"
    summary = f"{total_strong} strong dislocations detected in the last {window_minutes} minutes"
    lines = [header, summary, ""]

    lines.append("<b>STRONG SIGNALS</b>")
    for alert in strong_alerts:
        title = html.escape(alert.title[:120])
        raw_delta_pct = alert.delta_pct if alert.delta_pct is not None else alert.move
        delta_pct = (raw_delta_pct or 0.0) * 100
        is_up = alert.new_price >= alert.old_price
        direction = "UP" if is_up else "DOWN"
        sign = "+" if is_up else "-"
        lines.extend(
            [
                f"{direction}: {title}",
                f"{alert.old_price:.2f} -> {alert.new_price:.2f} ({sign}{delta_pct:.1f}%)",
                f"Liquidity: {_format_compact_usd(alert.liquidity)} | Vol24h: {_format_compact_usd(alert.volume_24h)}",
                "",
            ]
        )

    if medium_alerts:
        lines.append("<b>OTHER NOTABLE MOVES</b>")
        for alert in medium_alerts:
            title = html.escape(alert.title[:120])
            lines.append(f"- {title}")
        lines.append("")

    lines.append("<i>Read-only analytics - No execution - Not financial advice</i>")
    return "\n".join(lines).strip()


def _format_compact_usd(value: float) -> str:
    abs_value = abs(value)
    if abs_value >= 1_000_000_000:
        return f"${value / 1_000_000_000:.2f}B"
    if abs_value >= 1_000_000:
        return f"${value / 1_000_000:.2f}M"
    if abs_value >= 1_000:
        return f"${value / 1_000:.1f}k"
    return f"${value:,.0f}"
