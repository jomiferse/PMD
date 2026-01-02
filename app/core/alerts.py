import html
import json
import logging
from datetime import datetime, timedelta, timezone

import httpx
import redis
from sqlalchemy.orm import Session

from ..models import Alert
from ..settings import settings
from .alert_strength import AlertStrength

logger = logging.getLogger(__name__)
redis_conn = redis.from_url(settings.REDIS_URL)

DIGEST_LAST_SENT_KEY = "alerts:digest:last_sent:{tenant_id}"
DIGEST_LAST_PAYLOAD_KEY = "alerts:last_digest:{tenant_id}"


async def send_telegram_digest(db: Session, tenant_id: str) -> dict | None:
    if not settings.TELEGRAM_BOT_TOKEN or not settings.TELEGRAM_CHAT_ID:
        return

    window_minutes = max(settings.DIGEST_WINDOW_MINUTES, 1)
    now_ts = datetime.now(timezone.utc)
    if _digest_recently_sent(tenant_id, now_ts, window_minutes):
        return

    window_start = now_ts - timedelta(minutes=window_minutes)
    rows = (
        db.query(Alert)
        .filter(Alert.tenant_id == tenant_id, Alert.created_at >= window_start)
        .all()
    )
    if not rows:
        return

    strong_alerts = [a for a in rows if a.strength == AlertStrength.STRONG.value]
    medium_alerts = [a for a in rows if a.strength == AlertStrength.MEDIUM.value]
    if not strong_alerts:
        return

    strong_ranked = _rank_alerts(strong_alerts)[: settings.MAX_STRONG_ALERTS]
    medium_ranked = _rank_alerts(medium_alerts)[: settings.MAX_MEDIUM_ALERTS]

    text = _format_digest_message(
        strong_ranked,
        medium_ranked,
        window_minutes,
        total_strong=len(strong_alerts),
        total_medium=len(medium_alerts),
    )
    if not text:
        return

    url = f"https://api.telegram.org/bot{settings.TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": settings.TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            response = await client.post(url, json=payload)
        if response.is_success:
            _record_digest_sent(tenant_id, now_ts, window_minutes, strong_alerts, medium_alerts)
            logger.info(
                "digest_sent window_minutes=%s strong=%s medium=%s",
                window_minutes,
                len(strong_alerts),
                len(medium_alerts),
            )
        else:
            logger.error(
                "telegram_send_failed status=%s body=%s",
                response.status_code,
                response.text[:500],
            )
        return {
            "ok": response.is_success,
            "status_code": response.status_code,
            "text": response.text[:200],
        }
    except Exception:
        logger.exception("telegram_send_failed")
        return {"ok": False, "status_code": 0, "text": ""}


def _digest_recently_sent(tenant_id: str, now_ts: datetime, window_minutes: int) -> bool:
    key = DIGEST_LAST_SENT_KEY.format(tenant_id=tenant_id)
    try:
        last_sent_raw = redis_conn.get(key)
        if not last_sent_raw:
            return False
        last_sent = datetime.fromisoformat(last_sent_raw.decode())
        return (now_ts - last_sent).total_seconds() < window_minutes * 60
    except Exception:
        logger.exception("digest_last_sent_lookup_failed")
        return False


def _record_digest_sent(
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
    last_sent_key = DIGEST_LAST_SENT_KEY.format(tenant_id=tenant_id)
    last_payload_key = DIGEST_LAST_PAYLOAD_KEY.format(tenant_id=tenant_id)
    try:
        redis_conn.set(last_sent_key, sent_at.isoformat())
        redis_conn.set(last_payload_key, json.dumps(payload, ensure_ascii=True))
    except Exception:
        logger.exception("digest_state_write_failed")


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


def _format_digest_message(
    strong_alerts: list[Alert],
    medium_alerts: list[Alert],
    window_minutes: int,
    total_strong: int,
    total_medium: int,
) -> str:
    if not strong_alerts:
        return ""

    header = f"<b>PMD - Market Dislocation Digest ({window_minutes}m)</b>"
    summary = f"{total_strong} strong dislocations detected in the last {window_minutes} minutes"
    lines = [header, summary, ""]

    lines.append("<b>STRONG SIGNALS</b>")
    for alert in strong_alerts:
        title = html.escape(alert.title[:120])
        raw_delta_pct = alert.delta_pct if alert.delta_pct else alert.move
        delta_pct = raw_delta_pct * 100
        is_up = alert.new_price >= alert.old_price
        direction = "📈" if is_up else "📉"
        sign = "+" if is_up else "-"
        lines.extend(
            [
                f"{direction} {title}",
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
