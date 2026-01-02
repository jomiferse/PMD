import html
import logging
from datetime import datetime, timezone

import httpx
import redis

from ..models import Alert
from ..settings import settings

logger = logging.getLogger(__name__)
redis_conn = redis.from_url(settings.REDIS_URL)


async def send_telegram_alerts(alerts: list[Alert]) -> dict | None:
    if not settings.TELEGRAM_BOT_TOKEN or not settings.TELEGRAM_CHAT_ID:
        return
    if not alerts:
        return

    filtered: list[Alert] = []
    for alert in alerts:
        if _throttled(alert):
            continue
        filtered.append(alert)

    if not filtered:
        return

    # Rank by absolute move, then liquidity, to surface the most important alerts first.
    filtered.sort(
        key=lambda alert: (abs(alert.new_price - alert.old_price), alert.liquidity),
        reverse=True,
    )
    total_filtered = len(filtered)
    max_alerts = max(settings.TELEGRAM_MAX_ALERTS, 1)
    top_alerts = filtered[:max_alerts]

    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    lines = [
        f"<b>📈 PMD Dislocation Alerts — Last {settings.WINDOW_MINUTES}m</b>",
        f"<i>{timestamp}</i>",
        f"<i>{total_filtered} markets moved significantly in the last {settings.WINDOW_MINUTES}m</i>",
        "",
    ]
    for alert in top_alerts:
        title = html.escape(alert.title[:120])
        raw_delta_pct = alert.delta_pct if alert.delta_pct else alert.move
        delta_pct = raw_delta_pct * 100
        is_up = alert.new_price >= alert.old_price
        direction = "📈" if is_up else "📉"
        sign = "+" if is_up else "-"
        strong_signal = "🔥 " if raw_delta_pct >= settings.TELEGRAM_STRONG_MOVE_PCT else ""
        lines.extend(
            [
                f"{strong_signal}<b>{title}</b>",
                f"{direction} {sign}{delta_pct:.1f}% ({alert.old_price:.2f} → {alert.new_price:.2f})",
                f"💧 Liquidity: {_format_compact_usd(alert.liquidity)} | Vol24h: {_format_compact_usd(alert.volume_24h)}",
                "",
            ]
        )

    lines.append("<i>Read-only analytics • Not financial advice</i>")
    text = "\n".join(lines).strip()
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
        if not response.is_success:
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


def _throttled(alert: Alert) -> bool:
    key = f"alert:tg:{alert.alert_type}:{alert.market_id}"
    try:
        if redis_conn.get(key):
            return True
        redis_conn.setex(key, settings.TELEGRAM_THROTTLE_SECONDS, "1")
        return False
    except Exception:
        logger.exception("alert_throttle_failed")
        return False


def _format_compact_usd(value: float) -> str:
    abs_value = abs(value)
    if abs_value >= 1_000_000_000:
        return f"${value / 1_000_000_000:.2f}B"
    if abs_value >= 1_000_000:
        return f"${value / 1_000_000:.2f}M"
    if abs_value >= 1_000:
        return f"${value / 1_000:.1f}k"
    return f"${value:,.0f}"
