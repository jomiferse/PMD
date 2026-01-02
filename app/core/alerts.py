import logging

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
        if len(filtered) >= 10:
            break

    if not filtered:
        return

    lines = ["PMD alerts (analytics only)"]
    for alert in filtered:
        lines.append(
            f"- {alert.title[:60]} | p_yes={alert.market_p_yes:.2f} "
            f"move={alert.move:.2f} liq={alert.liquidity:.0f} vol24h={alert.volume_24h:.0f}"
        )

    text = "\n".join(lines)
    url = f"https://api.telegram.org/bot{settings.TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": settings.TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "Markdown",
        "disable_web_page_preview": True,
    }
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            response = await client.post(url, json=payload)
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
