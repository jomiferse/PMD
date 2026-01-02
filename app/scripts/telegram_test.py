import asyncio
from datetime import datetime, timezone

from app.core.alerts import send_telegram_alerts
from app.logging import configure_logging
from app.models import Alert
from app.settings import settings
import logging

logger = logging.getLogger(__name__)


async def main() -> None:
    configure_logging()
    logger.info("telegram_test_start")
    alert = Alert(
        tenant_id=settings.DEFAULT_TENANT_ID,
        alert_type="dislocation",
        market_id="test",
        title="PMD: Telegram connected",
        category="setup",
        move=0.1,
        market_p_yes=0.5,
        prev_market_p_yes=0.4,
        liquidity=12345,
        volume_24h=54321,
        snapshot_bucket=datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0),
        source_ts=datetime.now(timezone.utc),
        message="Test alert",
        created_at=datetime.now(timezone.utc),
    )
    result = await send_telegram_alerts([alert])
    logger.info("telegram_test_sent result=%s", result)


if __name__ == "__main__":
    asyncio.run(main())
