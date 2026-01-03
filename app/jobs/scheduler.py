import time
import logging
from datetime import datetime, timezone

import redis
from rq import Queue

from ..settings import settings
from ..logging import configure_logging
from .run import job_sync_wrapper, cleanup_sync_wrapper

logger = logging.getLogger(__name__)
CLEANUP_LAST_DATE_KEY = "cleanup:last_date"


def _maybe_enqueue_cleanup(queue: Queue, redis_conn, now_ts: datetime) -> None:
    if not settings.CLEANUP_ENABLED:
        return
    if now_ts.hour < settings.CLEANUP_SCHEDULE_HOUR_UTC:
        return
    last_date_raw = redis_conn.get(CLEANUP_LAST_DATE_KEY)
    today = now_ts.date().isoformat()
    if last_date_raw and last_date_raw.decode() == today:
        return
    job = queue.enqueue(cleanup_sync_wrapper)
    redis_conn.set(CLEANUP_LAST_DATE_KEY, today)
    logger.info("cleanup_enqueued id=%s", job.id)


def main() -> None:
    configure_logging()
    redis_conn = redis.from_url(settings.REDIS_URL)
    queue = Queue("default", connection=redis_conn)
    interval = max(30, settings.INGEST_INTERVAL_SECONDS)

    while True:
        try:
            count = queue.count if isinstance(queue.count, int) else queue.count()
            if count == 0:
                job = queue.enqueue(job_sync_wrapper)
                logger.info("ingest_enqueued id=%s", job.id)
            else:
                logger.info("ingest_skipped queue_count=%s", count)
        except Exception:
            logger.exception("ingest_enqueue_failed")
        try:
            _maybe_enqueue_cleanup(queue, redis_conn, datetime.now(timezone.utc))
        except Exception:
            logger.exception("cleanup_enqueue_failed")
        time.sleep(interval)


if __name__ == "__main__":
    main()
