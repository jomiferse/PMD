import os
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
SCHEDULER_HEARTBEAT_KEY = "scheduler:heartbeat"


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
    scheduler_id = f"{os.getpid()}:{int(time.time())}"

    while True:
        try:
            ttl_seconds = max(interval * 2, 60)
            claimed = redis_conn.set(SCHEDULER_HEARTBEAT_KEY, scheduler_id, nx=True, ex=ttl_seconds)
            if not claimed:
                existing = redis_conn.get(SCHEDULER_HEARTBEAT_KEY)
                existing_id = existing.decode() if existing else "unknown"
                if existing_id != scheduler_id:
                    logger.warning("scheduler_multiple_detected existing=%s current=%s", existing_id, scheduler_id)
                redis_conn.set(SCHEDULER_HEARTBEAT_KEY, scheduler_id, ex=ttl_seconds)
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
