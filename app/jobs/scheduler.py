import time
import logging

import redis
from rq import Queue

from ..settings import settings
from ..logging import configure_logging
from .run import job_sync_wrapper

logger = logging.getLogger(__name__)


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
        time.sleep(interval)


if __name__ == "__main__":
    main()
