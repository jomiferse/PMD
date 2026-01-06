import redis
from rq import Worker, Queue

from ..settings import settings
from ..logging import configure_logging

redis_conn = redis.from_url(settings.REDIS_URL)

if __name__ == "__main__":
    configure_logging()
    queue = Queue("default", connection=redis_conn)
    worker = Worker([queue], connection=redis_conn)
    worker.work()
