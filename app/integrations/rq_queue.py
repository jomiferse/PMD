from rq import Queue

from .redis_client import redis_conn

q = Queue("default", connection=redis_conn)
