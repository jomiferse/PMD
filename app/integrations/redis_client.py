import redis

from ..settings import settings

redis_conn = redis.from_url(settings.REDIS_URL)
