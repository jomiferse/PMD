import logging
import time

from fastapi import Depends, HTTPException
import redis

from .auth import api_key_auth
from .models import ApiKey
from .settings import settings

logger = logging.getLogger(__name__)
redis_conn = redis.from_url(settings.REDIS_URL)


def rate_limit(api_key: ApiKey = Depends(api_key_auth)) -> ApiKey:
    limit = api_key.rate_limit_per_min or settings.RATE_LIMIT_DEFAULT_PER_MIN
    if limit <= 0:
        return api_key

    bucket = int(time.time() // 60)
    key = f"rate:{api_key.id}:{bucket}"

    try:
        count = redis_conn.incr(key)
        if count == 1:
            redis_conn.expire(key, 120)
    except Exception:
        logger.exception("rate_limit_failed")
        return api_key

    if count > limit:
        raise HTTPException(status_code=429, detail="Rate limit exceeded")
    return api_key
