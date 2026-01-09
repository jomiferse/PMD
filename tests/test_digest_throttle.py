from datetime import datetime, timedelta, timezone
from uuid import uuid4

from app.core import alerts


def test_digest_recently_sent_respects_window(monkeypatch):
    now = datetime.now(timezone.utc)
    user_id = uuid4()

    class FakeRedis:
        def __init__(self):
            self.store = {}

        def get(self, key):
            return self.store.get(key)

    fake = FakeRedis()
    key = alerts.USER_DIGEST_LAST_SENT_KEY.format(user_id=user_id)
    fake.store[key] = (now - timedelta(minutes=5)).isoformat().encode("utf-8")

    monkeypatch.setattr(alerts, "redis_conn", fake)

    assert alerts._digest_recently_sent(user_id, now, window_minutes=10) is True
    assert alerts._digest_recently_sent(user_id, now, window_minutes=1) is False
