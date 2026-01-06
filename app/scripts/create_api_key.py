import argparse
import secrets
from datetime import datetime, timezone

from app.auth import hash_api_key
from app.db import SessionLocal
from app.models import ApiKey
from app.settings import settings


def main() -> None:
    parser = argparse.ArgumentParser(description="Create a new API key")
    parser.add_argument("--name", required=True, help="Key name (e.g., 'prod')")
    parser.add_argument("--tenant-id", default=settings.DEFAULT_TENANT_ID)
    parser.add_argument("--plan", default="free")
    parser.add_argument("--rate-limit-per-min", type=int, default=settings.RATE_LIMIT_DEFAULT_PER_MIN)
    args = parser.parse_args()

    raw_key = secrets.token_urlsafe(32)
    key_hash = hash_api_key(raw_key)

    db = SessionLocal()
    try:
        api_key = ApiKey(
            tenant_id=args.tenant_id,
            name=args.name,
            key_hash=key_hash,
            plan=args.plan,
            rate_limit_per_min=args.rate_limit_per_min,
            created_at=datetime.now(timezone.utc),
        )
        db.add(api_key)
        db.commit()
    finally:
        db.close()

    print(raw_key)


if __name__ == "__main__":
    main()
