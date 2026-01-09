import hashlib
import secrets
from datetime import datetime, timezone

from fastapi import Depends, Header, HTTPException
from sqlalchemy.orm import Session

from .db import get_db
from .models import ApiKey
from .settings import settings


def hash_api_key(raw_key: str) -> str:
    return hashlib.sha256(raw_key.encode("utf-8")).hexdigest()


PASSWORD_HASH_ITERATIONS = 120_000


def hash_password(raw_password: str) -> str:
    salt = secrets.token_bytes(16)
    digest = hashlib.pbkdf2_hmac(
        "sha256",
        raw_password.encode("utf-8"),
        salt,
        PASSWORD_HASH_ITERATIONS,
    )
    return f"{PASSWORD_HASH_ITERATIONS}${salt.hex()}${digest.hex()}"


def verify_password(raw_password: str, stored_hash: str) -> bool:
    try:
        iterations_raw, salt_hex, digest_hex = stored_hash.split("$", 2)
        iterations = int(iterations_raw)
        salt = bytes.fromhex(salt_hex)
        expected = bytes.fromhex(digest_hex)
    except (ValueError, TypeError):
        return False
    computed = hashlib.pbkdf2_hmac("sha256", raw_password.encode("utf-8"), salt, iterations)
    return secrets.compare_digest(computed, expected)


def create_session_token() -> str:
    return secrets.token_hex(32)


def api_key_auth(
    x_api_key: str = Header(default="", alias="X-API-Key"),
    db: Session = Depends(get_db),
) -> ApiKey:
    raw = x_api_key.strip()
    if not raw:
        raise HTTPException(status_code=401, detail="Missing API key")

    key_hash = hash_api_key(raw)
    api_key = (
        db.query(ApiKey)
        .filter(ApiKey.key_hash == key_hash, ApiKey.revoked_at.is_(None))
        .one_or_none()
    )
    if not api_key:
        raise HTTPException(status_code=401, detail="Invalid API key")

    api_key.last_used_at = datetime.now(timezone.utc)
    db.commit()
    return api_key


def admin_key_auth(
    x_admin_key: str = Header(default="", alias="X-Admin-Key"),
):
    expected = (settings.ADMIN_API_KEY or "").strip()
    if not expected:
        raise HTTPException(status_code=401, detail="Admin API key not configured")
    if x_admin_key.strip() != expected:
        raise HTTPException(status_code=401, detail="Invalid admin API key")
    return True
