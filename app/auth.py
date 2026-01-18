import hashlib
import secrets

from fastapi import Header, HTTPException

from .settings import settings


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


def admin_key_auth(
    x_admin_key: str = Header(default="", alias="X-Admin-Key"),
):
    expected = (settings.ADMIN_API_KEY or "").strip()
    if not expected:
        raise HTTPException(status_code=401, detail="Admin API key not configured")
    if x_admin_key.strip() != expected:
        raise HTTPException(status_code=401, detail="Invalid admin API key")
    return True
