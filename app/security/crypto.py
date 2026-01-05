import json
import logging
from typing import Any

from cryptography.fernet import Fernet, InvalidToken

from ..settings import settings

logger = logging.getLogger(__name__)


def encrypt_payload(payload: dict[str, Any]) -> str | None:
    fernet = _get_fernet()
    if not fernet:
        return None
    raw = json.dumps(payload, ensure_ascii=True).encode("utf-8")
    return fernet.encrypt(raw).decode("utf-8")


def decrypt_payload(token: str) -> dict[str, Any] | None:
    fernet = _get_fernet()
    if not fernet:
        return None
    try:
        raw = fernet.decrypt(token.encode("utf-8"))
    except InvalidToken:
        logger.error("credentials_decrypt_failed_invalid_token")
        return None
    except Exception:
        logger.exception("credentials_decrypt_failed")
        return None
    try:
        payload = json.loads(raw.decode("utf-8"))
    except json.JSONDecodeError:
        logger.error("credentials_decrypt_invalid_json")
        return None
    return payload if isinstance(payload, dict) else None


def _get_fernet() -> Fernet | None:
    key = settings.POLY_CREDENTIALS_ENCRYPTION_KEY
    if not key:
        logger.error("credentials_encryption_key_missing")
        return None
    try:
        return Fernet(key.encode("utf-8"))
    except Exception:
        logger.exception("credentials_encryption_key_invalid")
        return None
