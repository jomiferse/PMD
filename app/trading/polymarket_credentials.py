import logging
from typing import Any

from sqlalchemy.orm import Session

from ..models import UserPolymarketCredential
from ..security.crypto import encrypt_payload, decrypt_payload

logger = logging.getLogger(__name__)


def get_user_polymarket_credentials(db: Session, user_id) -> dict[str, Any] | None:
    record = (
        db.query(UserPolymarketCredential)
        .filter(UserPolymarketCredential.user_id == user_id)
        .one_or_none()
    )
    if not record:
        return None
    payload = decrypt_payload(record.encrypted_payload)
    if not payload:
        logger.error("polymarket_credentials_decrypt_failed user_id=%s", user_id)
        return None
    return payload


def set_user_polymarket_credentials(
    db: Session,
    user_id,
    payload: dict[str, Any],
    commit: bool = True,
) -> UserPolymarketCredential | None:
    encrypted = encrypt_payload(payload)
    if not encrypted:
        return None
    record = (
        db.query(UserPolymarketCredential)
        .filter(UserPolymarketCredential.user_id == user_id)
        .one_or_none()
    )
    if record:
        record.encrypted_payload = encrypted
    else:
        record = UserPolymarketCredential(user_id=user_id, encrypted_payload=encrypted)
        db.add(record)
    if commit:
        db.commit()
    return record


def clear_user_polymarket_credentials(db: Session, user_id, commit: bool = True) -> bool:
    record = (
        db.query(UserPolymarketCredential)
        .filter(UserPolymarketCredential.user_id == user_id)
        .one_or_none()
    )
    if not record:
        return False
    db.delete(record)
    if commit:
        db.commit()
    return True
