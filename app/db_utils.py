import logging

from sqlalchemy import text
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


def apply_db_timeout(db: Session, timeout_seconds: int) -> None:
    if timeout_seconds <= 0:
        return
    try:
        db.execute(text("SET LOCAL statement_timeout = :ms"), {"ms": int(timeout_seconds * 1000)})
    except Exception:
        logger.exception("db_statement_timeout_set_failed")


def is_statement_timeout(exc: OperationalError) -> bool:
    orig = getattr(exc, "orig", None)
    pgcode = getattr(orig, "pgcode", None)
    if pgcode == "57014":
        return True
    message = str(exc).lower()
    return "statement timeout" in message or "canceling statement" in message
