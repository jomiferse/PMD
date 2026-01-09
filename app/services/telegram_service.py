from sqlalchemy.orm import Session

from ..core.ai_copilot import handle_telegram_update


def handle_telegram_webhook(db: Session, payload: dict) -> dict:
    return handle_telegram_update(db, payload)
