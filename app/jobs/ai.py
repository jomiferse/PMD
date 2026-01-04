import uuid

from sqlalchemy.orm import Session

from ..db import SessionLocal
from ..models import Alert, User, UserAlertPreference
from ..core.ai_copilot import create_ai_recommendation


def ai_recommendation_job(user_id: str, alert_id: int) -> dict:
    db: Session = SessionLocal()
    try:
        parsed_user_id = user_id
        if isinstance(user_id, str):
            try:
                parsed_user_id = uuid.UUID(user_id)
            except ValueError:
                return {"ok": False, "reason": "invalid_user_id"}
        user = db.query(User).filter(User.user_id == parsed_user_id).one_or_none()
        if not user or not user.is_active:
            return {"ok": False, "reason": "user_inactive"}
        pref = (
            db.query(UserAlertPreference)
            .filter(UserAlertPreference.user_id == user.user_id)
            .one_or_none()
        )
        if not pref or not pref.ai_copilot_enabled:
            return {"ok": False, "reason": "ai_disabled"}
        alert = db.query(Alert).filter(Alert.id == alert_id).one_or_none()
        if not alert:
            return {"ok": False, "reason": "alert_missing"}
        rec = create_ai_recommendation(db, user, pref, alert)
        if not rec:
            return {"ok": False, "reason": "no_recommendation"}
        return {"ok": True, "recommendation_id": rec.id}
    finally:
        db.close()
