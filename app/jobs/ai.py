import logging
import uuid

from sqlalchemy.orm import Session

from ..db import SessionLocal
from ..models import Alert, User
from ..core.ai_copilot import create_ai_recommendation, _complete_copilot_run
from ..core.user_settings import get_effective_user_settings


def ai_recommendation_job(
    user_id: str,
    alert_id: int,
    run_id: str | None = None,
    signal_speed: str | None = None,
    window_minutes: int | None = None,
) -> dict:
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
        effective = get_effective_user_settings(user, db=db)
        if not effective.copilot_enabled:
            return {"ok": False, "reason": "ai_disabled"}
        alert = db.query(Alert).filter(Alert.id == alert_id).one_or_none()
        if not alert:
            return {"ok": False, "reason": "alert_missing"}
        rec = create_ai_recommendation(
            db,
            user,
            alert,
            run_id=run_id,
            signal_speed=signal_speed,
            window_minutes=window_minutes,
        )
        if not rec:
            return {"ok": False, "reason": "no_recommendation"}
        return {"ok": True, "recommendation_id": rec.id}
    except Exception:
        logger = logging.getLogger(__name__)
        logger.exception("ai_recommendation_job_failed user_id=%s alert_id=%s", user_id, alert_id)
        return {"ok": False, "reason": "exception"}
    finally:
        _complete_copilot_run(run_id)
        db.close()
