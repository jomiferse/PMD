import argparse
import json
import uuid
from datetime import datetime, timezone

from app.core.alert_classification import classify_alert_with_snapshots
from app.core.alerts import _enqueue_ai_recommendations, _evaluate_delivery_decision, _resolve_user_preferences
from app.db import SessionLocal
from app.models import Alert, User


def _load_user(db, user_id: str | None) -> User | None:
    if not user_id:
        return None
    try:
        parsed = uuid.UUID(user_id)
    except ValueError:
        return None
    return db.query(User).filter(User.user_id == parsed).one_or_none()


def _load_alert(db, alert_id: int | None, market_id: str | None) -> Alert | None:
    query = db.query(Alert).order_by(Alert.created_at.desc())
    if alert_id is not None:
        return db.query(Alert).filter(Alert.id == alert_id).one_or_none()
    if market_id:
        return query.filter(Alert.market_id == market_id).first()
    return query.first()


def main() -> None:
    parser = argparse.ArgumentParser(description="Copilot debug harness (dry run).")
    parser.add_argument("--user-id", required=True, help="User UUID")
    parser.add_argument("--alert-id", type=int, help="Alert id to evaluate")
    parser.add_argument("--market-id", help="Market id to pick most recent alert")
    parser.add_argument("--enqueue", action="store_true", help="Enqueue Copilot job (default: dry run)")
    args = parser.parse_args()

    db = SessionLocal()
    try:
        user = _load_user(db, args.user_id)
        if not user:
            print("User not found or invalid user id.")
            return
        alert = _load_alert(db, args.alert_id, args.market_id)
        if not alert:
            print("Alert not found.")
            return
        config = _resolve_user_preferences(user, pref=None, db=db)
        classification = classify_alert_with_snapshots(db, alert)
        decision = _evaluate_delivery_decision(alert, classification, config)
        print("Alert decision:", json.dumps(decision.__dict__, default=str, indent=2))
        if not decision.deliver:
            print(f"Not actionable: {decision.reason}")
            return
        evaluations = _enqueue_ai_recommendations(
            db,
            config,
            [alert],
            classifier=lambda *_args, **_kwargs: classification,
            allow_enqueue=config.ai_copilot_enabled,
            enqueue_jobs=args.enqueue,
            run_id=None,
            run_started_at=None,
            digest_window_minutes=config.digest_window_minutes,
        )
        summary = [
            {"theme_key": ev.theme_key, "market_id": ev.market_id, "reasons": ev.reasons}
            for ev in evaluations
        ]
        print("Copilot evaluation:", json.dumps(summary, indent=2))
        if args.enqueue:
            print("Enqueued Copilot jobs.")
        else:
            print("Dry run only (no enqueue).")
        print(f"Evaluated at {datetime.now(timezone.utc).isoformat()}")
    finally:
        db.close()


if __name__ == "__main__":
    main()
