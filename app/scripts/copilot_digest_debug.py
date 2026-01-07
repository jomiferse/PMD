import argparse
import json
import uuid
from datetime import datetime, timedelta, timezone

from app.core.alert_classification import classify_alert_with_snapshots
from app.core.alerts import (
    AlertClass,
    _copilot_skip_note,
    _enqueue_ai_recommendations,
    _evaluate_delivery_decision,
    _filter_alerts_for_user,
    _resolve_alert_class,
    _resolve_user_preferences,
)
from app.core.fast_signals import FAST_ALERT_TYPE
from app.db import SessionLocal
from app.models import Alert, User, UserAlertPreference
from app.settings import settings


def _load_user(db, user_id: str | None) -> User | None:
    if not user_id:
        return None
    try:
        parsed = uuid.UUID(user_id)
    except ValueError:
        return None
    return db.query(User).filter(User.user_id == parsed).one_or_none()


def _load_pref(db, user_id) -> UserAlertPreference | None:
    if not user_id:
        return None
    return db.query(UserAlertPreference).filter(UserAlertPreference.user_id == user_id).one_or_none()


def main() -> None:
    parser = argparse.ArgumentParser(description="Copilot digest debug harness (dry run).")
    parser.add_argument("--user-id", required=True, help="User UUID")
    parser.add_argument("--tenant-id", default=settings.DEFAULT_TENANT_ID, help="Tenant id (default: settings)")
    parser.add_argument("--window-minutes", type=int, help="Digest window minutes override")
    parser.add_argument("--enqueue", action="store_true", help="Enqueue Copilot jobs (default: dry run)")
    args = parser.parse_args()

    db = SessionLocal()
    try:
        user = _load_user(db, args.user_id)
        if not user:
            print("User not found or invalid user id.")
            return
        pref = _load_pref(db, user.user_id)
        config = _resolve_user_preferences(user, pref, db=db)
        window_minutes = args.window_minutes or config.digest_window_minutes
        now_ts = datetime.now(timezone.utc)
        window_start = now_ts - timedelta(minutes=window_minutes)

        alerts = (
            db.query(Alert)
            .filter(
                Alert.tenant_id == args.tenant_id,
                Alert.created_at >= window_start,
                Alert.alert_type != FAST_ALERT_TYPE,
            )
            .all()
        )

        included_alerts, _filtered_out, _filter_map, _filter_reasons = _filter_alerts_for_user(
            alerts, config
        )

        classification_cache = {}

        def classifier(alert):
            cached = classification_cache.get(alert.id)
            if cached:
                return cached
            classification = classify_alert_with_snapshots(db, alert)
            classification_cache[alert.id] = classification
            return classification

        deliverable_candidates = []
        for alert in included_alerts:
            decision = _evaluate_delivery_decision(alert, classifier(alert), config)
            if decision.deliver:
                deliverable_candidates.append(alert)

        actionable_alerts = []
        for alert in deliverable_candidates:
            classification = classifier(alert)
            if _resolve_alert_class(classification) != AlertClass.INFO_ONLY:
                actionable_alerts.append(alert)

        result = None
        if actionable_alerts:
            result = _enqueue_ai_recommendations(
                db,
                config,
                actionable_alerts,
                classifier=classifier,
                allow_enqueue=config.ai_copilot_enabled,
                enqueue_jobs=args.enqueue,
                run_id=None,
                run_started_at=None,
                digest_window_minutes=window_minutes,
            )

        summary = {
            "user_id": str(config.user_id),
            "tenant_id": args.tenant_id,
            "window_minutes": window_minutes,
            "alerts_total": len(alerts),
            "alerts_included": len(included_alerts),
            "actionable_alerts": len(actionable_alerts),
            "copilot_enqueued": result.enqueued if result else 0,
            "copilot_eligible": result.eligible_count if result else 0,
            "copilot_selected": result.selected_theme_keys if result else [],
            "copilot_skip_note": _copilot_skip_note(config, result),
            "ran_at": now_ts.isoformat(),
        }
        print(json.dumps(summary, indent=2))

        if result:
            themes = [
                {"theme_key": ev.theme_key, "market_id": ev.market_id, "reasons": ev.reasons}
                for ev in result.evaluations
            ]
            print("Copilot evaluation:", json.dumps(themes, indent=2))
        if args.enqueue:
            print("Enqueued Copilot jobs.")
        else:
            print("Dry run only (no enqueue).")
    finally:
        db.close()


if __name__ == "__main__":
    main()
