import html
import json
import logging
import re
import time
import uuid
from contextlib import nullcontext
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import redis
from sqlalchemy.orm import Session

from ..alerts.theme_key import extract_theme
from ..models import (
    AiMarketMute,
    AiRecommendation,
    AiRecommendationEvent,
    AiThemeMute,
    Alert,
    MarketSnapshot,
    PendingTelegramChat,
    User,
)
from ..settings import settings
from . import defaults
from .user_settings import get_effective_user_settings
from ..llm.client import get_trade_recommendation
from .alert_classification import classify_alert_with_snapshots
from .market_links import attach_market_slugs, market_url
from .signal_speed import SIGNAL_SPEED_FAST, SIGNAL_SPEED_STANDARD
from .telegram import send_telegram_message, answer_callback_query, edit_message_reply_markup

logger = logging.getLogger(__name__)
redis_conn = redis.from_url(settings.REDIS_URL)

REC_STATUS_PROPOSED = "PROPOSED"
REC_STATUS_CONFIRMED = "CONFIRMED"
REC_STATUS_SKIPPED = "SKIPPED"
REC_STATUS_EXPIRED = "EXPIRED"
COPILOT_FOOTER = "<i>Manual execution only. PMD does not place orders.</i>"
CALLBACK_SEEN_KEY = "ai:telegram:callback:{callback_id}"
CALLBACK_TTL_SECONDS = 60 * 60 * 24
COPILOT_THEME_DEDUPE_KEY = "copilot:theme:{user_id}:{theme_key}"
COPILOT_FAST_THEME_DEDUPE_KEY = "copilot:fast:theme:{user_id}:{theme_key}"
COPILOT_DAILY_COUNT_KEY = "copilot:count:{user_id}:{date}"
COPILOT_FAST_DAILY_COUNT_KEY = "copilot:fast:count:{user_id}:{date}"
COPILOT_HOURLY_COUNT_KEY = "copilot:hour:{user_id}:{hour}"
COPILOT_RUN_KEY = "copilot:run:{run_id}"
COPILOT_LAST_STATUS_KEY = "copilot:last_status:{user_id}"
COPILOT_LAST_STATUS_TTL_SECONDS = 60 * 60 * 24
COPILOT_RUN_TTL_SECONDS = 60 * 60 * 24
START_PAYLOAD_PREFIX = "pmd_"
START_FOOTER = ""
START_MESSAGE_SUCCESS = "✅ Account linked. You will now receive PMD alerts here."
START_MESSAGE_IDEMPOTENT = "✅ This chat is already linked."
START_MESSAGE_NOT_FOUND = "❌ I can’t find that PMD account. Generate a new link from your PMD dashboard."
START_MESSAGE_CONFLICT = (
    "⚠️ This account/chat is already linked elsewhere. Unlink from your PMD dashboard and try again."
)
START_MESSAGE_PENDING = (
    "👋 Welcome! To link your PMD account, go to PMD Dashboard → Telegram → Link account, "
    "then open the Telegram link."
)
START_MESSAGE_INVALID = "❌ Invalid link. Generate a new link from your PMD dashboard."


@dataclass(frozen=True)
class CopilotThemeClaim:
    claimed: bool
    key: str
    ttl_seconds: int
    ttl_remaining: int | None


def create_ai_recommendation(
    db: Session,
    user: User,
    alert: Alert,
    run_id: str | None = None,
    signal_speed: str | None = None,
    window_minutes: int | None = None,
) -> AiRecommendation | None:
    now_ts = datetime.now(timezone.utc)
    theme_key = _theme_key_for_alert(alert)
    effective = get_effective_user_settings(user, db=db)
    signal_speed = signal_speed or SIGNAL_SPEED_STANDARD
    full_ttl_minutes = max(int(effective.copilot_theme_ttl_minutes), 1)
    full_ttl_seconds = max(full_ttl_minutes * 60, 60)
    failure_ttl_seconds = min(
        full_ttl_seconds,
        max(int(defaults.COPILOT_DEDUPE_FAILURE_TTL_SECONDS), 60),
    )
    pending_ttl_minutes = max(int(failure_ttl_seconds / 60), 1)
    if not effective.copilot_enabled:
        logger.debug("ai_rec_skipped copilot_disabled user_id=%s", user.user_id)
        return None
    if _is_muted(db, user.user_id, alert.market_id, theme_key, now_ts):
        logger.debug(
            "ai_rec_skipped muted user_id=%s market_id=%s theme_key=%s",
            user.user_id,
            alert.market_id,
            theme_key,
        )
        return None

    existing = (
        db.query(AiRecommendation)
        .filter(AiRecommendation.user_id == user.user_id, AiRecommendation.alert_id == alert.id)
        .order_by(AiRecommendation.created_at.desc())
        .first()
    )
    if existing and existing.status in {REC_STATUS_PROPOSED, REC_STATUS_CONFIRMED}:
        if existing.expires_at and existing.expires_at < now_ts:
            existing.status = REC_STATUS_EXPIRED
            db.commit()
        else:
            return None

    claim = _claim_copilot_theme(
        user.user_id,
        theme_key,
        pending_ttl_minutes,
        signal_speed=signal_speed,
    )
    if not claim.claimed:
        logger.debug(
            "ai_rec_skipped reason=DEDUPE theme_key=%s ttl_remaining=%s user_id=%s dedupe_key=%s "
            "ttl_configured=%s scope=theme",
            theme_key,
            claim.ttl_remaining,
            user.user_id,
            claim.key,
            claim.ttl_seconds,
        )
        return None

    try:
        classification = classify_alert_with_snapshots(db, alert)
        evidence = _build_evidence(db, alert)
        if window_minutes is None:
            window_minutes = _parse_window_minutes_from_evidence(evidence)
        llm_context = _build_llm_context(
            alert,
            classification,
            user,
            effective,
            evidence,
            signal_speed=signal_speed,
            window_minutes=window_minutes,
        )
        _increment_copilot_run_counter(run_id, "llm_calls_attempted", 1)
        try:
            llm_result = get_trade_recommendation(llm_context)
        except Exception:
            _increment_copilot_run_counter(run_id, "llm_calls_failed", 1)
            logger.exception("copilot_llm_failed user_id=%s alert_id=%s", user.user_id, alert.id)
            _release_copilot_theme(claim.key, failure_ttl_seconds)
            return None
        llm_result = _apply_fast_recommendation_rules(
            llm_result,
            alert,
            evidence,
            signal_speed=signal_speed,
        )
        _increment_copilot_run_counter(run_id, "llm_calls_succeeded", 1)

        recommendation = AiRecommendation(
            user_id=user.user_id,
            alert_id=alert.id,
            recommendation=llm_result["recommendation"],
            confidence=llm_result["confidence"],
            rationale=llm_result["rationale"],
            risks=llm_result["risks"],
            status=REC_STATUS_PROPOSED,
            expires_at=now_ts + timedelta(minutes=defaults.AI_RECOMMENDATION_EXPIRES_MINUTES),
        )
        db.add(recommendation)
        db.commit()
        db.refresh(recommendation)

        record_ai_event(db, recommendation, "proposed", "ai_copilot_recommendation_created")
        logger.debug(
            "ai_rec_created user_id=%s alert_id=%s recommendation=%s confidence=%s",
            user.user_id,
            alert.id,
            recommendation.recommendation,
            recommendation.confidence,
        )
        if recommendation.recommendation in {"WAIT", "SKIP"}:
            logger.debug(
                "ai_rec_hold_reason user_id=%s alert_id=%s rationale=%s",
                user.user_id,
                alert.id,
                recommendation.rationale[:200],
            )
        sent = _send_recommendation_message(
            db,
            user,
            alert,
            recommendation,
            evidence,
            run_id=run_id,
            signal_speed=signal_speed,
            window_minutes=window_minutes,
        )
        if sent:
            _extend_copilot_theme_ttl(claim.key, full_ttl_seconds)
        else:
            _release_copilot_theme(claim.key, failure_ttl_seconds)
        return recommendation
    except Exception:
        _release_copilot_theme(claim.key, failure_ttl_seconds)
        raise


def handle_telegram_callback(db: Session, payload: dict[str, Any]) -> dict[str, Any]:
    callback = payload.get("callback_query") or {}
    data = callback.get("data")
    callback_id = callback.get("id")
    from_user = callback.get("from") or {}
    from_user_id = from_user.get("id")
    message = callback.get("message") or {}
    chat = message.get("chat") or {}
    chat_id = chat.get("id")
    if not data:
        if callback_id:
            answer_callback_query(callback_id, text="Missing callback data.")
        return {"ok": False, "reason": "missing_callback_data"}

    if callback_id and _callback_already_processed(callback_id):
        answer_callback_query(callback_id, text="Already processed.")
        return {"ok": True, "reason": "duplicate_callback", "message": "Already processed."}

    action, *parts = data.split(":")
    if action in {"confirm", "skip"} and parts:
        rec_id = int(parts[0])
        result = _handle_confirm_skip(db, rec_id, action, chat_id)
        if callback_id:
            answer_callback_query(callback_id, text=result.get("message", ""))
        _clear_message_actions(chat_id, message.get("message_id"))
        return result

    if action == "mute" and len(parts) >= 2:
        if len(parts) >= 3 and parts[0] in {"market", "theme", "theme_alert", "market_alert"}:
            target_type = parts[0]
            target_key = parts[1]
            minutes = int(parts[2])
        else:
            target_type = "market"
            target_key = parts[0]
            minutes = int(parts[1])
        result = _handle_mute(db, chat_id, from_user_id, target_type, target_key, minutes)
        if callback_id:
            answer_callback_query(callback_id, text=result.get("message", "Muted."))
        _clear_message_actions(chat_id, message.get("message_id"))
        return result

    if callback_id:
        answer_callback_query(callback_id, text="Unsupported action.")
    return {"ok": False, "reason": "unknown_action"}


def handle_telegram_update(db: Session, payload: dict[str, Any]) -> dict[str, Any]:
    if payload.get("callback_query"):
        return handle_telegram_callback(db, payload)

    message = payload.get("message") or {}
    text = message.get("text")
    if not text:
        return {"ok": True, "reason": "ignored"}

    command = text.strip().split(maxsplit=1)[0]
    if not command.startswith("/start"):
        return {"ok": True, "reason": "ignored"}

    return _handle_telegram_start(db, message, text)


def _handle_telegram_start(db: Session, message: dict[str, Any], text: str) -> dict[str, Any]:
    _log_event("start_received")
    chat = message.get("chat") or {}
    chat_id = _normalize_chat_id(chat.get("id"))
    if chat_id is None:
        _log_event("error")
        return {"ok": False, "reason": "missing_chat_id"}

    payload = _extract_start_payload(text)
    if payload is None:
        _upsert_pending_chat(db, chat_id)
        _send_start_reply(chat_id, START_MESSAGE_PENDING)
        return {"ok": True, "reason": "pending"}

    if not payload.startswith(START_PAYLOAD_PREFIX):
        _log_event("invalid_payload")
        _upsert_pending_chat(db, chat_id)
        _send_start_reply(chat_id, START_MESSAGE_INVALID)
        return {"ok": False, "reason": "invalid_payload"}

    user_id_raw = payload[len(START_PAYLOAD_PREFIX):].strip()
    try:
        user_id = uuid.UUID(user_id_raw)
    except ValueError:
        _log_event("invalid_payload")
        _upsert_pending_chat(db, chat_id)
        _send_start_reply(chat_id, START_MESSAGE_INVALID)
        return {"ok": False, "reason": "invalid_payload"}

    with _transaction(db):
        user = db.query(User).filter(User.user_id == user_id).one_or_none()
        if not user:
            _log_event("user_not_found")
            _send_start_reply(chat_id, START_MESSAGE_NOT_FOUND)
            return {"ok": False, "reason": "user_not_found"}

        existing = (
            db.query(User)
            .filter(User.telegram_chat_id == chat_id, User.user_id != user.user_id)
            .first()
        )
        if existing:
            _log_event("link_conflict")
            _send_start_reply(chat_id, START_MESSAGE_CONFLICT)
            return {"ok": False, "reason": "link_conflict"}

        if user.telegram_chat_id is None:
            user.telegram_chat_id = chat_id
            db.query(PendingTelegramChat).filter(
                PendingTelegramChat.telegram_chat_id == chat_id
            ).delete(synchronize_session=False)
            _log_event("link_success")
            _send_start_reply(chat_id, START_MESSAGE_SUCCESS)
            return {"ok": True, "reason": "link_success"}

        if user.telegram_chat_id == chat_id:
            _log_event("link_exists")
            _send_start_reply(chat_id, START_MESSAGE_IDEMPOTENT)
            return {"ok": True, "reason": "link_exists"}

        _log_event("link_conflict")
        _send_start_reply(chat_id, START_MESSAGE_CONFLICT)
        return {"ok": False, "reason": "link_conflict"}


def _extract_start_payload(text: str) -> str | None:
    parts = text.strip().split(maxsplit=1)
    if len(parts) < 2:
        return None
    payload = parts[1].strip()
    return payload or None


def _send_start_reply(chat_id: int, message: str) -> None:
    text = f"{message}\n\n{START_FOOTER}" if START_FOOTER else message
    send_telegram_message(str(chat_id), text)


def _upsert_pending_chat(db: Session, chat_id: int) -> None:
    now_ts = datetime.now(timezone.utc)
    with _transaction(db):
        existing = (
            db.query(PendingTelegramChat)
            .filter(PendingTelegramChat.telegram_chat_id == chat_id)
            .one_or_none()
        )
        if existing:
            existing.last_seen_at = now_ts
            _log_event("pending_exists")
            return

        db.add(
            PendingTelegramChat(
                telegram_chat_id=chat_id,
                first_seen_at=now_ts,
                last_seen_at=now_ts,
                status="pending",
            )
        )
        _log_event("pending_created")


def _normalize_chat_id(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _log_event(event: str) -> None:
    logger.debug(json.dumps({"event": event}))


def _transaction(db: Session):
    return db.begin() if not db.in_transaction() else nullcontext()


def _handle_confirm_skip(
    db: Session,
    rec_id: int,
    action: str,
    chat_id: int | str | None,
) -> dict[str, Any]:
    rec = db.query(AiRecommendation).filter(AiRecommendation.id == rec_id).one_or_none()
    if not rec:
        return {"ok": False, "reason": "recommendation_not_found"}

    now_ts = datetime.now(timezone.utc)
    expires_at = _ensure_aware(rec.expires_at)
    if expires_at and expires_at < now_ts:
        if rec.status == REC_STATUS_PROPOSED:
            rec.status = REC_STATUS_EXPIRED
            db.commit()
        return {"ok": False, "reason": "recommendation_expired", "message": "This recommendation expired."}

    if rec.status == REC_STATUS_CONFIRMED:
        return {"ok": True, "message": "Already confirmed."}
    if rec.status == REC_STATUS_SKIPPED:
        return {"ok": True, "message": "Already skipped."}
    if rec.status == REC_STATUS_EXPIRED:
        return {"ok": False, "reason": "recommendation_expired", "message": "This recommendation expired."}
    if rec.status != REC_STATUS_PROPOSED:
        return {"ok": False, "reason": "invalid_status", "message": "Recommendation is not actionable."}

    if action == "confirm":
        if rec.recommendation in {"WAIT", "SKIP"}:
            return {"ok": False, "reason": "not_actionable", "message": "Recommendation is not actionable."}
        rec.status = REC_STATUS_CONFIRMED
        db.commit()
        record_ai_event(db, rec, "confirmed", "telegram_confirm")
        logger.debug("ai_rec_confirmed rec_id=%s user_id=%s", rec.id, rec.user_id)
        if chat_id:
            _send_confirm_payload(db, str(chat_id), rec)
        return {"ok": True, "message": "Confirmed."}

    rec.status = REC_STATUS_SKIPPED
    db.commit()
    record_ai_event(db, rec, "skipped", "telegram_skip")
    logger.debug("ai_rec_skipped rec_id=%s user_id=%s", rec.id, rec.user_id)
    if chat_id:
        send_telegram_message(str(chat_id), "Skipped.")
    return {"ok": True, "message": "Skipped."}


def _handle_mute(
    db: Session,
    chat_id: int | str | None,
    from_user_id: str | int | None,
    target_type: str,
    target_key: str,
    minutes: int,
) -> dict[str, Any]:
    if not target_key:
        return {"ok": False, "reason": "missing_mute_target"}
    now_ts = datetime.now(timezone.utc)
    expires_at = now_ts + timedelta(minutes=max(minutes, 1))
    lookup_id = from_user_id if from_user_id is not None else chat_id
    user = _lookup_user_by_chat(db, lookup_id)
    if not user:
        return {"ok": False, "reason": "user_not_found"}

    if target_type in {"theme_alert", "market_alert"}:
        try:
            alert_id = int(target_key)
        except (TypeError, ValueError):
            return {"ok": False, "reason": "invalid_alert_id"}
        alert = db.query(Alert).filter(Alert.id == alert_id).one_or_none()
        if not alert:
            return {"ok": False, "reason": "alert_not_found"}
        if target_type == "theme_alert":
            target_key = _theme_key_for_alert(alert)
            target_type = "theme"
        else:
            target_key = alert.market_id
            target_type = "market"

    if target_type == "theme":
        mute = (
            db.query(AiThemeMute)
            .filter(AiThemeMute.user_id == user.user_id, AiThemeMute.theme_key == target_key)
            .one_or_none()
        )
        if mute:
            if mute.expires_at >= expires_at:
                return {"ok": True, "message": "Already muted."}
            mute.expires_at = expires_at
        else:
            mute = AiThemeMute(user_id=user.user_id, theme_key=target_key, expires_at=expires_at)
            db.add(mute)
        muted_label = "theme"
    else:
        mute = (
            db.query(AiMarketMute)
            .filter(AiMarketMute.user_id == user.user_id, AiMarketMute.market_id == target_key)
            .one_or_none()
        )
        if mute:
            if mute.expires_at >= expires_at:
                return {"ok": True, "message": "Already muted."}
            mute.expires_at = expires_at
        else:
            mute = AiMarketMute(user_id=user.user_id, market_id=target_key, expires_at=expires_at)
            db.add(mute)
        muted_label = "market"
    db.commit()
    if chat_id:
        send_telegram_message(str(chat_id), f"Muted this {muted_label} for 24h.")
    return {"ok": True, "message": f"Muted this {muted_label} for 24h."}


def _ensure_aware(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value


def _copilot_daily_count_key(
    user_id: Any,
    now_ts: datetime,
    signal_speed: str = SIGNAL_SPEED_STANDARD,
) -> str:
    date_key = now_ts.date().isoformat()
    if signal_speed == SIGNAL_SPEED_FAST:
        return COPILOT_FAST_DAILY_COUNT_KEY.format(user_id=user_id, date=date_key)
    return COPILOT_DAILY_COUNT_KEY.format(user_id=user_id, date=date_key)


def _copilot_hourly_count_key(user_id: Any, now_ts: datetime) -> str:
    hour_key = now_ts.strftime("%Y-%m-%d-%H")
    return COPILOT_HOURLY_COUNT_KEY.format(user_id=user_id, hour=hour_key)


def _increment_copilot_daily_count(user_id: Any, signal_speed: str = SIGNAL_SPEED_STANDARD) -> None:
    if not user_id:
        return
    now_ts = datetime.now(timezone.utc)
    key = _copilot_daily_count_key(user_id, now_ts, signal_speed)
    ttl_seconds = max(int(defaults.COPILOT_DAILY_TTL_SECONDS), 60)
    try:
        redis_conn.incr(key)
        redis_conn.expire(key, ttl_seconds)
    except Exception:
        logger.exception("copilot_daily_count_increment_failed user_id=%s", user_id)


def _increment_copilot_hourly_count(user_id: Any) -> None:
    if not user_id:
        return
    now_ts = datetime.now(timezone.utc)
    key = _copilot_hourly_count_key(user_id, now_ts)
    ttl_seconds = max(int(defaults.COPILOT_HOURLY_TTL_SECONDS), 60)
    try:
        redis_conn.incr(key)
        redis_conn.expire(key, ttl_seconds)
    except Exception:
        logger.exception("copilot_hourly_count_increment_failed user_id=%s", user_id)


def init_copilot_run(
    run_id: str,
    summary: dict[str, object],
    run_started_at: float | None,
    expected_jobs: int,
) -> None:
    if not run_id:
        return
    key = COPILOT_RUN_KEY.format(run_id=run_id)
    started_at = run_started_at if run_started_at is not None else time.time()
    try:
        redis_conn.hsetnx(key, "base_summary", json.dumps(summary, ensure_ascii=True))
        redis_conn.hsetnx(key, "expected_jobs", int(expected_jobs))
        redis_conn.hsetnx(key, "jobs_completed", 0)
        redis_conn.hsetnx(key, "llm_calls_attempted", 0)
        redis_conn.hsetnx(key, "llm_calls_succeeded", 0)
        redis_conn.hsetnx(key, "telegram_sends_attempted", 0)
        redis_conn.hsetnx(key, "telegram_sends_succeeded", 0)
        redis_conn.hsetnx(key, "started_at", float(started_at))
        redis_conn.hsetnx(key, "logged", 0)
        redis_conn.expire(key, COPILOT_RUN_TTL_SECONDS)
        _maybe_log_copilot_run(run_id)
    except Exception:
        logger.exception("copilot_run_init_failed run_id=%s", run_id)


def log_copilot_run_summary(summary: dict[str, object]) -> None:
    try:
        logger.info("copilot_run_summary %s", json.dumps(summary, ensure_ascii=True))
    except Exception:
        logger.exception("copilot_run_summary_log_failed")


def store_copilot_last_status(summary: dict[str, object]) -> None:
    user_id = summary.get("user_id")
    if not user_id:
        return
    reason_counts = summary.get("skipped_by_reason_counts") or {}
    top_reasons = []
    if isinstance(reason_counts, dict):
        ranked = sorted(
            ((key, reason_counts.get(key, 0)) for key in reason_counts),
            key=lambda item: (-int(item[1] or 0), str(item[0])),
        )
        top_reasons = [reason for reason, _count in ranked[:3]]
    payload = {
        "user_id": user_id,
        "run_id": summary.get("run_id"),
        "summary": summary,
        "top_reasons": top_reasons,
        "created_at": summary.get("created_at"),
    }
    try:
        redis_conn.set(
            COPILOT_LAST_STATUS_KEY.format(user_id=user_id),
            json.dumps(payload, ensure_ascii=True),
            ex=COPILOT_LAST_STATUS_TTL_SECONDS,
        )
    except Exception:
        logger.exception("copilot_last_status_store_failed user_id=%s", user_id)


def _increment_copilot_run_counter(run_id: str | None, field: str, amount: int) -> None:
    if not run_id:
        return
    key = COPILOT_RUN_KEY.format(run_id=run_id)
    try:
        redis_conn.hincrby(key, field, amount)
    except Exception:
        logger.exception("copilot_run_counter_failed run_id=%s field=%s", run_id, field)


def _complete_copilot_run(run_id: str | None) -> None:
    if not run_id:
        return
    key = COPILOT_RUN_KEY.format(run_id=run_id)
    try:
        redis_conn.hincrby(key, "jobs_completed", 1)
        _maybe_log_copilot_run(run_id)
    except Exception:
        logger.exception("copilot_run_complete_failed run_id=%s", run_id)


def _maybe_log_copilot_run(run_id: str | None) -> None:
    if not run_id:
        return
    key = COPILOT_RUN_KEY.format(run_id=run_id)
    try:
        expected_raw = redis_conn.hget(key, "expected_jobs")
        if isinstance(expected_raw, (bytes, bytearray)):
            expected_raw = expected_raw.decode()
        expected = int(expected_raw) if expected_raw is not None else 0
        completed_raw = redis_conn.hget(key, "jobs_completed")
        if isinstance(completed_raw, (bytes, bytearray)):
            completed_raw = completed_raw.decode()
        completed = int(completed_raw) if completed_raw is not None else 0
        if completed < expected:
            return
        logged_raw = redis_conn.hget(key, "logged")
        if str(logged_raw) in {"1", "b'1'"}:
            return
        if not redis_conn.hsetnx(key, "logged", 1):
            return
        base_raw = redis_conn.hget(key, "base_summary")
        if not base_raw:
            return
        if isinstance(base_raw, (bytes, bytearray)):
            base_raw = base_raw.decode()
        summary = json.loads(base_raw)

        def _get_int(field_name: str) -> int:
            raw = redis_conn.hget(key, field_name)
            if raw is None:
                return 0
            if isinstance(raw, (bytes, bytearray)):
                raw = raw.decode()
            try:
                return int(raw)
            except ValueError:
                return 0

        started_raw = redis_conn.hget(key, "started_at")
        if isinstance(started_raw, (bytes, bytearray)):
            started_raw = started_raw.decode()
        try:
            started_at = float(started_raw) if started_raw is not None else time.time()
        except ValueError:
            started_at = time.time()
        summary.update(
            {
                "llm_calls_attempted": _get_int("llm_calls_attempted"),
                "llm_calls_succeeded": _get_int("llm_calls_succeeded"),
                "telegram_sends_attempted": _get_int("telegram_sends_attempted"),
                "telegram_sends_succeeded": _get_int("telegram_sends_succeeded"),
                "sent": _get_int("telegram_sends_succeeded"),
                "window": summary.get("digest_window_minutes"),
                "selected": summary.get("themes_selected"),
                "duration_ms": int((time.time() - started_at) * 1000),
            }
        )
        reason_counts = summary.get("skipped_by_reason_counts") or {}
        if not isinstance(reason_counts, dict):
            reason_counts = {}
        llm_failures = max(
            int(summary.get("llm_calls_attempted") or 0) - int(summary.get("llm_calls_succeeded") or 0),
            0,
        )
        if llm_failures:
            reason_counts["LLM_ERROR"] = reason_counts.get("LLM_ERROR", 0) + llm_failures
        telegram_failures = max(
            int(summary.get("telegram_sends_attempted") or 0)
            - int(summary.get("telegram_sends_succeeded") or 0),
            0,
        )
        if telegram_failures:
            reason_counts["TELEGRAM_ERROR"] = reason_counts.get("TELEGRAM_ERROR", 0) + telegram_failures
        summary["skipped_by_reason_counts"] = reason_counts
        log_copilot_run_summary(summary)
        if summary.get("telegram_sends_succeeded") == 0:
            store_copilot_last_status(summary)
    except Exception:
        logger.exception("copilot_run_maybe_log_failed run_id=%s", run_id)


def _send_recommendation_message(
    db: Session,
    user: User,
    alert: Alert,
    rec: AiRecommendation,
    evidence: list[str],
    run_id: str | None = None,
    signal_speed: str = SIGNAL_SPEED_STANDARD,
    window_minutes: int | None = None,
) -> bool:
    if not user.telegram_chat_id:
        return False
    attach_market_slugs(db, [alert])
    text, markup = _format_ai_message(
        alert,
        rec,
        evidence,
        signal_speed=signal_speed,
        window_minutes=window_minutes,
    )
    _increment_copilot_run_counter(run_id, "telegram_sends_attempted", 1)
    response = send_telegram_message(user.telegram_chat_id, text, reply_markup=markup)
    success = bool(response and response.get("ok"))
    message_id = None
    if success:
        message_id = response.get("result", {}).get("message_id")
        _increment_copilot_daily_count(rec.user_id, signal_speed=signal_speed)
        _increment_copilot_hourly_count(rec.user_id)
        _increment_copilot_run_counter(run_id, "telegram_sends_succeeded", 1)
        logger.debug(
            "copilot_decision_sent user_id=%s alert_id=%s rec_id=%s recommendation=%s confidence=%s signal_speed=%s",
            user.user_id,
            alert.id,
            rec.id,
            rec.recommendation,
            rec.confidence,
            signal_speed,
        )
        if message_id:
            rec.telegram_message_id = str(message_id)
            db.commit()
    else:
        _increment_copilot_run_counter(run_id, "telegram_sends_failed", 1)
        logger.warning(
            "copilot_telegram_send_failed user_id=%s alert_id=%s rec_id=%s",
            user.user_id,
            alert.id,
            rec.id,
        )
    return success


def _send_confirm_payload(db: Session, chat_id: str, rec: AiRecommendation) -> None:
    if settings.EXECUTION_ENABLED:
        logger.error("execution_enabled_guard_triggered rec_id=%s user_id=%s", rec.id, rec.user_id)
        send_telegram_message(chat_id, "Execution is disabled in this environment.")
        return
    alert = db.query(Alert).filter(Alert.id == rec.alert_id).one_or_none()

    lines = ["<b>Confirmed (manual only).</b>"]
    if alert:
        attach_market_slugs(db, [alert])
        title = html.escape(alert.title[:160])
        slug = getattr(alert, "market_slug", None)
        slug_text = html.escape(slug) if slug else html.escape(alert.market_id)
        lines.extend([title, f"Slug: {slug_text}", "", _format_market_link(alert.market_id, slug)])

    lines.extend(["", COPILOT_FOOTER])
    send_telegram_message(chat_id, "\n".join(lines))


def _format_ai_message(
    alert: Alert,
    rec: AiRecommendation,
    evidence: list[str],
    signal_speed: str = SIGNAL_SPEED_STANDARD,
    window_minutes: int | None = None,
) -> tuple[str, dict[str, Any]]:
    title = html.escape(alert.title[:160])
    p_yes = _format_p_yes(alert)
    move = _format_move(alert)
    liq = _format_liquidity(alert)
    evidence_block = _format_evidence_lines(evidence)
    rationale_parts = _sanitize_threshold_claims(_split_bullet_text(rec.rationale), alert)
    risk_parts = _sanitize_threshold_claims(_split_bullet_text(rec.risks), alert)
    rationale = _format_bullet_parts(rationale_parts)
    rationale_header = "<b>Rationale</b>"
    risks_header = "<b>Risks</b>"
    what_changes_block = None
    if rec.recommendation in {"WAIT", "SKIP"}:
        hold_label = "WAIT" if rec.recommendation == "WAIT" else "SKIP"
        rationale_header = "<b>Rationale (why not entering now)</b>"
        risks_header = f"<b>Risks (what could invalidate this {hold_label})</b>"
        rr_note = _extreme_prob_risk_reward(alert)
        if rr_note and rr_note not in risk_parts:
            risk_parts.append(rr_note)
        what_changes_block = _format_bullet_parts(_build_wait_change_signals(alert, evidence))
    risks = _format_bullet_parts(risk_parts)

    display_action = rec.recommendation
    header = f"<b>AI Copilot: {rec.recommendation} ({rec.confidence})</b>"
    signal_window = window_minutes
    if signal_speed == SIGNAL_SPEED_FAST:
        if signal_window is None:
            signal_window = _parse_window_minutes_from_evidence(evidence)
        if signal_window is None:
            signal_window = 0
        if rec.recommendation in {"WAIT", "SKIP"}:
            display_action = "WATCH"
        header = f"<b>FAST Copilot: {display_action} (Early Signal)</b>"

    lines = [
        header,
    ]
    if signal_speed == SIGNAL_SPEED_FAST:
        context_window = _parse_window_minutes_from_evidence(evidence) or signal_window or 0
        lines.append(f"Signal window: {signal_window}m | Context window: {context_window}m")
    lines.extend(
        [
            title,
            f"Move: {move} | {p_yes}",
            liq,
            "",
            "<b>Evidence</b>",
            evidence_block,
            "",
            rationale_header,
            rationale,
            "",
            risks_header,
            risks,
        ]
    )
    if what_changes_block:
        lines.extend(["", "<b>What would change this view</b>", what_changes_block])

    lines.extend(
        [
            "",
            _format_market_link(alert.market_id, getattr(alert, "market_slug", None)),
        ]
    )
    lines.extend(["", COPILOT_FOOTER])
    keyboard: list[list[dict[str, Any]]] = []
    if rec.recommendation == "BUY":
        keyboard.append(
            [
                {"text": "Confirm", "callback_data": f"confirm:{rec.id}"},
                {"text": "Skip", "callback_data": f"skip:{rec.id}"},
            ]
        )
    keyboard.append(
        [
            {
                "text": "Mute theme 24h",
                "callback_data": f"mute:theme_alert:{alert.id}:1440",
            }
        ]
    )
    keyboard.append(
        [
            {
                "text": "Mute market 24h",
                "callback_data": f"mute:market_alert:{alert.id}:1440",
            }
        ]
    )
    markup = {"inline_keyboard": keyboard}
    return "\n".join(lines), markup


def _build_llm_context(
    alert: Alert,
    classification,
    user: User,
    effective,
    evidence: list[str],
    signal_speed: str = SIGNAL_SPEED_STANDARD,
    window_minutes: int | None = None,
) -> dict[str, Any]:
    return {
        "user_id": str(user.user_id),
        "alert_id": alert.id,
        "market_id": alert.market_id,
        "title": alert.title,
        "category": alert.category,
        "move": alert.move,
        "market_p_yes": alert.market_p_yes,
        "prev_market_p_yes": alert.prev_market_p_yes,
        "liquidity": alert.liquidity,
        "volume_24h": alert.volume_24h,
        "signal_type": classification.signal_type,
        "confidence": classification.confidence,
        "suggested_action": classification.suggested_action,
        "signal_speed": signal_speed,
        "window_minutes": window_minutes,
        "no_financial_advice": True,
        "evidence": evidence,
    }


def record_ai_event(db: Session, rec: AiRecommendation, action: str, details: str | None) -> None:
    event = AiRecommendationEvent(
        recommendation_id=rec.id,
        user_id=rec.user_id,
        alert_id=rec.alert_id,
        action=action,
        details=details,
    )
    db.add(event)
    db.commit()


def _theme_key_for_alert(alert: Alert) -> str:
    extracted = extract_theme(alert.title or "", category=alert.category, slug=alert.market_id)
    return extracted.theme_key


def _is_muted(db: Session, user_id, market_id: str, theme_key: str, now_ts: datetime) -> bool:
    market_muted = (
        db.query(AiMarketMute)
        .filter(
            AiMarketMute.user_id == user_id,
            AiMarketMute.market_id == market_id,
            AiMarketMute.expires_at > now_ts,
        )
        .count()
        > 0
    )
    if market_muted:
        return True
    return (
        db.query(AiThemeMute)
        .filter(
            AiThemeMute.user_id == user_id,
            AiThemeMute.theme_key == theme_key,
            AiThemeMute.expires_at > now_ts,
        )
        .count()
        > 0
    )


def _lookup_user_by_chat(db: Session, chat_id: int | str | None) -> User | None:
    normalized = _normalize_chat_id(chat_id)
    if normalized is None:
        return None
    rows = (
        db.query(User)
        .filter(User.telegram_chat_id == normalized)
        .order_by(User.created_at.desc())
        .limit(2)
        .all()
    )
    if not rows:
        return None
    if len(rows) > 1:
        logger.warning("telegram_chat_id_multiple_users chat_id=%s count=%s", chat_id, len(rows))
    return rows[0]


def _split_bullet_text(text: str) -> list[str]:
    if not text:
        return []
    return [part.strip(" -") for part in text.replace("\n", ";").split(";") if part.strip()]


def _sanitize_threshold_claims(parts: list[str], alert: Alert) -> list[str]:
    if not parts:
        return parts
    p_value = alert.market_p_yes
    threshold_pattern = re.compile(
        r"(?:\b0?\.?15\b|\b15%\b|\b15\s*percent\b|\b0?\.?85\b|\b85%\b|\b85\s*percent\b)",
        re.IGNORECASE,
    )
    filtered = [part for part in parts if not threshold_pattern.search(part)]
    if p_value is None:
        return filtered
    low = p_value < defaults.DEFAULT_P_MIN
    high = p_value > defaults.DEFAULT_P_MAX
    if not (low or high):
        return filtered
    label = _format_probability_label(alert)
    pct = p_value * 100
    threshold_pct = defaults.DEFAULT_P_MIN * 100 if low else defaults.DEFAULT_P_MAX * 100
    direction = "below" if low else "above"
    filtered.append(
        f"Risk/reward skew: {label} at {pct:.1f}% is {direction} {threshold_pct:.0f}%."
    )
    return filtered


def _format_bullet_parts(parts: list[str]) -> str:
    if not parts:
        return "- (none)"
    return "\n".join(f"- {html.escape(part)}" for part in parts[:4])


def _format_bullets(text: str) -> str:
    return _format_bullet_parts(_split_bullet_text(text))


def _extreme_prob_risk_reward(alert: Alert) -> str | None:
    p_yes = alert.market_p_yes
    if p_yes is None:
        return None
    if p_yes <= defaults.DEFAULT_P_MIN:
        return "Risk/reward skewed at low prices: limited upside vs a larger mean-reversion swing."
    if p_yes >= defaults.DEFAULT_P_MAX:
        return "Risk/reward skewed at high prices: limited upside vs a larger downside on pullback."
    return None


def _parse_sustained_from_evidence(evidence: list[str]) -> tuple[int | None, int | None]:
    pattern = re.compile(
        r"(?:Sustained move across|Observed across) (\d+) snapshots .*?\(~?(\d+)m\)"
    )
    for line in evidence:
        match = pattern.search(line)
        if match:
            return int(match.group(1)), int(match.group(2))
    return None, None


def _parse_abs_move_from_evidence(evidence: list[str]) -> float | None:
    pattern = re.compile(r"Abs move: [+-]?([0-9.]+)")
    for line in evidence:
        match = pattern.search(line)
        if match:
            try:
                return float(match.group(1))
            except ValueError:
                return None
    return None


def _parse_window_minutes_from_evidence(evidence: list[str]) -> int | None:
    abs_move_pattern = re.compile(r"Abs move: .*?\((\d+)m\)")
    sustained_pattern = re.compile(
        r"(?:Sustained move across|Observed across) \d+ snapshots .*?\(~?(\d+)m\)"
    )
    for line in evidence:
        match = abs_move_pattern.search(line)
        if match:
            try:
                return int(match.group(1))
            except ValueError:
                return None
    for line in evidence:
        match = sustained_pattern.search(line)
        if match:
            try:
                return int(match.group(1))
            except ValueError:
                return None
    return None


def _fast_buy_allowed(alert: Alert, evidence: list[str]) -> bool:
    abs_move = _parse_abs_move_from_evidence(evidence) or abs(_signed_price_delta(alert))
    sustained_snapshots, _ = _parse_sustained_from_evidence(evidence)
    sustained_count = sustained_snapshots or 0
    return abs_move >= 0.06 and sustained_count >= 3


def _apply_fast_recommendation_rules(
    llm_result: dict[str, str],
    alert: Alert,
    evidence: list[str],
    signal_speed: str,
) -> dict[str, str]:
    if signal_speed != SIGNAL_SPEED_FAST:
        return llm_result
    confidence = str(llm_result.get("confidence") or "").upper()
    if confidence and confidence != "HIGH":
        if llm_result.get("recommendation") != "WAIT":
            return {
                "recommendation": "WAIT",
                "confidence": confidence,
                "rationale": "FAST early signal; waiting for follow-through.",
                "risks": "Move fades quickly or reverses before confirmation.",
            }
        return llm_result
    if llm_result.get("recommendation") != "BUY":
        return llm_result
    if _fast_buy_allowed(alert, evidence):
        return llm_result
    return {
        "recommendation": "WAIT",
        "confidence": "MEDIUM",
        "rationale": "FAST early signal; waiting for follow-through.",
        "risks": "Move fades quickly or reverses before confirmation.",
    }


def _build_wait_change_signals(alert: Alert, evidence: list[str]) -> list[str]:
    signals: list[str] = []
    direction = "up" if _signed_price_delta(alert) >= 0 else "down"
    liquidity = alert.liquidity or 0.0
    volume_24h = alert.volume_24h or 0.0
    sustained_count, _sustained_minutes = _parse_sustained_from_evidence(evidence)
    if sustained_count is not None and sustained_count < 3:
        needed = max(3 - sustained_count, 1)
        signals.append(f"{needed} more snapshot(s) confirm the {direction} move.")
    if liquidity < defaults.STRONG_MIN_LIQUIDITY:
        signals.append(f"Liquidity clears ${defaults.STRONG_MIN_LIQUIDITY:,.0f}.")
    if len(signals) < 2 and volume_24h < defaults.STRONG_MIN_VOLUME_24H:
        signals.append(f"24h volume clears ${defaults.STRONG_MIN_VOLUME_24H:,.0f}.")
    if not signals:
        move_abs = _parse_abs_move_from_evidence(evidence) or abs(_signed_price_delta(alert))
        if move_abs > 0:
            signals.append(f"Move extends another {move_abs:.3f} without reversal.")
        else:
            signals.append(f"Two consecutive snapshots confirm a {direction} move.")
    return signals[:2]


def _format_p_yes(alert: Alert) -> str:
    label = _format_probability_label(alert)
    if alert.market_p_yes is None:
        return f"{label}: n/a"
    return f"{label}: {alert.market_p_yes * 100:.1f}%"


def _format_probability_label(alert: Alert) -> str:
    market_kind = getattr(alert, "market_kind", None)
    is_yesno = getattr(alert, "is_yesno", None)
    if market_kind == "yesno" or is_yesno is True:
        return "p_yes"
    mapping_confidence = getattr(alert, "mapping_confidence", None)
    if mapping_confidence != "verified":
        return "p_outcome0"
    label = getattr(alert, "primary_outcome_label", None)
    sanitized = _sanitize_outcome_label(label)
    if not sanitized:
        return "p_outcome0"
    if sanitized in {"OVER", "UNDER"} and market_kind != "ou":
        return "p_outcome0"
    return f"p_{sanitized}"


def _sanitize_outcome_label(label: str | None) -> str | None:
    if not label:
        return None
    cleaned = "".join(ch if ch.isalnum() else "_" for ch in str(label).strip()).strip("_")
    if not cleaned:
        return None
    cleaned = cleaned.upper()
    if cleaned in {"OUTCOME_0", "OUTCOME0"}:
        return None
    return cleaned


def _format_move(alert: Alert) -> str:
    delta = _signed_price_delta(alert)
    sign = "+" if delta >= 0 else "-"
    return f"{sign}{abs(delta):.3f}"


def _format_liquidity(alert: Alert) -> str:
    return f"Liquidity: ${alert.liquidity:,.0f} | Volume: ${alert.volume_24h:,.0f}"


def _format_market_link(market_id: str, slug: str | None = None) -> str:
    return market_url(market_id, slug)


def _signed_price_delta(alert: Alert) -> float:
    if alert.old_price is not None and alert.new_price is not None:
        return alert.new_price - alert.old_price
    return alert.move or 0.0


def _callback_already_processed(callback_id: str) -> bool:
    key = CALLBACK_SEEN_KEY.format(callback_id=callback_id)
    try:
        marked = redis_conn.set(key, "1", nx=True, ex=CALLBACK_TTL_SECONDS)
        return not bool(marked)
    except Exception:
        logger.exception("telegram_callback_idempotency_failed callback_id=%s", callback_id)
        return False


def _copilot_theme_dedupe_key(user_id: Any, theme_key: str, signal_speed: str) -> str:
    if signal_speed == SIGNAL_SPEED_FAST:
        return COPILOT_FAST_THEME_DEDUPE_KEY.format(user_id=user_id, theme_key=theme_key)
    return COPILOT_THEME_DEDUPE_KEY.format(user_id=user_id, theme_key=theme_key)


def _claim_copilot_theme(
    user_id: Any,
    theme_key: str,
    ttl_minutes: int,
    signal_speed: str = SIGNAL_SPEED_STANDARD,
) -> CopilotThemeClaim:
    ttl_seconds = max(int(ttl_minutes * 60), 60)
    key = _copilot_theme_dedupe_key(user_id, theme_key, signal_speed)
    ttl_remaining: int | None = None
    try:
        marked = redis_conn.set(key, "1", nx=True, ex=ttl_seconds)
        if marked:
            return CopilotThemeClaim(True, key, ttl_seconds, ttl_seconds)
        ttl_remaining = redis_conn.ttl(key)
        if ttl_remaining is not None and ttl_remaining < 0:
            ttl_remaining = None
        if ttl_remaining is None:
            try:
                redis_conn.expire(key, ttl_seconds)
            except Exception:
                logger.exception("copilot_theme_dedupe_expire_reset_failed user_id=%s theme_key=%s", user_id, theme_key)
    except Exception:
        logger.exception("copilot_theme_dedupe_failed user_id=%s theme_key=%s", user_id, theme_key)
        return CopilotThemeClaim(True, key, ttl_seconds, None)
    return CopilotThemeClaim(False, key, ttl_seconds, ttl_remaining)


def _release_copilot_theme(key: str, retry_ttl_seconds: int | None = None) -> None:
    if not key:
        return
    try:
        if retry_ttl_seconds:
            redis_conn.expire(key, max(int(retry_ttl_seconds), 1))
        else:
            redis_conn.delete(key)
    except Exception:
        logger.exception("copilot_theme_dedupe_release_failed key=%s retry_ttl=%s", key, retry_ttl_seconds)


def _extend_copilot_theme_ttl(key: str, ttl_seconds: int) -> None:
    if not key:
        return
    ttl_seconds = max(int(ttl_seconds), 60)
    try:
        redis_conn.expire(key, ttl_seconds)
    except Exception:
        logger.exception("copilot_theme_dedupe_extend_failed key=%s ttl_seconds=%s", key, ttl_seconds)


def _clear_message_actions(chat_id: str | None, message_id: Any) -> None:
    if not chat_id or not message_id:
        return
    edit_message_reply_markup(str(chat_id), str(message_id), {"inline_keyboard": []})


def _format_evidence_lines(evidence: list[str]) -> str:
    if not evidence:
        return "- Insufficient snapshot data."
    return "\n".join(f"- {html.escape(line)}" for line in evidence[:4])


def _build_evidence(db: Session, alert: Alert) -> list[str]:
    points = _load_price_points(db, alert, max_points=6)
    if not points:
        return []
    direction = _price_direction(alert)
    sustained_count, sustained_minutes = _sustained_snapshot_streak(points, direction)
    window_minutes = _points_window_minutes(points)
    move_abs = abs(_signed_price_delta(alert))
    move_pct = abs((alert.delta_pct or 0.0) * 100)
    sign = "+" if direction >= 0 else "-"

    liq_descriptor = _descriptor_from_thresholds(
        alert.liquidity,
        defaults.STRONG_MIN_LIQUIDITY,
        defaults.GLOBAL_MIN_LIQUIDITY,
    )
    vol_descriptor = _descriptor_from_thresholds(
        alert.volume_24h,
        defaults.STRONG_MIN_VOLUME_24H,
        defaults.GLOBAL_MIN_VOLUME_24H,
    )
    evidence = [
        f"Observed across {sustained_count} snapshots (~{sustained_minutes}m) within context window",
        f"Abs move: {sign}{move_abs:.3f} | pct: {sign}{move_pct:.1f}% ({window_minutes}m)",
        f"Liquidity: {liq_descriptor} {_format_usd(alert.liquidity)} | Vol24h: {vol_descriptor} {_format_usd(alert.volume_24h)}",
        _reversal_line(points, direction, window_minutes, move_abs),
    ]
    return evidence


def _load_price_points(db: Session, alert: Alert, max_points: int) -> list[tuple[datetime, float]]:
    if alert.snapshot_bucket is None:
        return []
    before = (
        db.query(MarketSnapshot.snapshot_bucket, MarketSnapshot.market_p_yes)
        .filter(
            MarketSnapshot.market_id == alert.market_id,
            MarketSnapshot.snapshot_bucket <= alert.snapshot_bucket,
            MarketSnapshot.market_p_yes.isnot(None),
        )
        .order_by(MarketSnapshot.snapshot_bucket.desc())
        .limit(max_points)
        .all()
    )
    after = (
        db.query(MarketSnapshot.snapshot_bucket, MarketSnapshot.market_p_yes)
        .filter(
            MarketSnapshot.market_id == alert.market_id,
            MarketSnapshot.snapshot_bucket >= alert.snapshot_bucket,
            MarketSnapshot.market_p_yes.isnot(None),
        )
        .order_by(MarketSnapshot.snapshot_bucket.asc())
        .limit(max_points)
        .all()
    )
    points: dict[datetime, float] = {}
    for bucket, price in before + after:
        points[bucket] = price
    return sorted(points.items(), key=lambda item: item[0])


def _price_direction(alert: Alert) -> int:
    if alert.new_price is not None and alert.old_price is not None:
        return 1 if alert.new_price - alert.old_price >= 0 else -1
    return 1 if (alert.move or 0.0) >= 0 else -1


def _sustained_snapshot_streak(
    points: list[tuple[datetime, float]],
    direction: int,
) -> tuple[int, int]:
    if len(points) < 2:
        return 1, 0
    streak_deltas = 0
    for idx in range(len(points) - 1, 0, -1):
        delta = points[idx][1] - points[idx - 1][1]
        if delta * direction > 0:
            streak_deltas += 1
        else:
            break
    streak_snapshots = max(streak_deltas + 1, 1)
    if streak_deltas == 0:
        return 1, 0
    start_idx = len(points) - 1 - streak_deltas
    minutes = int((points[-1][0] - points[start_idx][0]).total_seconds() / 60)
    return streak_snapshots, minutes


def _points_window_minutes(points: list[tuple[datetime, float]]) -> int:
    if len(points) < 2:
        return 0
    return int((points[-1][0] - points[0][0]).total_seconds() / 60)


def _reversal_line(
    points: list[tuple[datetime, float]],
    direction: int,
    window_minutes: int,
    move_abs: float,
) -> str:
    if len(points) < 2 or move_abs <= 0:
        return f"No reversal observed in last {window_minutes}m"
    baseline = points[0][1]
    last = points[-1][1]
    if direction >= 0:
        peak = max(price for _, price in points)
        total_move = peak - baseline
        retrace = (peak - last) / total_move if total_move > 0 else 0
    else:
        peak = min(price for _, price in points)
        total_move = baseline - peak
        retrace = (last - peak) / total_move if total_move > 0 else 0
    if retrace > 0:
        retrace_pct = retrace * 100
        return f"Reversal risk: last snapshot retraced {retrace_pct:.1f}% of peak move"
    return f"No reversal observed in last {window_minutes}m"


def _descriptor_from_thresholds(value: float, high: float, moderate: float) -> str:
    if value >= high:
        return "High"
    if value >= moderate:
        return "Moderate"
    return "Light"


def _format_usd(value: float) -> str:
    return f"${value:,.0f}"
