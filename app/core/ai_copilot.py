import html
import json
import logging
import time
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
    User,
)
from ..settings import settings
from . import defaults
from .user_settings import get_effective_user_settings
from ..trading.sizing import DraftUnavailable, compute_draft_size
from ..llm.client import get_trade_recommendation
from .alert_classification import classify_alert_with_snapshots
from .market_links import attach_market_slugs, market_url
from .telegram import send_telegram_message, answer_callback_query, edit_message_reply_markup

logger = logging.getLogger(__name__)
redis_conn = redis.from_url(settings.REDIS_URL)

REC_STATUS_PROPOSED = "PROPOSED"
REC_STATUS_CONFIRMED = "CONFIRMED"
REC_STATUS_SKIPPED = "SKIPPED"
REC_STATUS_EXPIRED = "EXPIRED"
READ_ONLY_DISCLAIMER = "<i>Read-only analytics • Manual execution only • Not financial advice</i>"

RISK_SPENT_KEY = "ai:risk:spent:{user_id}:{date}"
CALLBACK_SEEN_KEY = "ai:telegram:callback:{callback_id}"
CALLBACK_TTL_SECONDS = 60 * 60 * 24
COPILOT_THEME_DEDUPE_KEY = "copilot:theme:{user_id}:{theme_key}"
COPILOT_DAILY_COUNT_KEY = "copilot:count:{user_id}:{date}"
COPILOT_RUN_KEY = "copilot:run:{run_id}"
COPILOT_LAST_STATUS_KEY = "copilot:last_status:{user_id}"
COPILOT_LAST_STATUS_TTL_SECONDS = 60 * 60 * 24
COPILOT_RUN_TTL_SECONDS = 60 * 60 * 24


@dataclass(frozen=True)
class DraftOrder:
    side: str
    price: float
    size: float
    notional_usd: float


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
) -> AiRecommendation | None:
    now_ts = datetime.now(timezone.utc)
    theme_key = _theme_key_for_alert(alert)
    effective = get_effective_user_settings(user, db=db)
    if not effective.copilot_enabled:
        logger.info("ai_rec_skipped copilot_disabled user_id=%s", user.user_id)
        return None
    if _is_muted(db, user.user_id, alert.market_id, theme_key, now_ts):
        logger.info(
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

    claim = _claim_copilot_theme(user.user_id, theme_key, effective.copilot_theme_ttl_minutes)
    if not claim.claimed:
        logger.info(
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
        llm_context = _build_llm_context(alert, classification, user, effective, evidence)
        _increment_copilot_run_counter(run_id, "llm_calls_attempted", 1)
        llm_result = get_trade_recommendation(llm_context)
        _increment_copilot_run_counter(run_id, "llm_calls_succeeded", 1)

        draft = None
        if llm_result["recommendation"] == "BUY":
            draft = _build_draft(effective, alert, user, theme_key)

        recommendation = AiRecommendation(
            user_id=user.user_id,
            alert_id=alert.id,
            recommendation=llm_result["recommendation"],
            confidence=llm_result["confidence"],
            rationale=llm_result["rationale"],
            risks=llm_result["risks"],
            draft_side=draft.side if isinstance(draft, DraftOrder) else None,
            draft_price=draft.price if isinstance(draft, DraftOrder) else None,
            draft_size=draft.size if isinstance(draft, DraftOrder) else None,
            draft_notional_usd=draft.notional_usd if isinstance(draft, DraftOrder) else None,
            status=REC_STATUS_PROPOSED,
            expires_at=now_ts + timedelta(minutes=defaults.AI_RECOMMENDATION_EXPIRES_MINUTES),
        )
        db.add(recommendation)
        db.commit()
        db.refresh(recommendation)

        record_ai_event(db, recommendation, "proposed", "ai_copilot_recommendation_created")
        logger.info(
            "ai_rec_created user_id=%s alert_id=%s recommendation=%s confidence=%s",
            user.user_id,
            alert.id,
            recommendation.recommendation,
            recommendation.confidence,
        )
        if recommendation.recommendation in {"WAIT", "SKIP"}:
            logger.info(
                "ai_rec_hold_reason user_id=%s alert_id=%s rationale=%s",
                user.user_id,
                alert.id,
                recommendation.rationale[:200],
            )
        sent = _send_recommendation_message(db, user, alert, recommendation, evidence, run_id=run_id)
        if not sent:
            _release_copilot_theme(claim.key, defaults.COPILOT_DEDUPE_FAILURE_TTL_SECONDS)
        return recommendation
    except Exception:
        _release_copilot_theme(claim.key, defaults.COPILOT_DEDUPE_FAILURE_TTL_SECONDS)
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


def _handle_confirm_skip(db: Session, rec_id: int, action: str, chat_id: str | None) -> dict[str, Any]:
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
            return {"ok": False, "reason": "no_draft", "message": "No draft for WAIT/SKIP."}
        rec.status = REC_STATUS_CONFIRMED
        db.commit()
        record_ai_event(db, rec, "confirmed", "telegram_confirm")
        logger.info("ai_rec_confirmed rec_id=%s user_id=%s", rec.id, rec.user_id)
        if rec.draft_notional_usd and rec.user_id:
            _register_risk_spend(rec.user_id, rec.draft_notional_usd)
        if chat_id:
            _send_confirm_payload(db, str(chat_id), rec)
        return {"ok": True, "message": "Confirmed."}

    rec.status = REC_STATUS_SKIPPED
    db.commit()
    record_ai_event(db, rec, "skipped", "telegram_skip")
    logger.info("ai_rec_skipped rec_id=%s user_id=%s", rec.id, rec.user_id)
    if chat_id:
        send_telegram_message(str(chat_id), "Skipped.")
    return {"ok": True, "message": "Skipped."}


def _handle_mute(
    db: Session,
    chat_id: str | None,
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
    user = _lookup_user_by_chat(db, str(lookup_id) if lookup_id is not None else None)
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


def _copilot_daily_count_key(user_id: Any, now_ts: datetime) -> str:
    date_key = now_ts.date().isoformat()
    return COPILOT_DAILY_COUNT_KEY.format(user_id=user_id, date=date_key)


def _increment_copilot_daily_count(user_id: Any) -> None:
    if not user_id:
        return
    now_ts = datetime.now(timezone.utc)
    key = _copilot_daily_count_key(user_id, now_ts)
    ttl_seconds = max(int(defaults.COPILOT_DAILY_TTL_SECONDS), 60)
    try:
        redis_conn.incr(key)
        redis_conn.expire(key, ttl_seconds)
    except Exception:
        logger.exception("copilot_daily_count_increment_failed user_id=%s", user_id)


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
                "duration_ms": int((time.time() - started_at) * 1000),
            }
        )
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
) -> bool:
    if not user.telegram_chat_id:
        return False
    draft_unavailable = None
    if rec.recommendation == "BUY" and not _draft_complete(rec):
        effective = get_effective_user_settings(user, db=db)
        draft_unavailable = _draft_unavailable_reasons(alert, effective, rec.user_id)
    attach_market_slugs(db, [alert])
    text, markup = _format_ai_message(alert, rec, evidence, draft_unavailable)
    _increment_copilot_run_counter(run_id, "telegram_sends_attempted", 1)
    response = send_telegram_message(user.telegram_chat_id, text, reply_markup=markup)
    success = bool(response and response.get("ok"))
    message_id = None
    if success:
        message_id = response.get("result", {}).get("message_id")
        _increment_copilot_daily_count(rec.user_id)
        _increment_copilot_run_counter(run_id, "telegram_sends_succeeded", 1)
        if message_id:
            rec.telegram_message_id = str(message_id)
            db.commit()
    else:
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
    user = db.query(User).filter(User.user_id == rec.user_id).one_or_none()
    effective = get_effective_user_settings(user, db=db) if user else None
    missing = _draft_unavailable_reasons(alert, effective, rec.user_id) if alert else ["missing alert"]

    lines = ["<b>Confirmed (manual only).</b>"]
    if alert:
        attach_market_slugs(db, [alert])
        title = html.escape(alert.title[:160])
        slug = getattr(alert, "market_slug", None)
        slug_text = html.escape(slug) if slug else html.escape(alert.market_id)
        lines.extend([title, f"Slug: {slug_text}"])

        side_label = _format_side_label(alert)
        amount_parts = []
        if rec.draft_size and rec.draft_size > 0:
            amount_parts.append(f"{rec.draft_size:.2f} shares")
        if rec.draft_price and rec.draft_price > 0:
            amount_parts.append(f"@ {rec.draft_price:.4f}")
        if rec.draft_notional_usd and rec.draft_notional_usd > 0:
            amount_parts.append(f"(~${rec.draft_notional_usd:.2f})")
        amount_text = " ".join(amount_parts) if amount_parts else "n/a"
        lines.extend(
            [
                f"Suggested side: {side_label}",
                f"Suggested amount: {amount_text}",
                "",
                _format_market_link(alert.market_id, slug),
            ]
        )
    if missing and missing != ["missing alert"]:
        missing_text = ", ".join(missing)
        lines.extend(["", f"Draft inputs missing: {html.escape(missing_text)}"])

    lines.extend(["", READ_ONLY_DISCLAIMER])
    send_telegram_message(chat_id, "\n".join(lines))


def _format_ai_message(
    alert: Alert,
    rec: AiRecommendation,
    evidence: list[str],
    draft_unavailable: list[str] | None,
) -> tuple[str, dict[str, Any]]:
    title = html.escape(alert.title[:160])
    p_yes = _format_p_yes(alert)
    move = _format_move(alert)
    liq = _format_liquidity(alert)
    evidence_block = _format_evidence_lines(evidence)
    rationale = _format_bullets(rec.rationale)
    risks = _format_bullets(rec.risks)

    lines = [
        f"<b>AI Copilot: {rec.recommendation} ({rec.confidence})</b>",
        title,
        f"Move: {move} | {p_yes}",
        liq,
        "",
        "<b>Evidence</b>",
        evidence_block,
        "",
        "<b>Rationale</b>",
        rationale,
        "",
        "<b>Risks</b>",
        risks,
    ]

    if rec.draft_size and rec.draft_price and rec.draft_notional_usd:
        side_label = _format_side_label(alert)
        draft_block = (
            f"token_id: {alert.market_id}\n"
            f"side_label: {side_label}\n"
            f"price: {rec.draft_price:.4f}\n"
            f"size: {rec.draft_size:.2f}\n"
            f"notional_usd: {rec.draft_notional_usd:.2f}"
        )
        lines.extend(["", "<b>Draft order</b>", f"<pre>{html.escape(draft_block)}</pre>"])
    elif rec.recommendation in {"WAIT", "SKIP"}:
        lines.extend(["", "<b>Draft order</b>", "Draft not proposed for WAIT/SKIP."])
    else:
        lines.append("")
        lines.append("<b>Draft order</b>")
        if draft_unavailable:
            lines.append("Draft size unavailable:")
            lines.extend([f"- {html.escape(reason)}" for reason in draft_unavailable])
        else:
            lines.append("Draft size unavailable.")

    lines.extend(
        [
            "",
            _format_market_link(alert.market_id, getattr(alert, "market_slug", None)),
        ]
    )
    lines.extend(["", READ_ONLY_DISCLAIMER])
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
        "risk_budget_usd_per_day": effective.risk_budget_usd_per_day,
        "max_usd_per_trade": effective.max_usd_per_trade,
        "max_liquidity_fraction": effective.max_liquidity_fraction,
        "no_financial_advice": True,
        "evidence": evidence,
    }


def _build_draft(
    effective,
    alert: Alert,
    user: User,
    theme_key: str,
) -> DraftOrder | DraftUnavailable:
    price = _select_draft_price(alert)
    risk_budget_usd_per_day = effective.risk_budget_usd_per_day
    max_usd_per_trade = effective.max_usd_per_trade
    max_liquidity_fraction = effective.max_liquidity_fraction
    remaining = _risk_budget_remaining(user.user_id, risk_budget_usd_per_day)
    size = compute_draft_size(
        risk_budget_usd_per_day=risk_budget_usd_per_day,
        max_usd_per_trade=max_usd_per_trade,
        max_liquidity_fraction=max_liquidity_fraction,
        risk_budget_remaining=remaining,
        liquidity=alert.liquidity,
        price=price,
    )
    if isinstance(size, DraftUnavailable):
        logger.info(
            "ai_draft_sizing user_id=%s theme_key=%s alert_id=%s market_id=%s "
            "prefs_missing=%s max_usd_per_trade=%s risk_budget_usd_per_day=%s max_liquidity_fraction=%s "
            "price=%s size=%s notional_usd=%s",
            user.user_id,
            theme_key,
            alert.id,
            alert.market_id,
            False,
            max_usd_per_trade,
            risk_budget_usd_per_day,
            max_liquidity_fraction,
            price,
            0.0,
            0.0,
        )
        return size
    logger.info(
        "ai_draft_sizing user_id=%s theme_key=%s alert_id=%s market_id=%s "
        "prefs_missing=%s max_usd_per_trade=%s risk_budget_usd_per_day=%s max_liquidity_fraction=%s "
        "price=%s size=%s notional_usd=%s",
        user.user_id,
        theme_key,
        alert.id,
        alert.market_id,
        False,
        max_usd_per_trade,
        risk_budget_usd_per_day,
        max_liquidity_fraction,
        price,
        size.size_shares,
        size.notional_usd,
    )
    return DraftOrder(side="YES", price=price, size=size.size_shares, notional_usd=size.notional_usd)


def _risk_budget_remaining(user_id: Any, risk_budget_usd_per_day: float) -> float:
    if risk_budget_usd_per_day <= 0:
        return 0.0
    key = RISK_SPENT_KEY.format(user_id=user_id, date=_today_key())
    try:
        spent_raw = redis_conn.get(key)
        spent = float(spent_raw.decode()) if spent_raw else 0.0
        return max(risk_budget_usd_per_day - spent, 0.0)
    except Exception:
        logger.exception("ai_risk_budget_read_failed user_id=%s", user_id)
        return risk_budget_usd_per_day


def _register_risk_spend(user_id: Any, amount: float) -> None:
    if amount <= 0:
        return
    key = RISK_SPENT_KEY.format(user_id=user_id, date=_today_key())
    ttl_seconds = _seconds_until_end_of_day()
    try:
        redis_conn.incrbyfloat(key, amount)
        if ttl_seconds > 0:
            redis_conn.expire(key, ttl_seconds)
    except Exception:
        logger.exception("ai_risk_budget_write_failed user_id=%s", user_id)


def _today_key() -> str:
    now_ts = datetime.now(timezone.utc)
    return now_ts.strftime("%Y%m%d")


def _seconds_until_end_of_day() -> int:
    now_ts = datetime.now(timezone.utc)
    tomorrow = (now_ts + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    return int((tomorrow - now_ts).total_seconds())


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


def _lookup_user_by_chat(db: Session, chat_id: str | None) -> User | None:
    if not chat_id:
        return None
    rows = (
        db.query(User)
        .filter(User.telegram_chat_id == str(chat_id))
        .order_by(User.created_at.desc())
        .limit(2)
        .all()
    )
    if not rows:
        return None
    if len(rows) > 1:
        logger.warning("telegram_chat_id_multiple_users chat_id=%s count=%s", chat_id, len(rows))
    return rows[0]


def _format_bullets(text: str) -> str:
    if not text:
        return "- (none)"
    parts = [part.strip(" -") for part in text.replace("\n", ";").split(";") if part.strip()]
    if not parts:
        return "- (none)"
    return "\n".join(f"- {html.escape(part)}" for part in parts[:4])


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


def _draft_complete(rec: AiRecommendation) -> bool:
    if not rec.draft_side:
        return False
    if not rec.draft_price or rec.draft_price <= 0:
        return False
    if not rec.draft_size or rec.draft_size <= 0:
        return False
    if not rec.draft_notional_usd or rec.draft_notional_usd <= 0:
        return False
    return True


def _draft_unavailable_reasons(
    alert: Alert | None,
    effective,
    user_id: Any,
) -> list[str]:
    if not alert:
        return ["missing alert"]
    price = _select_draft_price(alert)
    liquidity = alert.liquidity or 0.0
    if not effective:
        reasons = [
            "max_usd_per_trade is 0 (or missing)",
            "risk_budget_usd_per_day is 0 (or missing)",
        ]
        if price <= 0:
            reasons.append("missing price")
        if liquidity <= 0:
            reasons.append("missing liquidity")
        return reasons
    remaining = _risk_budget_remaining(user_id, effective.risk_budget_usd_per_day)
    result = compute_draft_size(
        risk_budget_usd_per_day=effective.risk_budget_usd_per_day,
        max_usd_per_trade=effective.max_usd_per_trade,
        max_liquidity_fraction=effective.max_liquidity_fraction,
        risk_budget_remaining=remaining,
        liquidity=liquidity,
        price=price,
    )
    if isinstance(result, DraftUnavailable):
        return result.reasons
    return []


def _select_draft_price(alert: Alert) -> float:
    best_ask = getattr(alert, "best_ask", None)
    if best_ask and best_ask > 0:
        return best_ask
    if alert.market_p_yes and alert.market_p_yes > 0:
        return alert.market_p_yes
    if alert.new_price and alert.new_price > 0:
        return alert.new_price
    return 0.0


def _callback_already_processed(callback_id: str) -> bool:
    key = CALLBACK_SEEN_KEY.format(callback_id=callback_id)
    try:
        marked = redis_conn.set(key, "1", nx=True, ex=CALLBACK_TTL_SECONDS)
        return not bool(marked)
    except Exception:
        logger.exception("telegram_callback_idempotency_failed callback_id=%s", callback_id)
        return False


def _claim_copilot_theme(user_id: Any, theme_key: str, ttl_minutes: int) -> CopilotThemeClaim:
    ttl_seconds = max(int(ttl_minutes * 60), 60)
    key = COPILOT_THEME_DEDUPE_KEY.format(user_id=user_id, theme_key=theme_key)
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


def _clear_message_actions(chat_id: str | None, message_id: Any) -> None:
    if not chat_id or not message_id:
        return
    edit_message_reply_markup(str(chat_id), str(message_id), {"inline_keyboard": []})


def _format_side_label(alert: Alert) -> str:
    market_kind = getattr(alert, "market_kind", None)
    is_yesno = getattr(alert, "is_yesno", None)
    if market_kind == "yesno" or is_yesno is True:
        return "YES"
    mapping_confidence = getattr(alert, "mapping_confidence", None)
    if mapping_confidence != "verified":
        return "OUTCOME_0"
    label = getattr(alert, "primary_outcome_label", None)
    sanitized = _sanitize_outcome_label(label)
    if sanitized:
        if sanitized in {"OVER", "UNDER"} and market_kind != "ou":
            return "OUTCOME_0"
        return sanitized
    return "OUTCOME_0"


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
        f"Sustained move across {sustained_count} snapshots ({sustained_minutes}m)",
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
    last_delta = points[-1][1] - points[-2][1]
    if last_delta * direction < 0:
        retrace_pct = abs(last_delta) / move_abs * 100
        return f"Reversal risk: last snapshot retraced {retrace_pct:.1f}%"
    return f"No reversal observed in last {window_minutes}m"


def _descriptor_from_thresholds(value: float, high: float, moderate: float) -> str:
    if value >= high:
        return "High"
    if value >= moderate:
        return "Moderate"
    return "Light"


def _format_usd(value: float) -> str:
    return f"${value:,.0f}"
