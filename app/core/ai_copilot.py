import html
import json
import logging
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
    User,
    UserAlertPreference,
)
from ..settings import settings
from ..trading.sizing import compute_draft_size
from ..llm.client import get_trade_recommendation
from .alert_classification import classify_alert_with_snapshots
from .telegram import send_telegram_message, answer_callback_query, edit_message_reply_markup

logger = logging.getLogger(__name__)
redis_conn = redis.from_url(settings.REDIS_URL)

REC_STATUS_PROPOSED = "PROPOSED"
REC_STATUS_CONFIRMED = "CONFIRMED"
REC_STATUS_SKIPPED = "SKIPPED"
REC_STATUS_EXPIRED = "EXPIRED"

RISK_SPENT_KEY = "ai:risk:spent:{user_id}:{date}"
CALLBACK_SEEN_KEY = "ai:telegram:callback:{callback_id}"
CALLBACK_TTL_SECONDS = 60 * 60 * 24


@dataclass(frozen=True)
class DraftOrder:
    side: str
    price: float
    size: float
    notional_usd: float


def create_ai_recommendation(
    db: Session,
    user: User,
    pref: UserAlertPreference,
    alert: Alert,
) -> AiRecommendation | None:
    now_ts = datetime.now(timezone.utc)
    theme_key = _theme_key_for_alert(alert)
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

    classification = classify_alert_with_snapshots(db, alert)
    llm_context = _build_llm_context(alert, classification, user, pref)
    llm_result = get_trade_recommendation(llm_context)

    draft = None
    if llm_result["recommendation"] == "BUY":
        draft = _build_draft(pref, alert, user)

    recommendation = AiRecommendation(
        user_id=user.user_id,
        alert_id=alert.id,
        recommendation=llm_result["recommendation"],
        confidence=llm_result["confidence"],
        rationale=llm_result["rationale"],
        risks=llm_result["risks"],
        draft_side=draft.side if draft else None,
        draft_price=draft.price if draft else None,
        draft_size=draft.size if draft else None,
        draft_notional_usd=draft.notional_usd if draft else None,
        status=REC_STATUS_PROPOSED,
        expires_at=now_ts + timedelta(minutes=settings.AI_RECOMMENDATION_EXPIRES_MINUTES),
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
    _send_recommendation_message(db, user, alert, recommendation)
    return recommendation


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
        if len(parts) >= 3 and parts[0] in {"market", "theme"}:
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


def _send_recommendation_message(db: Session, user: User, alert: Alert, rec: AiRecommendation) -> None:
    if not user.telegram_chat_id:
        return
    text, markup = _format_ai_message(alert, rec)
    response = send_telegram_message(user.telegram_chat_id, text, reply_markup=markup)
    message_id = None
    if response and response.get("ok"):
        message_id = response.get("result", {}).get("message_id")
    if message_id:
        rec.telegram_message_id = str(message_id)
        db.commit()


def _send_confirm_payload(db: Session, chat_id: str, rec: AiRecommendation) -> None:
    alert = db.query(Alert).filter(Alert.id == rec.alert_id).one_or_none()
    pref = None
    if rec.user_id:
        pref = db.query(UserAlertPreference).filter(UserAlertPreference.user_id == rec.user_id).one_or_none()
    payload = _format_manual_payload(alert, rec)
    if payload:
        text = (
            "Confirmed. Manual execution only.\n"
            "Draft order payload:\n"
            f"<pre>{html.escape(json.dumps(payload, indent=2))}</pre>"
        )
    else:
        missing = _draft_missing_inputs(alert, pref, rec.user_id)
        missing_text = ", ".join(missing) if missing else "unknown"
        text = (
            "Confirmed. Manual execution only.\n"
            f"Draft order unavailable. Missing inputs: {missing_text}."
        )
    send_telegram_message(chat_id, text)


def _format_ai_message(alert: Alert, rec: AiRecommendation) -> tuple[str, dict[str, Any]]:
    title = html.escape(alert.title[:160])
    p_yes = _format_p_yes(alert)
    move = _format_move(alert)
    liq = _format_liquidity(alert)
    rationale = _format_bullets(rec.rationale)
    risks = _format_bullets(rec.risks)
    theme_key = _theme_key_for_alert(alert)

    lines = [
        f"<b>AI Copilot: {rec.recommendation} ({rec.confidence})</b>",
        title,
        f"Move: {move} | {p_yes}",
        liq,
        "",
        "<b>Rationale</b>",
        rationale,
        "",
        "<b>Risks</b>",
        risks,
    ]

    if rec.draft_size and rec.draft_price and rec.draft_notional_usd:
        draft_block = (
            f"token_id: {alert.market_id}\n"
            f"side: {rec.draft_side}\n"
            f"price: {rec.draft_price:.4f}\n"
            f"size: {rec.draft_size:.2f}\n"
            f"notional_usd: {rec.draft_notional_usd:.2f}"
        )
        lines.extend(["", "<b>Draft order</b>", f"<pre>{html.escape(draft_block)}</pre>"])
    else:
        lines.extend(["", "<b>Draft order</b>", "Draft size unavailable (set risk limits)."])

    lines.extend(
        [
            "",
            "Manual execution only. No orders are placed by PMD.",
            _format_market_link(alert.market_id),
        ]
    )
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
                "callback_data": f"mute:theme:{theme_key}:1440",
            }
        ]
    )
    keyboard.append(
        [
            {
                "text": "Mute market 24h",
                "callback_data": f"mute:market:{alert.market_id}:1440",
            }
        ]
    )
    markup = {"inline_keyboard": keyboard}
    return "\n".join(lines), markup


def _build_llm_context(alert: Alert, classification, user: User, pref: UserAlertPreference) -> dict[str, Any]:
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
        "risk_budget_usd_per_day": pref.risk_budget_usd_per_day,
        "max_usd_per_trade": pref.max_usd_per_trade,
        "max_liquidity_fraction": pref.max_liquidity_fraction,
        "no_financial_advice": True,
    }


def _build_draft(pref: UserAlertPreference, alert: Alert, user: User) -> DraftOrder | None:
    price = _select_draft_price(alert)
    remaining = _risk_budget_remaining(user.user_id, pref.risk_budget_usd_per_day)
    size = compute_draft_size(
        risk_budget_usd_per_day=pref.risk_budget_usd_per_day,
        max_usd_per_trade=pref.max_usd_per_trade,
        max_liquidity_fraction=pref.max_liquidity_fraction,
        risk_budget_remaining=remaining,
        liquidity=alert.liquidity,
        price=price,
    )
    if not size:
        return None
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
    is_yesno = getattr(alert, "is_yesno", None)
    if is_yesno is not False:
        return "p_yes"
    label = getattr(alert, "primary_outcome_label", None)
    sanitized = _sanitize_outcome_label(label)
    if sanitized:
        return f"p_{sanitized}"
    return "p_outcome0"


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
    if alert.old_price is not None and alert.new_price is not None:
        delta = alert.new_price - alert.old_price
        sign = "+" if delta >= 0 else "-"
        return f"{sign}{abs(delta):.3f}"
    return f"{alert.move:+.3f}"


def _format_liquidity(alert: Alert) -> str:
    return f"Liquidity: ${alert.liquidity:,.0f} | Volume: ${alert.volume_24h:,.0f}"


def _format_market_link(market_id: str) -> str:
    return f"https://polymarket.com/market/{market_id}"


def _format_manual_payload(alert: Alert | None, rec: AiRecommendation) -> dict[str, Any]:
    market_id = alert.market_id if alert else None
    if not _draft_complete(rec):
        return {}
    return {
        "market_id": market_id,
        "token_id": market_id,
        "side": rec.draft_side,
        "price": rec.draft_price,
        "size": rec.draft_size,
        "notional_usd": rec.draft_notional_usd,
        "note": "Manual execution only. Do not send via PMD.",
    }


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


def _draft_missing_inputs(
    alert: Alert | None,
    pref: UserAlertPreference | None,
    user_id: Any,
) -> list[str]:
    missing: list[str] = []
    if not alert:
        return ["alert"]
    price = _select_draft_price(alert)
    if price <= 0:
        missing.append("price")
    if alert.liquidity is None or alert.liquidity <= 0:
        missing.append("liquidity")
    if not pref:
        missing.append("risk_limits")
        return missing
    if pref.max_usd_per_trade <= 0:
        missing.append("max_usd_per_trade")
    if pref.risk_budget_usd_per_day <= 0:
        missing.append("risk_budget_usd_per_day")
    if pref.max_liquidity_fraction <= 0:
        missing.append("max_liquidity_fraction")
    remaining = _risk_budget_remaining(user_id, pref.risk_budget_usd_per_day)
    if remaining <= 0:
        missing.append("daily_remaining_budget")
    return missing


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


def _clear_message_actions(chat_id: str | None, message_id: Any) -> None:
    if not chat_id or not message_id:
        return
    edit_message_reply_markup(str(chat_id), str(message_id), {"inline_keyboard": []})
