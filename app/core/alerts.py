import hashlib
import html
import json
import logging
import re
import time
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from uuid import UUID, uuid4

import httpx
import redis
from rq import Queue
from sqlalchemy import func, inspect
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from ..alerts.theme_key import extract_theme, normalize_text, strip_stopwords
from ..jobs.ai import ai_recommendation_job
from ..models import (
    AiMarketMute,
    AiRecommendation,
    AiThemeMute,
    Alert,
    AlertDelivery,
    MarketSnapshot,
    User,
    UserAlertPreference,
)
from ..settings import settings
from ..http_logging import HttpxTimer, log_httpx_response
from . import defaults
from .ai_copilot import init_copilot_run, log_copilot_run_summary, store_copilot_last_status
from .alert_strength import AlertStrength
from .user_settings import get_effective_user_settings
from .alert_classification import AlertClass, AlertClassification, classify_alert, classify_alert_with_snapshots
from .fast_signals import FAST_ALERT_TYPE
from .market_links import attach_market_slugs, market_url
from .plans import upgrade_target_name
from .signal_speed import SIGNAL_SPEED_FAST, SIGNAL_SPEED_STANDARD, classify_signal_speed

logger = logging.getLogger(__name__)
redis_conn = redis.from_url(settings.REDIS_URL)
queue = Queue("default", connection=redis_conn)
READ_ONLY_DISCLAIMER = "<i>Read-only analytics - Manual execution only - Not financial advice</i>"

USER_DIGEST_LAST_SENT_KEY = "alerts:digest:last_sent:user:{user_id}"
USER_DIGEST_LAST_PAYLOAD_KEY = "alerts:last_digest:user:{user_id}"
TENANT_DIGEST_LAST_PAYLOAD_KEY = "alerts:last_digest:{tenant_id}"
USER_FAST_DIGEST_LAST_SENT_KEY = "alerts:fast:last_sent:user:{user_id}"
DIGEST_SENT_FINGERPRINT_KEY = "digest:sent:{user_id}:{fingerprint_hash}"
COPILOT_THEME_DEDUPE_KEY = "copilot:theme:{user_id}:{theme_key}"
COPILOT_FAST_THEME_DEDUPE_KEY = "copilot:fast:theme:{user_id}:{theme_key}"
COPILOT_LAST_EVAL_KEY = "copilot:last_eval:{user_id}"
COPILOT_DAILY_COUNT_KEY = "copilot:count:{user_id}:{date}"
COPILOT_FAST_DAILY_COUNT_KEY = "copilot:fast:count:{user_id}:{date}"
COPILOT_HOURLY_COUNT_KEY = "copilot:hour:{user_id}:{hour}"
COPILOT_LAST_EVAL_TTL_SECONDS = 60 * 60 * 24

DELIVERY_STATUS_SENT = "sent"
DELIVERY_STATUS_SKIPPED = "skipped"
DELIVERY_STATUS_FILTERED = "filtered"


@dataclass(frozen=True)
class UserDigestConfig:
    user_id: UUID
    name: str
    telegram_chat_id: str | None
    min_liquidity: float
    min_volume_24h: float
    min_abs_price_move: float
    alert_strengths: set[str]
    digest_window_minutes: int
    max_alerts_per_digest: int
    ai_copilot_enabled: bool
    copilot_user_enabled: bool
    copilot_plan_enabled: bool
    fast_signals_enabled: bool
    fast_window_minutes: int
    fast_max_themes_per_digest: int
    fast_max_markets_per_theme: int
    p_min: float
    p_max: float
    plan_name: str | None
    max_copilot_per_day: int
    max_fast_copilot_per_day: int
    max_copilot_per_hour: int
    max_copilot_per_digest: int
    copilot_theme_ttl_minutes: int
    max_themes_per_digest: int
    max_markets_per_theme: int
    p_soft_min: float = defaults.DEFAULT_SOFT_P_MIN
    p_soft_max: float = defaults.DEFAULT_SOFT_P_MAX
    p_strict_min: float = defaults.DEFAULT_STRICT_P_MIN
    p_strict_max: float = defaults.DEFAULT_STRICT_P_MAX
    allow_info_alerts: bool = defaults.DEFAULT_ALLOW_INFO_ALERTS
    allow_fast_alerts: bool = defaults.DEFAULT_ALLOW_FAST_ALERTS


@dataclass
class Theme:
    key: str
    label: str
    alerts: list[Alert]
    representative: Alert
    representative_classification: AlertClassification | None = None
    signal_speed: str | None = None
    tokens: set[str] = field(default_factory=set, repr=False)


@dataclass(frozen=True)
class FastDigestPayload:
    text: str
    window_minutes: int


@dataclass(frozen=True)
class DeliveryDecision:
    deliver: bool
    alert_class: AlertClass
    band_applied: str
    prob_used: float | None
    reason: str | None = None
    within_band: bool = True


class CopilotIneligibilityReason(str, Enum):
    USER_DISABLED = "USER_DISABLED"
    PLAN_DISABLED = "PLAN_DISABLED"
    CAP_REACHED = "CAP_REACHED"
    COPILOT_DEDUPE_ACTIVE = "COPILOT_DEDUPE_ACTIVE"
    MUTED = "MUTED"
    LABEL_MAPPING_UNKNOWN = "LABEL_MAPPING_UNKNOWN"
    NOT_REPRICING = "NOT_REPRICING"
    CONFIDENCE_NOT_HIGH = "CONFIDENCE_NOT_HIGH"
    NOT_FOLLOW = "NOT_FOLLOW"
    P_OUT_OF_BAND = "P_OUT_OF_BAND"
    INSUFFICIENT_SNAPSHOTS = "INSUFFICIENT_SNAPSHOTS"
    MISSING_PRICE_OR_LIQUIDITY = "MISSING_PRICE_OR_LIQUIDITY"


_COPILOT_REASON_ORDER = [
    CopilotIneligibilityReason.USER_DISABLED.value,
    CopilotIneligibilityReason.PLAN_DISABLED.value,
    CopilotIneligibilityReason.CAP_REACHED.value,
    CopilotIneligibilityReason.COPILOT_DEDUPE_ACTIVE.value,
    CopilotIneligibilityReason.MUTED.value,
    CopilotIneligibilityReason.LABEL_MAPPING_UNKNOWN.value,
    CopilotIneligibilityReason.NOT_REPRICING.value,
    CopilotIneligibilityReason.CONFIDENCE_NOT_HIGH.value,
    CopilotIneligibilityReason.NOT_FOLLOW.value,
    CopilotIneligibilityReason.P_OUT_OF_BAND.value,
    CopilotIneligibilityReason.INSUFFICIENT_SNAPSHOTS.value,
    CopilotIneligibilityReason.MISSING_PRICE_OR_LIQUIDITY.value,
]


class FilterReason(str, Enum):
    NO_STRONG_IN_DIGEST = "NO_STRONG_IN_DIGEST"
    STRENGTH_NOT_ALLOWED = "STRENGTH_NOT_ALLOWED"
    LIQUIDITY_BELOW_MIN = "LIQUIDITY_BELOW_MIN"
    VOLUME_BELOW_MIN = "VOLUME_BELOW_MIN"
    ABS_MOVE_BELOW_MIN = "ABS_MOVE_BELOW_MIN"
    P_OUT_OF_BAND = "P_OUT_OF_BAND"
    LABEL_MAPPING_UNKNOWN_BLOCKED = "LABEL_MAPPING_UNKNOWN_BLOCKED"
    MUTED = "MUTED"
    DIGEST_THROTTLED = "DIGEST_THROTTLED"
    COPILOT_CAP_REACHED = "COPILOT_CAP_REACHED"
    TENANT_MISMATCH = "TENANT_MISMATCH"
    PREFS_MISSING_DEFAULTS_USED = "PREFS_MISSING_DEFAULTS_USED"
    NON_ACTIONABLE = "NON_ACTIONABLE"
    INFO_ONLY_BLOCKED = "INFO_ONLY_BLOCKED"
    STRICT_BAND_BLOCKED = "STRICT_BAND_BLOCKED"
    FAST_NOT_ALLOWED = "FAST_NOT_ALLOWED"


COPILOT_RUN_SKIP_NO_ACTIONABLE_THEMES = "NO_ACTIONABLE_THEMES"
COPILOT_RUN_SKIP_NO_ALERTS = "NO_ALERTS"
COPILOT_RUN_SKIP_MISSING_CHAT_ID = "MISSING_CHAT_ID"
COPILOT_RUN_SKIP_RECENT_DIGEST = "DIGEST_RECENTLY_SENT"


@dataclass
class CopilotThemeEvaluation:
    theme_key: str
    market_id: str | None
    signal_speed: str
    reasons: list[str]

    def add_reason(self, reason: str) -> None:
        if reason not in self.reasons:
            self.reasons.append(reason)
            _sort_copilot_reasons(self.reasons)


@dataclass(frozen=True)
class CopilotEnqueueResult:
    evaluations: list[CopilotThemeEvaluation]
    selected_theme_keys: list[str]
    enqueued: int
    eligible_count: int
    cap_reached_reason: str | None


async def send_user_digests(db: Session, tenant_id: str) -> dict | None:
    if not settings.TELEGRAM_BOT_TOKEN:
        return {"ok": False, "reason": "telegram_disabled"}

    users = db.query(User).filter(User.is_active.is_(True)).all()
    if not users:
        return {"ok": True, "users": 0, "sent": 0}

    prefs = _load_user_preferences(db, users)
    results = []
    sent_count = 0
    for user in users:
        config = _resolve_user_preferences(user, prefs.get(user.user_id), db=db)
        now_ts = datetime.now(timezone.utc)
        fast_payload, _ = _prepare_fast_digest(
            db,
            tenant_id,
            config,
            now_ts,
            include_footer=defaults.FAST_DIGEST_MODE == "separate",
        )
        append_fast = defaults.FAST_DIGEST_MODE == "append" and fast_payload is not None
        if append_fast:
            result = await _send_user_digest(db, tenant_id, config, fast_section=fast_payload.text)
            if result.get("sent"):
                _record_fast_digest_sent(config.user_id, now_ts)
        else:
            result = await _send_user_digest(db, tenant_id, config)
            if defaults.FAST_DIGEST_MODE == "separate" and fast_payload is not None:
                fast_result = await _send_user_fast_digest(
                    db,
                    tenant_id,
                    config,
                    fast_payload,
                    now_ts,
                )
                result["fast"] = fast_result
        results.append(result)
        if result.get("sent"):
            sent_count += 1

    return {"ok": True, "users": len(users), "sent": sent_count, "results": results}


def _load_user_preferences(
    db: Session,
    users: list[User],
) -> dict[UUID, UserAlertPreference]:
    user_ids = [user.user_id for user in users]
    if not user_ids:
        return {}
    rows = (
        db.query(UserAlertPreference)
        .filter(UserAlertPreference.user_id.in_(user_ids))
        .all()
    )
    return {row.user_id: row for row in rows}


def _resolve_user_preferences(
    user: User,
    pref: UserAlertPreference | None,
    db: Session | None = None,
) -> UserDigestConfig:
    effective = get_effective_user_settings(user, pref=pref, db=db)
    plan = getattr(user, "plan", None)
    plan_copilot_enabled = True
    if plan is not None and getattr(plan, "copilot_enabled", None) is not None:
        plan_copilot_enabled = bool(plan.copilot_enabled)
    user_copilot_enabled = bool(getattr(user, "copilot_enabled", False))
    min_liquidity = effective.min_liquidity
    min_volume_24h = effective.min_volume_24h
    min_abs_price_move = effective.min_abs_move
    alert_strengths = _normalize_allowed_strengths(effective.allowed_strengths)
    digest_window_minutes = effective.digest_window_minutes
    max_alerts_per_digest = effective.max_alerts_per_digest
    ai_copilot_enabled = effective.copilot_enabled
    fast_mode = str(getattr(effective, "fast_mode", "WATCH_ONLY")).upper()
    if fast_mode not in {"WATCH_ONLY", "FULL"}:
        fast_mode = "WATCH_ONLY"
    fast_signals_enabled = effective.fast_signals_enabled
    allow_fast_alerts = bool(effective.allow_fast_alerts) and fast_mode == "FULL"
    return UserDigestConfig(
        user_id=user.user_id,
        name=user.name,
        telegram_chat_id=user.telegram_chat_id,
        min_liquidity=min_liquidity,
        min_volume_24h=min_volume_24h,
        min_abs_price_move=min_abs_price_move,
        alert_strengths=alert_strengths,
        digest_window_minutes=max(int(digest_window_minutes), 1),
        max_alerts_per_digest=max(int(max_alerts_per_digest), 1),
        ai_copilot_enabled=ai_copilot_enabled,
        copilot_user_enabled=user_copilot_enabled,
        copilot_plan_enabled=plan_copilot_enabled,
        fast_signals_enabled=fast_signals_enabled,
        fast_window_minutes=max(int(effective.fast_window_minutes), 1),
        fast_max_themes_per_digest=max(int(effective.fast_max_themes_per_digest), 1),
        fast_max_markets_per_theme=max(int(effective.fast_max_markets_per_theme), 1),
        p_min=float(effective.p_min),
        p_max=float(effective.p_max),
        p_soft_min=float(effective.p_soft_min),
        p_soft_max=float(effective.p_soft_max),
        p_strict_min=float(effective.p_strict_min),
        p_strict_max=float(effective.p_strict_max),
        allow_info_alerts=bool(effective.allow_info_alerts),
        allow_fast_alerts=allow_fast_alerts,
        plan_name=effective.plan_name,
        max_copilot_per_day=max(int(effective.max_copilot_per_day), 0),
        max_fast_copilot_per_day=max(int(effective.max_fast_copilot_per_day), 0),
        max_copilot_per_hour=max(int(effective.max_copilot_per_hour), 0),
        max_copilot_per_digest=max(int(effective.max_copilot_per_digest), 1),
        copilot_theme_ttl_minutes=max(int(effective.copilot_theme_ttl_minutes), 1),
        max_themes_per_digest=max(int(effective.max_themes_per_digest), 1),
        max_markets_per_theme=max(int(effective.max_markets_per_theme), 1),
    )


def _normalize_allowed_strengths(raw: set[str] | None) -> set[str]:
    allowed = {AlertStrength.STRONG.value, AlertStrength.MEDIUM.value}
    if not raw:
        return allowed
    parts = {str(part).strip().upper() for part in raw if str(part).strip()}
    parsed = {part for part in parts if part in allowed}
    return parsed or {AlertStrength.STRONG.value}


def _note_filter_reason(
    alert: Alert,
    reason: str,
    reason_map: dict[int, list[str]],
    reason_events: list[str],
) -> None:
    reason_events.append(reason)
    if alert.id is None:
        return
    reasons = reason_map.setdefault(alert.id, [])
    if reason not in reasons:
        reasons.append(reason)


async def _send_user_digest(
    db: Session,
    tenant_id: str,
    config: UserDigestConfig,
    fast_section: str | None = None,
) -> dict:
    run_id = str(uuid4())
    run_started_at = time.time()
    metrics = {
        "candidate_alerts_count": 0,
        "after_pref_filter_count": 0,
        "after_strength_gate_count": 0,
        "after_p_band_count": 0,
        "after_actionable_count": 0,
        "after_caps_count": 0,
        "delivered_count": 0,
    }
    filter_reasons_by_alert_id: dict[int, list[str]] = {}
    filter_reason_events: list[str] = []
    window_minutes = max(config.digest_window_minutes, 1)
    now_ts = datetime.now(timezone.utc)
    hourly_limit = max(config.max_copilot_per_hour, 0)
    hourly_count = _get_copilot_hourly_count(config.user_id, now_ts)

    if _digest_recently_sent(config.user_id, now_ts, window_minutes):
        _emit_copilot_run_summary(
            run_id=run_id,
            run_started_at=run_started_at,
            config=config,
            digest_window_minutes=window_minutes,
            themes_total=0,
            themes_candidate=0,
            themes_eligible=0,
            themes_selected=0,
            skipped_by_reason_counts={COPILOT_RUN_SKIP_RECENT_DIGEST: 1},
            daily_count=0,
            daily_limit=max(config.max_copilot_per_day, 0),
            hourly_count=hourly_count,
            hourly_limit=hourly_limit,
            digest_limit=max(config.max_copilot_per_digest, 1),
            llm_calls_attempted=0,
            llm_calls_succeeded=0,
            telegram_sends_attempted=0,
            telegram_sends_succeeded=0,
        )
        _log_digest_metrics(config.user_id, metrics, filter_reason_events)
        return {"user_id": str(config.user_id), "sent": False, "reason": "recent_digest"}

    window_start = now_ts - timedelta(minutes=window_minutes)
    rows = (
        db.query(Alert)
        .filter(
            Alert.tenant_id == tenant_id,
            Alert.created_at >= window_start,
            Alert.alert_type != FAST_ALERT_TYPE,
        )
        .all()
    )
    metrics["candidate_alerts_count"] = len(rows)
    if not rows:
        _emit_copilot_run_summary(
            run_id=run_id,
            run_started_at=run_started_at,
            config=config,
            digest_window_minutes=window_minutes,
            themes_total=0,
            themes_candidate=0,
            themes_eligible=0,
            themes_selected=0,
            skipped_by_reason_counts={COPILOT_RUN_SKIP_NO_ALERTS: 1},
            daily_count=0,
            daily_limit=max(config.max_copilot_per_day, 0),
            hourly_count=hourly_count,
            hourly_limit=hourly_limit,
            digest_limit=max(config.max_copilot_per_digest, 1),
            llm_calls_attempted=0,
            llm_calls_succeeded=0,
            telegram_sends_attempted=0,
            telegram_sends_succeeded=0,
        )
        _log_digest_metrics(config.user_id, metrics, filter_reason_events)
        return {"user_id": str(config.user_id), "sent": False, "reason": "no_alerts"}

    included_alerts, filtered_out, base_filter_map, base_filter_reasons = _filter_alerts_for_user(
        rows,
        config,
    )
    filter_reasons_by_alert_id.update(base_filter_map)
    filter_reason_events.extend(base_filter_reasons)
    metrics["after_pref_filter_count"] = len(included_alerts)
    metrics["after_strength_gate_count"] = len(included_alerts)

    classification_cache: dict[int, AlertClassification] = {}

    def classifier(alert: Alert) -> AlertClassification:
        key = alert.id if alert.id is not None else id(alert)
        cached = classification_cache.get(key)
        if cached:
            return cached
        classification = classify_alert_with_snapshots(db, alert)
        classification_cache[key] = classification
        return classification

    deliverable_candidates: list[Alert] = []
    filtered_by_decision: list[Alert] = []
    for alert in included_alerts:
        classification = classifier(alert)
        decision = _evaluate_delivery_decision(alert, classification, config)
        if decision.deliver:
            deliverable_candidates.append(alert)
        else:
            filtered_by_decision.append(alert)
            if decision.reason:
                _note_filter_reason(
                    alert,
                    decision.reason,
                    filter_reasons_by_alert_id,
                    filter_reason_events,
                )
            _log_alert_filter_decision(alert, classification, decision)
    metrics["after_p_band_count"] = len(deliverable_candidates)

    if not config.telegram_chat_id:
        _emit_copilot_run_summary(
            run_id=run_id,
            run_started_at=run_started_at,
            config=config,
            digest_window_minutes=window_minutes,
            themes_total=0,
            themes_candidate=0,
            themes_eligible=0,
            themes_selected=0,
            skipped_by_reason_counts={COPILOT_RUN_SKIP_MISSING_CHAT_ID: 1},
            daily_count=0,
            daily_limit=max(config.max_copilot_per_day, 0),
            hourly_count=hourly_count,
            hourly_limit=hourly_limit,
            digest_limit=max(config.max_copilot_per_digest, 1),
            llm_calls_attempted=0,
            llm_calls_succeeded=0,
            telegram_sends_attempted=0,
            telegram_sends_succeeded=0,
        )
        _record_alert_deliveries(
            db,
            filtered_out + filtered_by_decision,
            deliverable_candidates,
            config.user_id,
            now_ts,
            sent_alert_ids=set(),
            skip_reason="missing_chat_id",
            filter_reasons=filter_reasons_by_alert_id,
        )
        _log_digest_metrics(config.user_id, metrics, filter_reason_events)
        return {"user_id": str(config.user_id), "sent": False, "reason": "missing_chat_id"}

    actionable_alerts: list[Alert] = []
    info_only_alerts: list[Alert] = []
    for alert in deliverable_candidates:
        classification = classifier(alert)
        alert_class = _resolve_alert_class(classification)
        if alert_class == AlertClass.INFO_ONLY:
            info_only_alerts.append(alert)
        else:
            actionable_alerts.append(alert)

    actionable_ranked = _dedupe_by_market_id(_rank_alerts(actionable_alerts))
    info_ranked = _dedupe_by_market_id(
        _rank_alerts(info_only_alerts),
        exclude_market_ids={alert.market_id for alert in actionable_ranked},
    )
    total_actionable = len(actionable_ranked)
    metrics["after_actionable_count"] = total_actionable
    filtered_for_delivery = filtered_out + filtered_by_decision
    included_for_delivery = deliverable_candidates

    max_alerts = min(
        max(config.max_alerts_per_digest, 1),
        max(defaults.MAX_ACTIONABLE_PER_DIGEST, 1),
    )

    selected_actionable = actionable_ranked[:max_alerts]
    for alert in actionable_ranked[max_alerts:]:
        _note_filter_reason(
            alert,
            FilterReason.DIGEST_THROTTLED.value,
            filter_reasons_by_alert_id,
            filter_reason_events,
        )
        filtered_for_delivery.append(alert)
    selected_alerts = list(selected_actionable)
    remaining_slots = max(max_alerts - len(selected_actionable), 0)
    info_slots = max_alerts if not selected_actionable else remaining_slots
    if config.allow_info_alerts and info_slots > 0 and info_ranked:
        selected_info = info_ranked[:info_slots]
        selected_alerts.extend(selected_info)
        for alert in info_ranked[info_slots:]:
            _note_filter_reason(
                alert,
                FilterReason.DIGEST_THROTTLED.value,
                filter_reasons_by_alert_id,
                filter_reason_events,
            )
            filtered_for_delivery.append(alert)

    copilot_result: CopilotEnqueueResult | None = None
    if actionable_alerts:
        copilot_result = _enqueue_ai_recommendations(
            db,
            config,
            actionable_alerts,
            classifier,
            allow_enqueue=config.ai_copilot_enabled,
            run_id=run_id,
            run_started_at=run_started_at,
            digest_window_minutes=window_minutes,
        )
    else:
        _emit_copilot_run_summary(
            run_id=run_id,
            run_started_at=run_started_at,
            config=config,
            digest_window_minutes=window_minutes,
            themes_total=0,
            themes_candidate=0,
            themes_eligible=0,
            themes_selected=0,
            skipped_by_reason_counts={COPILOT_RUN_SKIP_NO_ACTIONABLE_THEMES: 1},
            daily_count=0,
            daily_limit=max(config.max_copilot_per_day, 0),
            hourly_count=hourly_count,
            hourly_limit=hourly_limit,
            digest_limit=max(config.max_copilot_per_digest, 1),
            llm_calls_attempted=0,
            llm_calls_succeeded=0,
            telegram_sends_attempted=0,
            telegram_sends_succeeded=0,
        )
    metrics["after_caps_count"] = len(selected_alerts)
    attach_market_slugs(db, selected_alerts)

    if not selected_alerts:
        _record_alert_deliveries(
            db,
            filtered_for_delivery,
            included_for_delivery,
            config.user_id,
            now_ts,
            sent_alert_ids=set(),
            skip_reason="no_selected_alerts",
            filter_reasons=filter_reasons_by_alert_id,
        )
        _log_digest_metrics(config.user_id, metrics, filter_reason_events)
        return {"user_id": str(config.user_id), "sent": False, "reason": "no_selected_alerts"}

    counts = {"REPRICING": 0, "LIQUIDITY_SWEEP": 0, "NOISY": 0}
    for alert in selected_alerts:
        classification = classifier(alert)
        counts[classification.signal_type] = counts.get(classification.signal_type, 0) + 1
    logger.debug(
        "alert_classification_summary user_id=%s repricing=%s liquidity_sweep=%s noisy=%s total=%s",
        config.user_id,
        counts.get("REPRICING", 0),
        counts.get("LIQUIDITY_SWEEP", 0),
        counts.get("NOISY", 0),
        len(selected_alerts),
    )

    text = _format_digest_message(
        selected_alerts,
        window_minutes,
        total_actionable=total_actionable,
        user_name=config.name,
        max_themes_per_digest=config.max_themes_per_digest,
        max_markets_per_theme=config.max_markets_per_theme,
        classifier=classifier,
        db=db,
        now_ts=now_ts,
    )
    if not text:
        _record_alert_deliveries(
            db,
            filtered_for_delivery,
            included_for_delivery,
            config.user_id,
            now_ts,
            sent_alert_ids=set(),
            skip_reason="empty_message",
            filter_reasons=filter_reasons_by_alert_id,
        )
        _log_digest_metrics(config.user_id, metrics, filter_reason_events)
        return {"user_id": str(config.user_id), "sent": False, "reason": "empty_message"}

    copilot_note = _copilot_skip_note(config, copilot_result)
    text = _append_copilot_note(text, copilot_note)

    if fast_section:
        text = _append_fast_section(text, fast_section)

    if not _claim_digest_fingerprint(config.user_id, window_minutes, selected_alerts):
        _record_alert_deliveries(
            db,
            filtered_for_delivery,
            included_for_delivery,
            config.user_id,
            now_ts,
            sent_alert_ids=set(),
            skip_reason="digest_dedupe",
            filter_reasons=filter_reasons_by_alert_id,
        )
        _log_digest_metrics(config.user_id, metrics, filter_reason_events)
        return {"user_id": str(config.user_id), "sent": False, "reason": "digest_dedupe"}

    url = f"https://api.telegram.org/bot{settings.TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": config.telegram_chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            timer = HttpxTimer()
            response = await client.post(url, json=payload)
        log_httpx_response(response, timer.elapsed(), log_error=False)
        if response.is_success:
            sent_alert_ids = {alert.id for alert in selected_alerts if alert.id is not None}
            _record_digest_sent(config.user_id, tenant_id, now_ts, window_minutes, actionable_alerts, [])
            _record_alert_deliveries(
                db,
                filtered_for_delivery,
                included_for_delivery,
                config.user_id,
                now_ts,
                sent_alert_ids=sent_alert_ids,
                filter_reasons=filter_reasons_by_alert_id,
            )
            metrics["delivered_count"] = len(sent_alert_ids)
            _log_digest_metrics(config.user_id, metrics, filter_reason_events)
            logger.info(
                "digest_sent user_id=%s window_minutes=%s actionable=%s",
                config.user_id,
                window_minutes,
                len(actionable_alerts),
            )
            return {"user_id": str(config.user_id), "sent": True, "status_code": response.status_code}

        logger.error(
            "telegram_send_failed user_id=%s status=%s body=%s",
            config.user_id,
            response.status_code,
            response.text[:500],
        )
        _record_alert_deliveries(
            db,
            filtered_for_delivery,
            included_for_delivery,
            config.user_id,
            now_ts,
            sent_alert_ids=set(),
            skip_reason="telegram_failed",
            filter_reasons=filter_reasons_by_alert_id,
        )
        _log_digest_metrics(config.user_id, metrics, filter_reason_events)
        return {
            "user_id": str(config.user_id),
            "sent": False,
            "status_code": response.status_code,
            "text": response.text[:200],
        }
    except Exception:
        logger.exception("telegram_send_failed user_id=%s", config.user_id)
        _record_alert_deliveries(
            db,
            filtered_for_delivery,
            included_for_delivery,
            config.user_id,
            now_ts,
            sent_alert_ids=set(),
            skip_reason="telegram_exception",
            filter_reasons=filter_reasons_by_alert_id,
        )
        _log_digest_metrics(config.user_id, metrics, filter_reason_events)
        return {"user_id": str(config.user_id), "sent": False, "status_code": 0, "text": ""}


def _filter_alerts_for_user(
    alerts: list[Alert],
    config: UserDigestConfig,
) -> tuple[list[Alert], list[Alert], dict[int, list[str]], list[str]]:
    included: list[Alert] = []
    filtered: list[Alert] = []
    reasons_by_alert_id: dict[int, list[str]] = {}
    reason_events: list[str] = []

    for alert in alerts:
        if alert.liquidity < config.min_liquidity:
            filtered.append(alert)
            _note_filter_reason(
                alert,
                FilterReason.LIQUIDITY_BELOW_MIN.value,
                reasons_by_alert_id,
                reason_events,
            )
            continue
        if alert.volume_24h < config.min_volume_24h:
            filtered.append(alert)
            _note_filter_reason(
                alert,
                FilterReason.VOLUME_BELOW_MIN.value,
                reasons_by_alert_id,
                reason_events,
            )
            continue
        if _alert_abs_move(alert) < config.min_abs_price_move:
            filtered.append(alert)
            _note_filter_reason(
                alert,
                FilterReason.ABS_MOVE_BELOW_MIN.value,
                reasons_by_alert_id,
                reason_events,
            )
            continue
        if alert.strength not in config.alert_strengths:
            filtered.append(alert)
            _note_filter_reason(
                alert,
                FilterReason.STRENGTH_NOT_ALLOWED.value,
                reasons_by_alert_id,
                reason_events,
            )
            continue
        included.append(alert)

    return included, filtered, reasons_by_alert_id, reason_events


def _alert_abs_move(alert: Alert) -> float:
    if alert.old_price is not None and alert.new_price is not None:
        return abs(alert.new_price - alert.old_price)
    if alert.delta_pct is not None:
        return abs(alert.delta_pct)
    return abs(alert.move or 0.0)


def _digest_recently_sent(user_id: UUID, now_ts: datetime, window_minutes: int) -> bool:
    key = USER_DIGEST_LAST_SENT_KEY.format(user_id=user_id)
    try:
        last_sent_raw = redis_conn.get(key)
        if not last_sent_raw:
            return False
        last_sent = datetime.fromisoformat(last_sent_raw.decode())
        return (now_ts - last_sent).total_seconds() < window_minutes * 60
    except Exception:
        logger.exception("digest_last_sent_lookup_failed user_id=%s", user_id)
        return False


def _claim_digest_fingerprint(
    user_id: UUID,
    window_minutes: int,
    alerts: list[Alert],
) -> bool:
    fingerprint_hash = _digest_fingerprint_hash(window_minutes, alerts)
    if not fingerprint_hash:
        return True
    ttl_seconds = max(int(window_minutes * 90), 60)
    key = DIGEST_SENT_FINGERPRINT_KEY.format(user_id=user_id, fingerprint_hash=fingerprint_hash)
    try:
        marked = redis_conn.set(key, "1", nx=True, ex=ttl_seconds)
        return bool(marked)
    except Exception:
        logger.exception("digest_fingerprint_write_failed user_id=%s", user_id)
        return True


def _digest_fingerprint_hash(window_minutes: int, alerts: list[Alert]) -> str:
    if not alerts:
        return ""
    themes: dict[str, list[Alert]] = {}
    for alert in alerts:
        theme_key = _theme_key(alert)
        themes.setdefault(theme_key, []).append(alert)

    theme_items: list[dict[str, str]] = []
    for theme_key, theme_alerts in themes.items():
        rep = max(
            theme_alerts,
            key=lambda item: (
                item.liquidity,
                item.volume_24h,
                item.market_id or "",
            ),
        )
        theme_items.append(
            {
                "theme_key": theme_key,
                "market_id": rep.market_id or "",
                "bucket": _digest_bucket_value(rep),
            }
        )

    theme_items.sort(key=lambda item: item["theme_key"])
    payload = {
        "window_minutes": int(window_minutes),
        "themes": theme_items,
    }
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _digest_bucket_value(alert: Alert) -> str:
    value = getattr(alert, "snapshot_bucket", None) or getattr(alert, "triggered_at", None)
    if isinstance(value, datetime):
        return value.isoformat()
    if value is None:
        return ""
    return str(value)


def _record_digest_sent(
    user_id: UUID,
    tenant_id: str,
    sent_at: datetime,
    window_minutes: int,
    strong_alerts: list[Alert],
    medium_alerts: list[Alert],
) -> None:
    payload = {
        "sent_at": sent_at.isoformat(),
        "window_minutes": window_minutes,
        "strong": len(strong_alerts),
        "medium": len(medium_alerts),
    }
    last_sent_key = USER_DIGEST_LAST_SENT_KEY.format(user_id=user_id)
    last_payload_key = USER_DIGEST_LAST_PAYLOAD_KEY.format(user_id=user_id)
    tenant_payload_key = TENANT_DIGEST_LAST_PAYLOAD_KEY.format(tenant_id=tenant_id)
    try:
        redis_conn.set(last_sent_key, sent_at.isoformat())
        redis_conn.set(last_payload_key, json.dumps(payload, ensure_ascii=True))
        redis_conn.set(tenant_payload_key, json.dumps(payload, ensure_ascii=True))
    except Exception:
        logger.exception("digest_state_write_failed user_id=%s", user_id)


def _fast_digest_recently_sent(user_id: UUID, now_ts: datetime, window_minutes: int) -> bool:
    key = USER_FAST_DIGEST_LAST_SENT_KEY.format(user_id=user_id)
    try:
        last_sent_raw = redis_conn.get(key)
        if not last_sent_raw:
            return False
        last_sent = datetime.fromisoformat(last_sent_raw.decode())
        return (now_ts - last_sent).total_seconds() < window_minutes * 60
    except Exception:
        logger.exception("fast_digest_last_sent_lookup_failed user_id=%s", user_id)
        return False


def _record_fast_digest_sent(user_id: UUID, sent_at: datetime) -> None:
    key = USER_FAST_DIGEST_LAST_SENT_KEY.format(user_id=user_id)
    try:
        redis_conn.set(key, sent_at.isoformat())
    except Exception:
        logger.exception("fast_digest_state_write_failed user_id=%s", user_id)


def _record_alert_deliveries(
    db: Session,
    filtered_alerts: list[Alert],
    included_alerts: list[Alert],
    user_id: UUID,
    delivered_at: datetime,
    sent_alert_ids: set[int],
    skip_reason: str | None = None,
    filter_reasons: dict[int, list[str]] | None = None,
) -> None:
    # Use dict keyed by alert_id to deduplicate - later entries win
    # This prevents CardinalityViolation when same alert appears in both lists
    rows_by_alert: dict[int, dict] = {}
    filter_reasons = filter_reasons or {}
    for alert in filtered_alerts:
        if alert.id is None:
            continue
        rows_by_alert[alert.id] = {
            "alert_id": alert.id,
            "user_id": user_id,
            "delivered_at": delivered_at,
            "delivery_status": DELIVERY_STATUS_FILTERED,
            "filter_reasons": filter_reasons.get(alert.id, []),
        }

    for alert in included_alerts:
        if alert.id is None:
            continue
        status = DELIVERY_STATUS_SENT if alert.id in sent_alert_ids else DELIVERY_STATUS_SKIPPED
        rows_by_alert[alert.id] = {
            "alert_id": alert.id,
            "user_id": user_id,
            "delivered_at": delivered_at,
            "delivery_status": status,
            "filter_reasons": filter_reasons.get(alert.id, []),
        }

    rows = list(rows_by_alert.values())
    if not rows:
        return

    if skip_reason:
        logger.info("digest_delivery_skipped user_id=%s reason=%s", user_id, skip_reason)

    stmt = pg_insert(AlertDelivery).values(rows)
    stmt = stmt.on_conflict_do_update(
        index_elements=["alert_id", "user_id"],
        set_={
            "delivered_at": delivered_at,
            "delivery_status": stmt.excluded.delivery_status,
            "filter_reasons": stmt.excluded.filter_reasons,
        },
    )
    db.execute(stmt)
    db.commit()


def _log_digest_metrics(
    user_id: UUID,
    metrics: dict,
    filter_reason_events: list[str] | None,
) -> None:
    counter = Counter(filter_reason_events or [])
    top_reasons = counter.most_common(3)
    top_summary = ",".join(f"{reason}:{count}" for reason, count in top_reasons)
    logger.info(
        (
            "digest_metrics user_id=%s candidate_alerts_count=%s "
            "after_pref_filter_count=%s after_strength_gate_count=%s after_p_band_count=%s "
            "after_actionable_count=%s after_caps_count=%s delivered_count=%s top_filter_reasons=%s"
        ),
        user_id,
        metrics.get("candidate_alerts_count", 0),
        metrics.get("after_pref_filter_count", 0),
        metrics.get("after_strength_gate_count", 0),
        metrics.get("after_p_band_count", 0),
        metrics.get("after_actionable_count", 0),
        metrics.get("after_caps_count", 0),
        metrics.get("delivered_count", 0),
        top_summary or "none",
    )


def _prepare_fast_digest(
    db: Session,
    tenant_id: str,
    config: UserDigestConfig,
    now_ts: datetime,
    include_footer: bool,
) -> tuple[FastDigestPayload | None, str | None]:
    if not settings.FAST_SIGNALS_GLOBAL_ENABLED or not config.fast_signals_enabled:
        return None, "fast_disabled"

    window_minutes = max(config.fast_window_minutes, 1)
    if _fast_digest_recently_sent(config.user_id, now_ts, window_minutes):
        return None, "recent_fast_digest"

    window_start = now_ts - timedelta(minutes=window_minutes)
    rows = (
        db.query(Alert)
        .filter(
            Alert.tenant_id == tenant_id,
            Alert.alert_type == FAST_ALERT_TYPE,
            Alert.created_at >= window_start,
        )
        .all()
    )
    if not rows:
        return None, "no_fast_alerts"

    filtered = []
    for alert in rows:
        if alert.liquidity < defaults.FAST_MIN_LIQUIDITY:
            continue
        if alert.volume_24h < defaults.FAST_MIN_VOLUME_24H:
            continue
        if alert.market_p_yes is not None and not (
            defaults.FAST_PYES_MIN <= alert.market_p_yes <= defaults.FAST_PYES_MAX
        ):
            continue
        if _alert_abs_move(alert) < defaults.FAST_MIN_ABS_MOVE:
            continue
        if alert.delta_pct is not None and abs(alert.delta_pct) < defaults.FAST_MIN_PCT_MOVE:
            continue
        filtered.append(alert)

    if not filtered:
        return None, "no_fast_alerts"

    attach_market_slugs(db, filtered)
    text = _format_fast_digest_message(
        filtered,
        window_minutes,
        max_themes_per_digest=config.fast_max_themes_per_digest,
        max_markets_per_theme=config.fast_max_markets_per_theme,
        include_footer=include_footer,
    )
    if not text:
        return None, "empty_fast_message"

    return FastDigestPayload(text=text, window_minutes=window_minutes), None


async def _send_user_fast_digest(
    db: Session,
    tenant_id: str,
    config: UserDigestConfig,
    payload: FastDigestPayload,
    now_ts: datetime,
) -> dict:
    if not config.telegram_chat_id:
        return {"user_id": str(config.user_id), "sent": False, "reason": "missing_chat_id"}

    if not payload.text:
        return {"user_id": str(config.user_id), "sent": False, "reason": "empty_message"}

    url = f"https://api.telegram.org/bot{settings.TELEGRAM_BOT_TOKEN}/sendMessage"
    send_payload = {
        "chat_id": config.telegram_chat_id,
        "text": payload.text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            timer = HttpxTimer()
            response = await client.post(url, json=send_payload)
        log_httpx_response(response, timer.elapsed(), log_error=False)
        if response.is_success:
            _record_fast_digest_sent(config.user_id, now_ts)
            logger.info(
                "fast_digest_sent user_id=%s window_minutes=%s",
                config.user_id,
                payload.window_minutes,
            )
            return {"user_id": str(config.user_id), "sent": True, "status_code": response.status_code}

        logger.error(
            "fast_telegram_send_failed user_id=%s status=%s body=%s",
            config.user_id,
            response.status_code,
            response.text[:500],
        )
        return {
            "user_id": str(config.user_id),
            "sent": False,
            "status_code": response.status_code,
            "text": response.text[:200],
        }
    except Exception:
        logger.exception("fast_telegram_send_failed user_id=%s", config.user_id)
        return {"user_id": str(config.user_id), "sent": False, "status_code": 0, "text": ""}


def _rank_alerts(alerts: list[Alert]) -> list[Alert]:
    return sorted(
        alerts,
        key=lambda alert: (
            abs(alert.new_price - alert.old_price),
            alert.liquidity,
            alert.volume_24h,
        ),
        reverse=True,
    )


def _score_copilot_theme(theme: Theme) -> float:
    rep = theme.representative
    abs_move = abs(_signed_price_delta(rep))
    if abs_move <= 0:
        abs_move = abs(rep.move or 0.0)
    liquidity = rep.liquidity or 0.0
    volume = rep.volume_24h or 0.0
    sustained = max(int(getattr(rep, "sustained_snapshots", 1) or 1), 1)

    def _factor(value: float, threshold: float) -> float:
        if threshold <= 0:
            return 1.0
        return 1.0 + min(value / threshold, 3.0)

    liquidity_factor = _factor(liquidity, defaults.STRONG_MIN_LIQUIDITY)
    volume_factor = _factor(volume, defaults.STRONG_MIN_VOLUME_24H)
    sustained_factor = float(sustained)
    return abs_move * liquidity_factor * volume_factor * sustained_factor


def _rotate_themes_by_category(themes: list[Theme], limit: int) -> list[Theme]:
    if limit <= 0:
        return []
    buckets: dict[str, list[Theme]] = {}
    order: list[str] = []
    for theme in themes:
        category = (theme.representative.category or "unknown").lower()
        if category not in buckets:
            buckets[category] = []
            order.append(category)
        buckets[category].append(theme)
    selected: list[Theme] = []
    while len(selected) < limit and order:
        made_progress = False
        for category in list(order):
            bucket = buckets.get(category, [])
            if not bucket:
                if category in order:
                    order.remove(category)
                continue
            selected.append(bucket.pop(0))
            made_progress = True
            if len(selected) >= limit:
                break
        if not made_progress:
            break
    return selected


def _dedupe_by_market_id(
    alerts: list[Alert],
    exclude_market_ids: set[str] | None = None,
) -> list[Alert]:
    seen: set[str] = set(exclude_market_ids or set())
    deduped: list[Alert] = []
    for alert in alerts:
        if alert.market_id in seen:
            continue
        seen.add(alert.market_id)
        deduped.append(alert)
    return deduped


def _format_digest_message(
    alerts: list[Alert],
    window_minutes: int,
    total_actionable: int,
    user_name: str | None = None,
    max_themes_per_digest: int | None = None,
    max_markets_per_theme: int | None = None,
    classifier=None,
    db: Session | None = None,
    now_ts: datetime | None = None,
) -> str:
    if not alerts:
        return ""

    actionable_included = 0
    for alert in alerts:
        classification = classifier(alert) if classifier else classify_alert(alert)
        if _is_actionable_classification(classification):
            actionable_included += 1

    if defaults.THEME_GROUPING_ENABLED:
        return _format_grouped_digest_message(
            alerts,
            window_minutes,
            max_themes_per_digest,
            max_markets_per_theme,
            classifier,
            db=db,
            now_ts=now_ts,
        )

    header_label = f"{actionable_included} actionable repricings" if actionable_included else f"{len(alerts)} signals"
    header = f"<b>PMD - {header_label} ({window_minutes}m)</b>"
    lines = [header, ""]

    actionable_displayed = 0
    for idx, alert in enumerate(alerts, start=1):
        classification = classifier(alert) if classifier else classify_alert(alert)
        if _is_actionable_classification(classification):
            actionable_displayed += 1
        lines.extend(_format_digest_alert(alert, idx, window_minutes, classifier, classification=classification))
        lines.append("")

    remaining_actionable = total_actionable - actionable_displayed
    if remaining_actionable > 0:
        lines.extend([f"+{remaining_actionable} more actionable repricings not shown", ""])

    lines.append(READ_ONLY_DISCLAIMER)
    return "\n".join(lines).strip()


def _append_fast_section(confirmed_text: str, fast_section: str) -> str:
    if not fast_section:
        return confirmed_text
    footer = READ_ONLY_DISCLAIMER
    if footer in confirmed_text:
        base, _ = confirmed_text.rsplit(footer, 1)
        base = base.rstrip()
        fast_section = fast_section.strip()
        return f"{base}\n\n{fast_section}\n\n{footer}"
    return f"{confirmed_text}\n\n{fast_section}"


def _append_copilot_note(confirmed_text: str, note: str | None) -> str:
    if not note:
        return confirmed_text
    footer = READ_ONLY_DISCLAIMER
    if footer in confirmed_text:
        base, _ = confirmed_text.rsplit(footer, 1)
        base = base.rstrip()
        return f"{base}\n\n{note}\n\n{footer}"
    return f"{confirmed_text}\n\n{note}"


def _format_fast_digest_message(
    alerts: list[Alert],
    window_minutes: int,
    max_themes_per_digest: int,
    max_markets_per_theme: int,
    include_footer: bool = True,
) -> str:
    if not alerts:
        return ""

    themes = group_alerts_into_themes(alerts)
    max_themes = max(int(max_themes_per_digest), 1)
    themes = themes[:max_themes]
    if not themes:
        return ""

    header = (
        f"<b>PMD - FAST: {len(themes)} watchlist theme{'' if len(themes) == 1 else 's'} "
        f"({window_minutes}m)</b>"
    )
    lines = [header, ""]

    for idx, theme in enumerate(themes, start=1):
        label = html.escape(theme.label)
        lines.append(
            f"<b>#{idx} FAST - {label} watchlist ({len(theme.alerts)} market{'' if len(theme.alerts) == 1 else 's'})</b>"
        )
        rep = theme.representative
        rep_title = html.escape(_short_title(rep, theme_hint=True))
        rep_move = _format_compact_move(rep)
        rep_p_yes = _format_p_yes_compact(rep)
        rep_liq, rep_vol = _format_liquidity_descriptors(rep)
        confidence = _fast_confidence_label(rep.strength)
        lines.append(
            f"Rep: {rep_title} | Move {rep_move} | {rep_p_yes} | Liq {rep_liq} | Vol {rep_vol} | WATCH ({confidence})"
        )
        related = [alert for alert in theme.alerts if alert is not rep]
        for related_alert in related[: max(int(max_markets_per_theme), 1)]:
            bullet_title = html.escape(_short_title(related_alert, theme_hint=True))
            bullet_move = _format_compact_move(related_alert)
            bullet_p_yes = _format_p_yes_compact(related_alert)
            lines.append(f"- {bullet_title} | {bullet_move} | {bullet_p_yes}")
        lines.append(_format_market_link(rep.market_id, getattr(rep, "market_slug", None)))
        lines.append("")

    if include_footer:
        lines.append(READ_ONLY_DISCLAIMER)
    return "\n".join(lines).strip()


def _fast_confidence_label(raw: str | None) -> str:
    normalized = (raw or "").upper()
    if normalized == "MEDIUM":
        return "MEDIUM"
    return "LOW"


def _format_grouped_digest_message(
    alerts: list[Alert],
    window_minutes: int,
    max_themes_per_digest: int | None = None,
    max_markets_per_theme: int | None = None,
    classifier=None,
    db: Session | None = None,
    now_ts: datetime | None = None,
) -> str:
    themes = group_alerts_into_themes(alerts, classifier)
    max_themes = max(max_themes_per_digest or defaults.DEFAULT_MAX_THEMES_PER_DIGEST, 1)
    themes = themes[:max_themes]
    header = f"<b>PMD - {len(themes)} theme{'' if len(themes) == 1 else 's'} ({window_minutes}m)</b>"
    lines = [header, ""]
    evidence_by_market: dict[str, str] = {}
    if db is not None and now_ts is not None and _db_has_table(db, MarketSnapshot.__tablename__):
        window_start = now_ts - timedelta(minutes=window_minutes)
        evidence_by_market = _build_theme_digest_evidence(db, themes, window_start, now_ts)

    for idx, theme in enumerate(themes, start=1):
        label = html.escape(theme.label)
        lines.append(
            f"<b>#{idx} THEME - {label} ({len(theme.alerts)} market{'' if len(theme.alerts) == 1 else 's'})</b>"
        )
        rep = theme.representative
        rep_title = html.escape(_short_title(rep, theme_hint=True))
        rep_move = _format_compact_move(rep)
        rep_p_yes = _format_p_yes_compact(rep)
        rep_liq, rep_vol = _format_liquidity_descriptors(rep)
        lines.append(
            f"Rep: {rep_title} | Move {rep_move} | {rep_p_yes} | Liq {rep_liq} | Vol {rep_vol}"
        )
        evidence_line = evidence_by_market.get(rep.market_id)
        if evidence_line:
            lines.append(evidence_line)
        related = [alert for alert in theme.alerts if alert is not rep]
        max_markets = max_markets_per_theme or defaults.DEFAULT_MAX_MARKETS_PER_THEME
        for related_alert in related[: max(int(max_markets), 1)]:
            bullet_title = html.escape(_short_title(related_alert, theme_hint=True))
            bullet_move = _format_compact_move(related_alert)
            bullet_p_yes = _format_p_yes_compact(related_alert)
            lines.append(f"- {bullet_title} | {bullet_move} | {bullet_p_yes}")
        lines.append(_format_market_link(rep.market_id, getattr(rep, "market_slug", None)))
        lines.append("")

    lines.append(READ_ONLY_DISCLAIMER)
    return "\n".join(lines).strip()


def _db_has_table(db: Session, table_name: str) -> bool:
    try:
        inspector = inspect(db.bind)
        return inspector.has_table(table_name)
    except Exception:
        logger.exception("db_table_inspect_failed table=%s", table_name)
        return False


def _build_theme_digest_evidence(
    db: Session,
    themes: list[Theme],
    window_start: datetime,
    window_end: datetime,
) -> dict[str, str]:
    market_ids = [theme.representative.market_id for theme in themes if theme.representative.market_id]
    if not market_ids:
        return {}
    rows = (
        db.query(MarketSnapshot.market_id, MarketSnapshot.snapshot_bucket, MarketSnapshot.market_p_yes)
        .filter(
            MarketSnapshot.market_id.in_(market_ids),
            MarketSnapshot.snapshot_bucket >= window_start,
            MarketSnapshot.snapshot_bucket <= window_end,
            MarketSnapshot.market_p_yes.isnot(None),
        )
        .order_by(MarketSnapshot.market_id.asc(), MarketSnapshot.snapshot_bucket.asc())
        .all()
    )
    points_by_market: dict[str, list[tuple[datetime, float]]] = {}
    for market_id, bucket, price in rows:
        points_by_market.setdefault(market_id, []).append((bucket, price))

    evidence_by_market: dict[str, str] = {}
    for theme in themes:
        rep = theme.representative
        if not rep.market_id:
            continue
        points = points_by_market.get(rep.market_id, [])
        direction = _alert_direction(rep)
        sustained = _sustained_snapshot_count(points, direction)
        reversal = _reversal_flag(points, direction)
        evidence_by_market[rep.market_id] = _format_theme_evidence_line(sustained, reversal)
    return evidence_by_market


def _alert_direction(alert: Alert) -> int:
    if alert.new_price is not None and alert.old_price is not None:
        return 1 if alert.new_price - alert.old_price >= 0 else -1
    return 1 if (alert.move or 0.0) >= 0 else -1


def _sustained_snapshot_count(
    points: list[tuple[datetime, float]],
    direction: int,
) -> int:
    if not points:
        return 1
    if len(points) < 2:
        return 1
    streak = 1
    for idx in range(len(points) - 1, 0, -1):
        delta = points[idx][1] - points[idx - 1][1]
        if delta * direction > 0:
            streak += 1
        else:
            break
    return max(streak, 1)


def _reversal_flag(points: list[tuple[datetime, float]], direction: int) -> str:
    if len(points) < 2:
        return "none"
    baseline = points[0][1]
    last = points[-1][1]
    if direction >= 0:
        peak = max(price for _, price in points)
        total_move = peak - baseline
        if total_move <= 0:
            return "none"
        retrace_last = (peak - last) / total_move
    else:
        peak = min(price for _, price in points)
        total_move = baseline - peak
        if total_move <= 0:
            return "none"
        retrace_last = (last - peak) / total_move

    if retrace_last >= 1.0:
        return "full"
    if retrace_last >= 0.5:
        return "partial"
    return "none"


def _format_theme_evidence_line(sustained_snapshots: int, reversal_flag: str) -> str:
    return f"Evidence: sustained_snapshots={sustained_snapshots} | reversal_flag={reversal_flag}"


def _format_digest_alert(
    alert: Alert,
    idx: int,
    window_minutes: int,
    classifier=None,
    classification: AlertClassification | None = None,
) -> list[str]:
    title = html.escape(alert.title[:120])
    classification = classification or (classifier(alert) if classifier else classify_alert(alert))
    rank_label = _rank_label_for_class(_resolve_alert_class(classification))
    move_text = _format_move(alert)
    p_yes_text = _format_p_yes(alert)
    liquidity_text = _format_liquidity_volume(alert)
    return [
        f"<b>#{idx} {rank_label} - {classification.signal_type} ({classification.confidence})</b>",
        title,
        f"Move: {move_text} | {p_yes_text}",
        liquidity_text,
        f"Suggested action: {classification.suggested_action}",
        _format_market_link(alert.market_id, getattr(alert, "market_slug", None)),
    ]


def _rank_label_for_class(alert_class: AlertClass | None) -> str:
    if alert_class == AlertClass.ACTIONABLE_FAST:
        return "FAST"
    if alert_class == AlertClass.ACTIONABLE_STANDARD:
        return "ACTIONABLE"
    return "INFO"


def _format_compact_usd(value: float) -> str:
    abs_value = abs(value)
    if abs_value >= 1_000_000_000:
        return f"${value / 1_000_000_000:.2f}B"
    if abs_value >= 1_000_000:
        return f"${value / 1_000_000:.2f}M"
    if abs_value >= 1_000:
        return f"${value / 1_000:.1f}k"
    return f"${value:,.0f}"


def _rank_digest_alerts(alerts: list[Alert]) -> list[Alert]:
    def _strength_weight(alert: Alert) -> int:
        return 1 if alert.strength == AlertStrength.STRONG.value else 0

    return sorted(
        alerts,
        key=lambda alert: (
            _strength_weight(alert),
            abs(_signed_price_delta(alert)),
            alert.liquidity,
            alert.volume_24h,
        ),
        reverse=True,
    )


def _resolve_alert_class(classification: AlertClassification) -> AlertClass:
    if classification.alert_class is not None:
        return classification.alert_class
    signal = (classification.signal_type or "").upper()
    confidence = (classification.confidence or "").upper()
    if signal in {"REPRICING", "LIQUIDITY_SWEEP"} and confidence == "HIGH":
        return AlertClass.ACTIONABLE_FAST
    if signal in {"REPRICING", "LIQUIDITY_SWEEP"}:
        return AlertClass.ACTIONABLE_STANDARD
    if signal == "MOMENTUM" and confidence in {"HIGH", "MEDIUM"}:
        return AlertClass.ACTIONABLE_STANDARD
    return AlertClass.INFO_ONLY


def _evaluate_delivery_decision(
    alert: Alert,
    classification: AlertClassification,
    config: UserDigestConfig,
) -> DeliveryDecision:
    alert_class = _resolve_alert_class(classification)
    suggested_action = (classification.suggested_action or "").upper()
    if alert_class in {AlertClass.ACTIONABLE_FAST, AlertClass.ACTIONABLE_STANDARD} and suggested_action != "FOLLOW":
        return DeliveryDecision(
            deliver=False,
            alert_class=alert_class,
            band_applied="none",
            prob_used=_get_probability_for_band(alert),
            within_band=True,
            reason=FilterReason.NON_ACTIONABLE.value,
        )
    within_band = True
    prob_within, prob_value = _is_within_probability_band(alert, config.p_soft_min, config.p_soft_max)
    band_applied = "none"
    reason: str | None = None

    if alert_class == AlertClass.ACTIONABLE_FAST:
        if not config.allow_fast_alerts:
            reason = FilterReason.FAST_NOT_ALLOWED.value
            return DeliveryDecision(
                deliver=False,
                alert_class=alert_class,
                band_applied=band_applied,
                prob_used=prob_value,
                reason=reason,
            )
        return DeliveryDecision(
            deliver=True,
            alert_class=alert_class,
            band_applied=band_applied,
            prob_used=prob_value,
        )

    if alert_class == AlertClass.ACTIONABLE_STANDARD:
        band_applied = "soft"
        within_band = prob_within
        reason = FilterReason.P_OUT_OF_BAND.value if not within_band else None
        return DeliveryDecision(
            deliver=within_band,
            alert_class=alert_class,
            band_applied=band_applied,
            prob_used=prob_value,
            within_band=within_band,
            reason=reason,
        )

    band_applied = "strict"
    prob_within, prob_value = _is_within_probability_band(alert, config.p_strict_min, config.p_strict_max)
    if not config.allow_info_alerts:
        return DeliveryDecision(
            deliver=False,
            alert_class=alert_class,
            band_applied=band_applied,
            prob_used=prob_value,
            within_band=prob_within,
            reason=FilterReason.INFO_ONLY_BLOCKED.value,
        )
    if not prob_within:
        return DeliveryDecision(
            deliver=False,
            alert_class=alert_class,
            band_applied=band_applied,
            prob_used=prob_value,
            within_band=prob_within,
            reason=FilterReason.STRICT_BAND_BLOCKED.value,
        )

    return DeliveryDecision(
        deliver=True,
        alert_class=alert_class,
        band_applied=band_applied,
        prob_used=prob_value,
        within_band=prob_within,
    )


def _log_alert_filter_decision(
    alert: Alert,
    classification: AlertClassification,
    decision: DeliveryDecision,
) -> None:
    if decision.deliver:
        return
    alert_class = _resolve_alert_class(classification)
    logger.debug(
        "alert_filtered alert_id=%s market_id=%s market_kind=%s alert_class=%s signal_type=%s confidence=%s "
        "band_applied=%s prob_used=%s within_band=%s reason=%s",
        alert.id,
        alert.market_id,
        getattr(alert, "market_kind", None),
        alert_class.value,
        classification.signal_type,
        classification.confidence,
        decision.band_applied,
        decision.prob_used,
        decision.within_band,
        decision.reason,
    )


def _is_actionable_classification(classification: AlertClassification) -> bool:
    return _resolve_alert_class(classification) in {
        AlertClass.ACTIONABLE_FAST,
        AlertClass.ACTIONABLE_STANDARD,
    }


def _is_within_actionable_pyes(alert: Alert, p_min: float, p_max: float) -> bool:
    prob_within, _ = _is_within_probability_band(alert, p_min, p_max)
    return prob_within


def _is_within_probability_band(alert: Alert, p_min: float, p_max: float) -> tuple[bool, float | None]:
    value = _get_probability_for_band(alert)
    if value is None:
        return True, None
    return (p_min <= value <= p_max), value


def _get_probability_for_band(alert: Alert) -> float | None:
    market_kind = getattr(alert, "market_kind", None)
    is_yesno = getattr(alert, "is_yesno", None)
    treat_yesno = (is_yesno is True) or (market_kind and market_kind.lower() == "yesno")
    if treat_yesno and alert.market_p_yes is not None:
        return alert.market_p_yes
    if getattr(alert, "is_yesno", None) is False and not treat_yesno:
        for attr in ("market_p_primary", "p_primary"):
            prob = getattr(alert, attr, None)
            if prob is not None:
                return prob
    if alert.market_p_yes is not None:
        return alert.market_p_yes
    for attr in ("market_p_primary", "p_primary"):
        prob = getattr(alert, attr, None)
        if prob is not None:
            return prob
    return None


def _signed_price_delta(alert: Alert) -> float:
    if alert.new_price is not None and alert.old_price is not None:
        return alert.new_price - alert.old_price
    return alert.move or 0.0


def _format_move(alert: Alert) -> str:
    signed_delta = _signed_price_delta(alert)
    abs_move = abs(signed_delta)
    raw_delta_pct = alert.delta_pct if alert.delta_pct is not None else alert.move
    delta_pct = abs((raw_delta_pct or 0.0) * 100)
    sign = "+" if signed_delta >= 0 else "-"
    return f"{sign}{abs_move:.3f} ({sign}{delta_pct:.1f}%)"


def _format_p_yes(alert: Alert) -> str:
    label = _format_probability_label(alert)
    new_value = alert.market_p_yes
    if new_value is None:
        return f"{label} now: n/a"
    new_text = f"{new_value * 100:.1f}%"
    if alert.prev_market_p_yes is not None:
        prev_text = f"{alert.prev_market_p_yes * 100:.1f}%"
        return f"{label}: {prev_text} -> {new_text}"
    return f"{label} now: {new_text}"


def _format_liquidity_volume(alert: Alert) -> str:
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
    return f"Liquidity: {liq_descriptor} | Volume: {vol_descriptor}"


def _format_liquidity_descriptors(alert: Alert) -> tuple[str, str]:
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
    return liq_descriptor, vol_descriptor


def _descriptor_from_thresholds(value: float, high: float, moderate: float) -> str:
    if value >= high:
        return "High"
    if value >= moderate:
        return "Moderate"
    return "Light"


def _format_market_link(market_id: str, slug: str | None = None) -> str:
    return market_url(market_id, slug)


def _sort_copilot_reasons(reasons: list[str]) -> None:
    order = {reason: idx for idx, reason in enumerate(_COPILOT_REASON_ORDER)}
    reasons.sort(key=lambda reason: order.get(reason, len(order)))


def _count_snapshot_points(db: Session, alert: Alert, max_points: int = 5) -> int:
    if alert.snapshot_bucket is None:
        return 0
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
    return len(points)


def _count_snapshot_points_bulk(
    db: Session,
    alerts: list[Alert],
    window_start: datetime,
    window_end: datetime,
) -> dict[str, int]:
    market_ids = {alert.market_id for alert in alerts if alert.market_id}
    if not market_ids:
        return {}
    rows = (
        db.query(MarketSnapshot.market_id, func.count(MarketSnapshot.market_id))
        .filter(
            MarketSnapshot.market_id.in_(market_ids),
            MarketSnapshot.snapshot_bucket >= window_start,
            MarketSnapshot.snapshot_bucket <= window_end,
            MarketSnapshot.market_p_yes.isnot(None),
        )
        .group_by(MarketSnapshot.market_id)
        .all()
    )
    return {market_id: int(count) for market_id, count in rows}


def _missing_price_or_liquidity(alert: Alert) -> bool:
    missing_price = alert.old_price is None or alert.new_price is None
    missing_liquidity = alert.liquidity is None or alert.volume_24h is None
    return missing_price or missing_liquidity


def _label_mapping_unknown(alert: Alert) -> bool:
    mapping_confidence = getattr(alert, "mapping_confidence", None)
    return mapping_confidence != "verified"


def _copilot_daily_count_key(user_id: UUID, now_ts: datetime, signal_speed: str = SIGNAL_SPEED_STANDARD) -> str:
    date_key = now_ts.date().isoformat()
    if signal_speed == SIGNAL_SPEED_FAST:
        return COPILOT_FAST_DAILY_COUNT_KEY.format(user_id=user_id, date=date_key)
    return COPILOT_DAILY_COUNT_KEY.format(user_id=user_id, date=date_key)


def _copilot_hourly_count_key(user_id: UUID, now_ts: datetime) -> str:
    hour_key = now_ts.strftime("%Y-%m-%d-%H")
    return COPILOT_HOURLY_COUNT_KEY.format(user_id=user_id, hour=hour_key)


def _get_copilot_daily_count(
    user_id: UUID,
    now_ts: datetime,
    signal_speed: str = SIGNAL_SPEED_STANDARD,
) -> int:
    key = _copilot_daily_count_key(user_id, now_ts, signal_speed)
    try:
        raw = redis_conn.get(key)
        if raw is None:
            return 0
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode()
        return int(raw)
    except Exception:
        logger.exception("copilot_daily_count_lookup_failed user_id=%s", user_id)
        return 0


def _get_copilot_hourly_count(user_id: UUID, now_ts: datetime) -> int:
    key = _copilot_hourly_count_key(user_id, now_ts)
    try:
        raw = redis_conn.get(key)
        if raw is None:
            return 0
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode()
        return int(raw)
    except Exception:
        logger.exception("copilot_hourly_count_lookup_failed user_id=%s", user_id)
        return 0


def _copilot_theme_dedupe_key(user_id: UUID, theme_key: str, signal_speed: str) -> str:
    if signal_speed == SIGNAL_SPEED_FAST:
        return COPILOT_FAST_THEME_DEDUPE_KEY.format(user_id=user_id, theme_key=theme_key)
    return COPILOT_THEME_DEDUPE_KEY.format(user_id=user_id, theme_key=theme_key)


def _get_copilot_theme_ttl(
    user_id: UUID,
    theme_key: str,
    signal_speed: str = SIGNAL_SPEED_STANDARD,
) -> int | None:
    key = _copilot_theme_dedupe_key(user_id, theme_key, signal_speed)
    try:
        ttl = redis_conn.ttl(key)
        if ttl is None or ttl < 0:
            return None
        return int(ttl)
    except Exception:
        logger.exception("copilot_theme_ttl_lookup_failed user_id=%s theme_key=%s", user_id, theme_key)
        return None


def _log_copilot_evaluation(
    config: UserDigestConfig,
    evaluations: list[CopilotThemeEvaluation],
    now_ts: datetime,
    daily_count: int,
    daily_limit: int,
    sent_this_digest: int,
    digest_limit: int,
    cap_reached: str | None,
    selected_themes: list[str],
) -> None:
    eligible_count = sum(1 for evaluation in evaluations if not evaluation.reasons)
    daily_usage = f"{min(daily_count, daily_limit)}/{daily_limit}"
    digest_usage = f"{min(sent_this_digest, digest_limit)}/{digest_limit}"
    cap_reached_message = _build_cap_reached_message(
        config,
        daily_usage=daily_usage,
        digest_usage=digest_usage,
        cap_reached=cap_reached,
    )
    payload = {
        "user_id": str(config.user_id),
        "plan_name": config.plan_name,
        "themes_total": len(evaluations),
        "themes_eligible_count": eligible_count,
        "daily_count": daily_count,
        "daily_limit": daily_limit,
        "daily_usage": daily_usage,
        "sent_this_digest": sent_this_digest,
        "digest_limit": digest_limit,
        "digest_usage": digest_usage,
        "cap_reached": cap_reached,
        "cap_reached_message": cap_reached_message,
        "selected_themes": selected_themes,
        "created_at": now_ts.isoformat(),
        "themes": [
            {
                "theme_key": evaluation.theme_key,
                "market_id": evaluation.market_id,
                "signal_speed": evaluation.signal_speed,
                "reasons": evaluation.reasons,
            }
            for evaluation in sorted(evaluations, key=lambda item: item.theme_key)
        ],
    }
    logger.debug(
        "copilot_theme_eval user_id=%s plan_name=%s themes_total=%s themes_eligible_count=%s "
        "daily_count=%s daily_limit=%s daily_usage=%s sent_this_digest=%s digest_limit=%s digest_usage=%s "
        "cap_reached=%s selected_themes=%s themes=%s",
        payload["user_id"],
        payload["plan_name"],
        payload["themes_total"],
        payload["themes_eligible_count"],
        payload["daily_count"],
        payload["daily_limit"],
        payload["daily_usage"],
        payload["sent_this_digest"],
        payload["digest_limit"],
        payload["digest_usage"],
        payload["cap_reached"],
        json.dumps(payload["selected_themes"]),
        json.dumps(payload["themes"]),
    )
    try:
        redis_conn.set(
            COPILOT_LAST_EVAL_KEY.format(user_id=config.user_id),
            json.dumps(payload),
            ex=COPILOT_LAST_EVAL_TTL_SECONDS,
        )
    except Exception:
        logger.exception("copilot_last_eval_store_failed user_id=%s", config.user_id)


def _enqueue_ai_recommendations(
    db: Session,
    config: UserDigestConfig,
    actionable_alerts: list[Alert],
    classifier=None,
    allow_enqueue: bool | None = None,
    run_id: str | None = None,
    run_started_at: float | None = None,
    digest_window_minutes: int | None = None,
    enqueue_jobs: bool = True,
) -> CopilotEnqueueResult:
    if not actionable_alerts:
        return CopilotEnqueueResult([], [], 0, 0, None)
    now_ts = datetime.now(timezone.utc)
    run_started_at = run_started_at or time.time()
    if classifier is None:
        classifier = lambda alert: classify_alert_with_snapshots(db, alert)
    window_minutes = digest_window_minutes or config.digest_window_minutes
    daily_limit = max(config.max_copilot_per_day, 0)
    fast_daily_limit = max(config.max_fast_copilot_per_day, 0)
    hourly_limit = max(config.max_copilot_per_hour, 0)
    digest_limit = max(config.max_copilot_per_digest, 1)
    daily_count = _get_copilot_daily_count(config.user_id, now_ts, SIGNAL_SPEED_STANDARD)
    fast_daily_count = _get_copilot_daily_count(config.user_id, now_ts, SIGNAL_SPEED_FAST)
    hourly_count = _get_copilot_hourly_count(config.user_id, now_ts)
    remaining_daily_by_speed = {
        SIGNAL_SPEED_STANDARD: max(daily_limit - daily_count, 0),
        SIGNAL_SPEED_FAST: max(fast_daily_limit - fast_daily_count, 0),
    }
    remaining_hourly = max(hourly_limit - hourly_count, 0)
    sent_this_digest = 0
    cap_reached_reason: str | None = None
    selected_theme_keys: list[str] = []
    if allow_enqueue is None:
        allow_enqueue = config.ai_copilot_enabled

    themes = group_alerts_into_themes(actionable_alerts, classifier)
    actionable_theme_count = len(themes)
    if not themes:
        logger.debug(
            "copilot_theme_counts user_id=%s actionable_themes=%s eligible_themes=%s sent=%s reason=no_themes",
            config.user_id,
            actionable_theme_count,
            0,
            0,
        )
        _log_copilot_evaluation(
            config,
            [],
            now_ts,
            daily_count,
            daily_limit,
            sent_this_digest,
            digest_limit,
            cap_reached_reason,
            selected_theme_keys,
        )
        _record_copilot_run(
            run_id=run_id,
            run_started_at=run_started_at,
            config=config,
            digest_window_minutes=digest_window_minutes,
            now_ts=now_ts,
            themes_total=0,
            themes_candidate=0,
            themes_eligible=0,
            themes_selected=0,
            evaluations=[],
            daily_count=daily_count,
            daily_limit=daily_limit,
            hourly_count=hourly_count,
            hourly_limit=hourly_limit,
            digest_limit=digest_limit,
            enqueued=0,
        )
        return CopilotEnqueueResult([], [], 0, 0, None)

    window_start = now_ts - timedelta(minutes=max(int(window_minutes), 1))
    snapshot_counts = _count_snapshot_points_bulk(
        db,
        [theme.representative for theme in themes],
        window_start,
        now_ts,
    )

    muted_market_ids = {
        row.market_id
        for row in db.query(AiMarketMute)
        .filter(
            AiMarketMute.user_id == config.user_id,
            AiMarketMute.expires_at > now_ts,
        )
        .all()
    }
    muted_theme_keys = {
        row.theme_key
        for row in db.query(AiThemeMute)
        .filter(
            AiThemeMute.user_id == config.user_id,
            AiThemeMute.expires_at > now_ts,
        )
        .all()
    }

    eligible_themes: list[Theme] = []
    evaluations: list[CopilotThemeEvaluation] = []
    evaluations_by_key: dict[str, CopilotThemeEvaluation] = {}
    for theme in themes:
        rep = theme.representative
        rep_classification = theme.representative_classification
        if rep_classification is None:
            rep_classification = classifier(rep) if classifier else classify_alert_with_snapshots(db, rep)
        snapshot_count = snapshot_counts.get(rep.market_id, 0)
        if snapshot_count <= 0:
            snapshot_count = 1
        setattr(rep, "sustained_snapshots", snapshot_count)
        signal_speed = classify_signal_speed(rep, window_minutes)
        theme.signal_speed = signal_speed
        base_reasons: list[str] = []
        if not allow_enqueue:
            if not config.copilot_user_enabled:
                base_reasons.append(CopilotIneligibilityReason.USER_DISABLED.value)
            if not config.copilot_plan_enabled:
                base_reasons.append(CopilotIneligibilityReason.PLAN_DISABLED.value)
        if theme.key in muted_theme_keys or rep.market_id in muted_market_ids:
            base_reasons.append(CopilotIneligibilityReason.MUTED.value)
        if rep_classification.signal_type != "REPRICING":
            base_reasons.append(CopilotIneligibilityReason.NOT_REPRICING.value)
        if rep_classification.confidence != "HIGH":
            base_reasons.append(CopilotIneligibilityReason.CONFIDENCE_NOT_HIGH.value)
        if rep_classification.suggested_action != "FOLLOW":
            base_reasons.append(CopilotIneligibilityReason.NOT_FOLLOW.value)

        decision = _evaluate_delivery_decision(rep, rep_classification, config)
        if not decision.deliver and decision.reason:
            if decision.reason == FilterReason.P_OUT_OF_BAND.value and signal_speed == SIGNAL_SPEED_FAST:
                pass
            elif decision.reason == FilterReason.P_OUT_OF_BAND.value:
                base_reasons.append(CopilotIneligibilityReason.P_OUT_OF_BAND.value)
            else:
                base_reasons.append(decision.reason)

        reasons = list(base_reasons)
        if reasons:
            if _label_mapping_unknown(rep):
                reasons.append(CopilotIneligibilityReason.LABEL_MAPPING_UNKNOWN.value)
            if _missing_price_or_liquidity(rep):
                reasons.append(CopilotIneligibilityReason.MISSING_PRICE_OR_LIQUIDITY.value)
            if signal_speed != SIGNAL_SPEED_FAST and snapshot_count < 3:
                reasons.append(CopilotIneligibilityReason.INSUFFICIENT_SNAPSHOTS.value)

        _sort_copilot_reasons(reasons)
        evaluation = CopilotThemeEvaluation(theme.key, rep.market_id, signal_speed, reasons)
        evaluations.append(evaluation)
        evaluations_by_key[theme.key] = evaluation
        if not evaluation.reasons:
            eligible_themes.append(theme)

    if not eligible_themes:
        reason = "no_follow_high_repricings"
        skipped_muted = sum(
            1
            for evaluation in evaluations
            if CopilotIneligibilityReason.MUTED.value in evaluation.reasons
        )
        skipped_pyes = sum(
            1
            for evaluation in evaluations
            if CopilotIneligibilityReason.P_OUT_OF_BAND.value in evaluation.reasons
        )
        if skipped_muted and skipped_muted >= actionable_theme_count:
            reason = "all_themes_muted"
        elif skipped_pyes and skipped_pyes >= actionable_theme_count:
            reason = "all_outside_pyes_band"
        logger.debug(
            "copilot_theme_counts user_id=%s actionable_themes=%s eligible_themes=%s sent=%s reason=%s",
            config.user_id,
            actionable_theme_count,
            0,
            0,
            reason,
        )
        _log_copilot_evaluation(
            config,
            evaluations,
            now_ts,
            daily_count,
            daily_limit,
            sent_this_digest,
            digest_limit,
            cap_reached_reason,
            selected_theme_keys,
        )
        _record_copilot_run(
            run_id=run_id,
            run_started_at=run_started_at,
            config=config,
            digest_window_minutes=digest_window_minutes,
            now_ts=now_ts,
            themes_total=actionable_theme_count,
            themes_candidate=actionable_theme_count,
            themes_eligible=0,
            themes_selected=0,
            evaluations=evaluations,
            daily_count=daily_count,
            daily_limit=daily_limit,
            hourly_count=hourly_count,
            hourly_limit=hourly_limit,
            digest_limit=digest_limit,
            enqueued=0,
        )
        return CopilotEnqueueResult(evaluations, [], 0, 0, cap_reached_reason)

    ranked = sorted(
        eligible_themes,
        key=lambda theme: (
            _score_copilot_theme(theme),
            theme.representative.liquidity,
            theme.representative.volume_24h,
            theme.key,
        ),
        reverse=True,
    )
    ranked = _rotate_themes_by_category(ranked, limit=len(ranked))

    candidate_ids = [theme.representative.id for theme in ranked if theme.representative.id is not None]
    existing_alert_ids = set()
    if candidate_ids:
        existing_alert_ids = {
            row.alert_id
            for row in db.query(AiRecommendation.alert_id)
            .filter(
                AiRecommendation.user_id == config.user_id,
                AiRecommendation.alert_id.in_(candidate_ids),
            )
            .all()
        }

    enqueued = 0
    for idx, theme in enumerate(ranked):
        signal_speed = theme.signal_speed or SIGNAL_SPEED_STANDARD
        if hourly_limit > 0 and remaining_hourly <= 0:
            if cap_reached_reason is None:
                cap_reached_reason = "hourly"
                logger.debug(
                    "copilot_cap_reached user_id=%s plan_name=%s hourly=%s/%s digest=%s/%s",
                    config.user_id,
                    config.plan_name,
                    hourly_count,
                    hourly_limit,
                    sent_this_digest,
                    digest_limit,
                )
            evaluations_by_key[theme.key].add_reason(CopilotIneligibilityReason.CAP_REACHED.value)
            continue
        if remaining_daily_by_speed.get(signal_speed, 0) <= 0:
            if signal_speed == SIGNAL_SPEED_STANDARD and cap_reached_reason is None:
                cap_reached_reason = "daily"
                logger.debug(
                    "copilot_cap_reached user_id=%s plan_name=%s daily=%s/%s digest=%s/%s",
                    config.user_id,
                    config.plan_name,
                    daily_count,
                    daily_limit,
                    sent_this_digest,
                    digest_limit,
                )
            evaluations_by_key[theme.key].add_reason(CopilotIneligibilityReason.CAP_REACHED.value)
            continue
        if sent_this_digest >= digest_limit:
            cap_reached_reason = "digest"
            logger.debug(
                "copilot_cap_reached user_id=%s plan_name=%s daily=%s/%s digest=%s/%s",
                config.user_id,
                config.plan_name,
                daily_count,
                daily_limit,
                sent_this_digest,
                digest_limit,
            )
            for remaining in ranked[idx:]:
                evaluations_by_key[remaining.key].add_reason(
                    CopilotIneligibilityReason.CAP_REACHED.value
                )
            break
        alert = theme.representative
        if alert.id is None or alert.id in existing_alert_ids:
            evaluations_by_key[theme.key].add_reason(
                CopilotIneligibilityReason.COPILOT_DEDUPE_ACTIVE.value
            )
            continue
        ttl_remaining = _get_copilot_theme_ttl(config.user_id, theme.key, signal_speed)
        if ttl_remaining:
            evaluations_by_key[theme.key].add_reason(
                CopilotIneligibilityReason.COPILOT_DEDUPE_ACTIVE.value
            )
            dedupe_key = _copilot_theme_dedupe_key(config.user_id, theme.key, signal_speed)
            logger.debug(
                "copilot_theme_skip_dedupe user_id=%s theme_key=%s ttl_remaining=%s dedupe_key=%s scope=theme",
                config.user_id,
                theme.key,
                ttl_remaining,
                dedupe_key,
            )
            continue
        selected_theme_keys.append(theme.key)
        if allow_enqueue and enqueue_jobs:
            queue.enqueue(
                ai_recommendation_job,
                str(config.user_id),
                alert.id,
                run_id,
                signal_speed,
                window_minutes,
            )
            enqueued += 1
        sent_this_digest += 1
        remaining_daily_by_speed[signal_speed] = remaining_daily_by_speed.get(signal_speed, 0) - 1
        remaining_hourly = max(remaining_hourly - 1, 0)

    if (
        (config.plan_name or "").lower() == "elite"
        and enqueued == 0
        and eligible_themes
        and cap_reached_reason is None
        and (hourly_limit == 0 or remaining_hourly > 0)
        and sent_this_digest < digest_limit
    ):
        for theme in ranked:
            signal_speed = theme.signal_speed or SIGNAL_SPEED_STANDARD
            if remaining_daily_by_speed.get(signal_speed, 0) <= 0:
                continue
            alert = theme.representative
            if alert.id is None or alert.id in existing_alert_ids:
                continue
            selected_theme_keys.append(theme.key)
            if allow_enqueue and enqueue_jobs:
                queue.enqueue(
                    ai_recommendation_job,
                    str(config.user_id),
                    alert.id,
                    run_id,
                    signal_speed,
                    window_minutes,
                )
                enqueued += 1
            sent_this_digest += 1
            remaining_daily_by_speed[signal_speed] = remaining_daily_by_speed.get(signal_speed, 0) - 1
            remaining_hourly = max(remaining_hourly - 1, 0)
            logger.info(
                "copilot_elite_override user_id=%s theme_key=%s signal_speed=%s",
                config.user_id,
                theme.key,
                signal_speed,
            )
            break

    logger.debug(
        "copilot_theme_counts user_id=%s actionable_themes=%s eligible_themes=%s sent=%s",
        config.user_id,
        actionable_theme_count,
        len(eligible_themes),
        enqueued,
    )
    _log_copilot_evaluation(
        config,
        evaluations,
        now_ts,
        daily_count,
        daily_limit,
        sent_this_digest,
        digest_limit,
        cap_reached_reason,
        selected_theme_keys,
    )
    _record_copilot_run(
        run_id=run_id,
        run_started_at=run_started_at,
        config=config,
        digest_window_minutes=digest_window_minutes,
        now_ts=now_ts,
        themes_total=actionable_theme_count,
        themes_candidate=actionable_theme_count,
        themes_eligible=len(eligible_themes),
        themes_selected=len(selected_theme_keys),
        evaluations=evaluations,
        daily_count=daily_count,
        daily_limit=daily_limit,
        hourly_count=hourly_count,
        hourly_limit=hourly_limit,
        digest_limit=digest_limit,
        enqueued=enqueued,
    )
    return CopilotEnqueueResult(
        evaluations,
        selected_theme_keys,
        enqueued,
        len(eligible_themes),
        cap_reached_reason,
    )


def _build_copilot_run_summary(
    config: UserDigestConfig,
    now_ts: datetime,
    digest_window_minutes: int | None,
    run_id: str | None,
    themes_total: int,
    themes_candidate: int,
    themes_eligible: int,
    themes_selected: int,
    skipped_by_reason_counts: dict[str, int],
    daily_count: int,
    daily_limit: int,
    hourly_count: int,
    hourly_limit: int,
    digest_limit: int,
) -> dict[str, object]:
    caps_remaining_day = max(daily_limit - daily_count, 0)
    caps_remaining_hour = max(hourly_limit - hourly_count, 0)
    return {
        "run_id": run_id,
        "user_id": str(config.user_id),
        "plan": config.plan_name,
        "digest_window_minutes": digest_window_minutes or config.digest_window_minutes,
        "themes_total": themes_total,
        "themes_candidate": themes_candidate,
        "themes_eligible": themes_eligible,
        "themes_selected": themes_selected,
        "daily_count": daily_count,
        "daily_limit": daily_limit,
        "hourly_count": hourly_count,
        "hourly_limit": hourly_limit,
        "caps_remaining_day": caps_remaining_day,
        "caps_remaining_hour": caps_remaining_hour,
        "digest_limit": digest_limit,
        "skipped_by_reason_counts": dict(skipped_by_reason_counts),
        "created_at": now_ts.isoformat(),
    }


def _emit_copilot_run_summary(
    run_id: str | None,
    run_started_at: float | None,
    config: UserDigestConfig,
    digest_window_minutes: int,
    themes_total: int,
    themes_candidate: int,
    themes_eligible: int,
    themes_selected: int,
    skipped_by_reason_counts: dict[str, int],
    daily_count: int,
    daily_limit: int,
    hourly_count: int,
    hourly_limit: int,
    digest_limit: int,
    llm_calls_attempted: int,
    llm_calls_succeeded: int,
    telegram_sends_attempted: int,
    telegram_sends_succeeded: int,
) -> dict[str, object]:
    summary = _build_copilot_run_summary(
        config=config,
        now_ts=datetime.now(timezone.utc),
        digest_window_minutes=digest_window_minutes,
        run_id=run_id,
        themes_total=themes_total,
        themes_candidate=themes_candidate,
        themes_eligible=themes_eligible,
        themes_selected=themes_selected,
        skipped_by_reason_counts=skipped_by_reason_counts,
        daily_count=daily_count,
        daily_limit=daily_limit,
        hourly_count=hourly_count,
        hourly_limit=hourly_limit,
        digest_limit=digest_limit,
    )
    duration_ms = 0
    if run_started_at:
        duration_ms = int((time.time() - run_started_at) * 1000)
    summary.update(
        {
            "llm_calls_attempted": llm_calls_attempted,
            "llm_calls_succeeded": llm_calls_succeeded,
            "telegram_sends_attempted": telegram_sends_attempted,
            "telegram_sends_succeeded": telegram_sends_succeeded,
            "duration_ms": duration_ms,
        }
    )
    log_copilot_run_summary(summary)
    store_copilot_last_status(summary)
    return summary


def _record_copilot_run(
    run_id: str | None,
    run_started_at: float | None,
    config: UserDigestConfig,
    digest_window_minutes: int | None,
    now_ts: datetime,
    themes_total: int,
    themes_candidate: int,
    themes_eligible: int,
    themes_selected: int,
    evaluations: list[CopilotThemeEvaluation],
    daily_count: int,
    daily_limit: int,
    hourly_count: int,
    hourly_limit: int,
    digest_limit: int,
    enqueued: int,
) -> None:
    reason_counts = Counter()
    for evaluation in evaluations:
        for reason in evaluation.reasons:
            reason_counts[reason] += 1
    if not evaluations and themes_total == 0:
        reason_counts[COPILOT_RUN_SKIP_NO_ACTIONABLE_THEMES] += 1
    summary = _build_copilot_run_summary(
        config=config,
        now_ts=now_ts,
        digest_window_minutes=digest_window_minutes,
        run_id=run_id,
        themes_total=themes_total,
        themes_candidate=themes_candidate,
        themes_eligible=themes_eligible,
        themes_selected=themes_selected,
        skipped_by_reason_counts=dict(reason_counts),
        daily_count=daily_count,
        daily_limit=daily_limit,
        hourly_count=hourly_count,
        hourly_limit=hourly_limit,
        digest_limit=digest_limit,
    )
    if run_id and enqueued > 0:
        init_copilot_run(run_id, summary, run_started_at=run_started_at, expected_jobs=enqueued)
        return
    _emit_copilot_run_summary(
        run_id=run_id,
        run_started_at=run_started_at,
        config=config,
        digest_window_minutes=summary["digest_window_minutes"] if isinstance(summary["digest_window_minutes"], int) else config.digest_window_minutes,
        themes_total=themes_total,
        themes_candidate=themes_candidate,
        themes_eligible=themes_eligible,
        themes_selected=themes_selected,
        skipped_by_reason_counts=summary["skipped_by_reason_counts"] if isinstance(summary["skipped_by_reason_counts"], dict) else {},
        daily_count=daily_count,
        daily_limit=daily_limit,
        hourly_count=hourly_count,
        hourly_limit=hourly_limit,
        digest_limit=digest_limit,
        llm_calls_attempted=0,
        llm_calls_succeeded=0,
        telegram_sends_attempted=0,
        telegram_sends_succeeded=0,
    )


def _build_cap_reached_message(
    config: UserDigestConfig,
    daily_usage: str,
    digest_usage: str,
    cap_reached: str | None,
) -> str | None:
    if not cap_reached:
        return None
    plan_label = (config.plan_name or "free").upper()
    if cap_reached == "daily":
        usage_text = daily_usage
        period_label = "today"
    elif cap_reached == "hourly":
        usage_text = "hourly"
        period_label = "this hour"
    else:
        usage_text = digest_usage
        period_label = "this digest"
    upgrade_plan = upgrade_target_name(config.plan_name)
    if upgrade_plan:
        return (
            f"CAP_REACHED: {plan_label} plan limit hit ({usage_text} Copilot {period_label}). "
            f"Upgrade to {upgrade_plan.upper()} for higher caps."
        )
    return (
        f"CAP_REACHED: {plan_label} plan limit hit ({usage_text} Copilot {period_label}). "
        "Contact support for higher caps."
    )


def _dedupe_by_theme(alerts: list[Alert]) -> list[Alert]:
    buckets: dict[str, Alert] = {}
    for alert in alerts:
        key = _theme_key(alert)
        existing = buckets.get(key)
        if not existing or (alert.liquidity, alert.volume_24h) > (existing.liquidity, existing.volume_24h):
            buckets[key] = alert
    return list(buckets.values())


def _theme_key(alert: Alert) -> str:
    extracted = extract_theme(alert.title or "", category=alert.category, slug=alert.market_id)
    return extracted.theme_key


def group_alerts_into_themes(alerts: list[Alert], classifier=None) -> list[Theme]:
    themes: list[Theme] = []
    for alert in alerts:
        title = alert.title or ""
        extracted = extract_theme(title, category=alert.category, slug=alert.market_id)
        theme_key = extracted.theme_key
        token_list = strip_stopwords(normalize_text(title).split())
        tokens = set(token_list)
        matched = None
        for theme in themes:
            if theme.key == theme_key:
                matched = theme
                break
        if matched is None:
            for theme in themes:
                if _jaccard_similarity(theme.tokens, tokens) >= 0.6:
                    matched = theme
                    break
        if matched:
            matched.alerts.append(alert)
            matched.tokens |= tokens
        else:
            label = extracted.theme_label
            themes.append(
                Theme(
                    key=theme_key,
                    label=label,
                    alerts=[alert],
                    representative=alert,
                    tokens=tokens,
                )
            )

    for theme in themes:
        rep, rep_classification = _pick_theme_representative(theme.alerts, classifier)
        theme.representative = rep
        theme.representative_classification = rep_classification
    return themes


def _pick_theme_representative(
    alerts: list[Alert],
    classifier=None,
) -> tuple[Alert, AlertClassification]:
    def _strength_weight(alert: Alert) -> int:
        return 1 if alert.strength == AlertStrength.STRONG.value else 0

    def _confidence_weight(classification: AlertClassification) -> int:
        return {"HIGH": 2, "MEDIUM": 1, "LOW": 0}.get(classification.confidence, 0)

    scored: list[tuple[Alert, AlertClassification]] = []
    for alert in alerts:
        classification = classifier(alert) if classifier else classify_alert(alert)
        scored.append((alert, classification))

    best_alert, best_classification = sorted(
        scored,
        key=lambda pair: (
            _strength_weight(pair[0]),
            _confidence_weight(pair[1]),
            _alert_abs_move(pair[0]),
            pair[0].liquidity,
            pair[0].volume_24h,
            pair[0].market_id or "",
        ),
        reverse=True,
    )[0]
    return best_alert, best_classification


def _short_title(alert: Alert, theme_hint: bool = False) -> str:
    title = alert.title or ""
    if theme_hint:
        return extract_theme(title, category=alert.category, slug=alert.market_id).short_title

    cleaned = re.sub(r"^\s*will\s+", "", title, flags=re.IGNORECASE)
    cleaned = re.sub(r"^\s*the\s+price\s+of\s+", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s+on\s+.*$", "", cleaned, flags=re.IGNORECASE)
    cleaned = cleaned.replace("?", "").strip()
    return cleaned[:80] or title[:80]


def _format_compact_move(alert: Alert) -> str:
    raw_delta_pct = alert.delta_pct if alert.delta_pct is not None else alert.move
    delta_pct = (raw_delta_pct or 0.0) * 100
    sign = "+" if delta_pct >= 0 else "-"
    return f"{sign}{abs(delta_pct):.1f}%"


def _format_p_yes_compact(alert: Alert) -> str:
    label = _format_probability_label(alert)
    new_value = alert.market_p_yes
    if new_value is None:
        return f"{label} n/a"
    new_text = f"{new_value * 100:.1f}"
    if alert.prev_market_p_yes is not None:
        prev_text = f"{alert.prev_market_p_yes * 100:.1f}"
        return f"{label} {prev_text}->{new_text}"
    return f"{label} {new_text}"


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
    cleaned = re.sub(r"[^A-Za-z0-9]+", "_", str(label).strip()).strip("_")
    if not cleaned:
        return None
    cleaned = cleaned.upper()
    if cleaned in {"OUTCOME_0", "OUTCOME0"}:
        return None
    return cleaned


def _extract_theme_metadata(title: str) -> dict:
    text = title.lower()
    date = _extract_date(text)
    range_match = re.search(r"between\s+\$?([\d,]+(?:\.\d+)?)\s+and\s+\$?([\d,]+(?:\.\d+)?)", text)
    if range_match:
        return {
            "kind": "range",
            "underlying": _extract_underlying(text),
            "date": date,
            "range_low": _parse_number(range_match.group(1)),
            "range_high": _parse_number(range_match.group(2)),
        }
    above_below = re.search(r"\b(above|below)\s+\$?([\d,]+(?:\.\d+)?)", text)
    if above_below:
        return {
            "kind": above_below.group(1),
            "underlying": _extract_underlying(text),
            "date": date,
            "strike": _parse_number(above_below.group(2)),
        }
    matchup = _extract_matchup(text)
    if matchup:
        return {"kind": "matchup", "matchup": matchup, "date": date}
    return {"kind": "fallback", "date": date}


def _build_theme_key(meta: dict, title: str, market_id: str | None) -> str:
    if meta.get("kind") in {"range", "above", "below"} and meta.get("underlying") and meta.get("date"):
        underlying = _normalize_key(meta["underlying"])
        date = _normalize_key(meta["date"])
        return f"{underlying}-{date}-price-band"
    if meta.get("kind") == "matchup" and meta.get("matchup"):
        matchup = _normalize_key(meta["matchup"])
        date_suffix = f"-{_normalize_key(meta['date'])}" if meta.get("date") else ""
        return f"{matchup}{date_suffix}"
    tokens = _significant_tokens(title)
    if tokens:
        return "_".join(tokens[:6])
    return (market_id or "").lower() or title.lower()


def _theme_label(meta: dict, title: str) -> str:
    if meta.get("kind") in {"range", "above", "below"} and meta.get("underlying"):
        base = meta["underlying"].upper()
        if meta.get("date"):
            base = f"{base} {meta['date'].title()}"
        return f"{base} price band"
    if meta.get("kind") == "matchup" and meta.get("matchup"):
        label = meta["matchup"].replace("_", " ").title()
        if meta.get("date"):
            label = f"{label} {meta['date'].title()}"
        return label
    tokens = _significant_tokens(title)
    if tokens:
        return " ".join(tokens[:4]).title()
    return "Market theme"


def _normalize_key(value: str) -> str:
    return re.sub(r"\s+", "-", value.strip())


def _extract_underlying(text: str) -> str | None:
    tokens = re.findall(r"[a-z0-9]+", text)
    if "price" in tokens and "of" in tokens:
        try:
            idx = tokens.index("of") + 1
        except ValueError:
            return None
        parts = []
        for token in tokens[idx:]:
            if token in _STOPWORDS or token in {"be", "above", "below", "between", "on", "in", "at"}:
                break
            parts.append(token)
        if parts:
            return " ".join(parts)
    return None


def _copilot_skip_note(config: UserDigestConfig, result: CopilotEnqueueResult | None) -> str | None:
    if result is None or result.enqueued > 0:
        return None
    if result.cap_reached_reason:
        period = "hour" if result.cap_reached_reason == "hourly" else "day"
        if result.cap_reached_reason == "digest":
            period = "digest"
        return f"Copilot skipped: CAP_REACHED ({period})"
    reasons = {reason for evaluation in result.evaluations for reason in evaluation.reasons}
    if CopilotIneligibilityReason.CAP_REACHED.value in reasons:
        return "Copilot skipped: CAP_REACHED"
    if CopilotIneligibilityReason.PLAN_DISABLED.value in reasons:
        return "Copilot skipped: PLAN_DISABLED"
    if CopilotIneligibilityReason.USER_DISABLED.value in reasons:
        return "Copilot skipped: USER_DISABLED"
    dedupe_hit = any(
        CopilotIneligibilityReason.COPILOT_DEDUPE_ACTIVE.value in evaluation.reasons
        for evaluation in result.evaluations
    )
    if dedupe_hit:
        return "Copilot skipped: DEDUPE_HIT"
    if result.eligible_count <= 0:
        return "Copilot skipped: NO_ELIGIBLE_THEMES"
    return "Copilot skipped: NO_ELIGIBLE_THEMES"


def _extract_matchup(text: str) -> str | None:
    match = re.search(r"\b([a-z0-9]+)\s+(?:vs|v)\s+([a-z0-9]+)\b", text)
    if match:
        return f"{match.group(1)}_{match.group(2)}"
    return None


def _extract_date(text: str) -> str | None:
    iso_match = re.search(r"\b\d{4}-\d{2}-\d{2}\b", text)
    if iso_match:
        return iso_match.group(0)
    month_match = re.search(
        r"\b(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|"
        r"jul(?:y)?|aug(?:ust)?|sep(?:tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\s+(\d{1,2})\b",
        text,
    )
    if not month_match:
        return None
    month = _MONTHS.get(month_match.group(1)[:3], month_match.group(1))
    day = month_match.group(2)
    return f"{month} {day}"


def _parse_number(raw: str) -> float:
    try:
        return float(raw.replace(",", ""))
    except ValueError:
        return 0.0


def _significant_tokens(title: str) -> list[str]:
    tokens = re.findall(r"[a-z0-9]+", title.lower())
    return [token for token in tokens if token not in _STOPWORDS]


def _jaccard_similarity(tokens_a: set[str], tokens_b: set[str]) -> float:
    if not tokens_a or not tokens_b:
        return 0.0
    intersection = tokens_a & tokens_b
    union = tokens_a | tokens_b
    if not union:
        return 0.0
    return len(intersection) / len(union)


_MONTHS = {
    "jan": "january",
    "feb": "february",
    "mar": "march",
    "apr": "april",
    "may": "may",
    "jun": "june",
    "jul": "july",
    "aug": "august",
    "sep": "september",
    "oct": "october",
    "nov": "november",
    "dec": "december",
}

_STOPWORDS = {
    "the",
    "will",
    "be",
    "of",
    "price",
    "between",
    "on",
    "by",
    "is",
    "a",
    "an",
    "to",
    "for",
    "in",
    "at",
    "from",
    "above",
    "below",
    "over",
    "under",
    "and",
    "or",
    "vs",
    "v",
    "yes",
    "no",
    "market",
    "it",
    "do",
    "does",
    "did",
    "has",
    "have",
    "before",
    "after",
    "what",
    "when",
    "who",
    "how",
    "much",
    "usd",
    "dollars",
}
