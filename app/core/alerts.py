import hashlib
import html
import json
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from urllib.parse import quote
from uuid import UUID

import httpx
import redis
from rq import Queue
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
from . import defaults
from .alert_strength import AlertStrength
from .user_settings import get_effective_user_settings
from .alert_classification import AlertClassification, classify_alert, classify_alert_with_snapshots
from .fast_signals import FAST_ALERT_TYPE
from .plans import upgrade_target_name

logger = logging.getLogger(__name__)
redis_conn = redis.from_url(settings.REDIS_URL)
queue = Queue("default", connection=redis_conn)

USER_DIGEST_LAST_SENT_KEY = "alerts:digest:last_sent:user:{user_id}"
USER_DIGEST_LAST_PAYLOAD_KEY = "alerts:last_digest:user:{user_id}"
TENANT_DIGEST_LAST_PAYLOAD_KEY = "alerts:last_digest:{tenant_id}"
USER_FAST_DIGEST_LAST_SENT_KEY = "alerts:fast:last_sent:user:{user_id}"
DIGEST_SENT_FINGERPRINT_KEY = "digest:sent:{user_id}:{fingerprint_hash}"
COPILOT_THEME_DEDUPE_KEY = "copilot:sent:{user_id}:{theme_key}"
COPILOT_LAST_EVAL_KEY = "copilot:last_eval:{user_id}"
COPILOT_DAILY_COUNT_KEY = "copilot:count:{user_id}:{date}"
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
    risk_budget_usd_per_day: float
    max_usd_per_trade: float
    max_liquidity_fraction: float
    fast_signals_enabled: bool
    fast_window_minutes: int
    fast_max_themes_per_digest: int
    fast_max_markets_per_theme: int
    p_min: float
    p_max: float
    plan_name: str | None
    max_copilot_per_day: int
    max_copilot_per_digest: int
    copilot_theme_ttl_minutes: int
    max_themes_per_digest: int
    max_markets_per_theme: int


@dataclass
class Theme:
    key: str
    label: str
    alerts: list[Alert]
    representative: Alert
    representative_classification: AlertClassification | None = None
    tokens: set[str] = field(default_factory=set, repr=False)


@dataclass(frozen=True)
class FastDigestPayload:
    text: str
    window_minutes: int


class CopilotIneligibilityReason(str, Enum):
    USER_DISABLED = "USER_DISABLED"
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


@dataclass
class CopilotThemeEvaluation:
    theme_key: str
    market_id: str | None
    reasons: list[str]

    def add_reason(self, reason: str) -> None:
        if reason not in self.reasons:
            self.reasons.append(reason)
            _sort_copilot_reasons(self.reasons)


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
        config = _resolve_user_preferences(user, prefs.get(user.user_id))
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
) -> UserDigestConfig:
    effective = get_effective_user_settings(user, pref=pref)
    min_liquidity = effective.min_liquidity
    min_volume_24h = effective.min_volume_24h
    min_abs_price_move = effective.min_abs_move
    alert_strengths = _normalize_allowed_strengths(effective.allowed_strengths)
    digest_window_minutes = effective.digest_window_minutes
    max_alerts_per_digest = effective.max_alerts_per_digest
    ai_copilot_enabled = effective.copilot_enabled
    risk_budget_usd_per_day = effective.risk_budget_usd_per_day
    max_usd_per_trade = effective.max_usd_per_trade
    max_liquidity_fraction = effective.max_liquidity_fraction
    fast_signals_enabled = effective.fast_signals_enabled
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
        risk_budget_usd_per_day=risk_budget_usd_per_day,
        max_usd_per_trade=max_usd_per_trade,
        max_liquidity_fraction=max_liquidity_fraction,
        fast_signals_enabled=fast_signals_enabled,
        fast_window_minutes=max(int(effective.fast_window_minutes), 1),
        fast_max_themes_per_digest=max(int(effective.fast_max_themes_per_digest), 1),
        fast_max_markets_per_theme=max(int(effective.fast_max_markets_per_theme), 1),
        p_min=float(effective.p_min),
        p_max=float(effective.p_max),
        plan_name=effective.plan_name,
        max_copilot_per_day=max(int(effective.max_copilot_per_day), 0),
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


async def _send_user_digest(
    db: Session,
    tenant_id: str,
    config: UserDigestConfig,
    fast_section: str | None = None,
) -> dict:
    window_minutes = max(config.digest_window_minutes, 1)
    now_ts = datetime.now(timezone.utc)

    if _digest_recently_sent(config.user_id, now_ts, window_minutes):
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
    if not rows:
        return {"user_id": str(config.user_id), "sent": False, "reason": "no_alerts"}

    included_alerts, filtered_out = _filter_alerts_for_user(rows, config)

    pyes_filtered: list[Alert] = []
    pyes_candidates: list[Alert] = []
    for alert in included_alerts:
        if not _is_within_actionable_pyes(alert, config.p_min, config.p_max):
            pyes_filtered.append(alert)
            continue
        pyes_candidates.append(alert)

    if not config.telegram_chat_id:
        _record_alert_deliveries(
            db,
            filtered_out + pyes_filtered,
            pyes_candidates,
            config.user_id,
            now_ts,
            sent_alert_ids=set(),
            skip_reason="missing_chat_id",
        )
        return {"user_id": str(config.user_id), "sent": False, "reason": "missing_chat_id"}

    classification_cache: dict[int, AlertClassification] = {}

    def classifier(alert: Alert) -> AlertClassification:
        key = alert.id if alert.id is not None else id(alert)
        cached = classification_cache.get(key)
        if cached:
            return cached
        classification = classify_alert_with_snapshots(db, alert)
        classification_cache[key] = classification
        return classification

    actionable_alerts: list[Alert] = []
    non_actionable_alerts: list[Alert] = []
    for alert in pyes_candidates:
        classification = classifier(alert)
        if _is_actionable_classification(classification):
            actionable_alerts.append(alert)
        else:
            non_actionable_alerts.append(alert)

    actionable_ranked = _dedupe_by_market_id(_rank_alerts(actionable_alerts))
    total_actionable = len(actionable_ranked)
    filtered_for_delivery = filtered_out + pyes_filtered
    if defaults.DIGEST_ACTIONABLE_ONLY:
        filtered_for_delivery += non_actionable_alerts
    included_for_delivery = actionable_alerts if defaults.DIGEST_ACTIONABLE_ONLY else pyes_candidates

    if total_actionable < 1:
        _record_alert_deliveries(
            db,
            filtered_for_delivery,
            included_for_delivery,
            config.user_id,
            now_ts,
            sent_alert_ids=set(),
            skip_reason="no_actionable_alerts",
        )
        return {"user_id": str(config.user_id), "sent": False, "reason": "no_actionable_alerts"}

    max_alerts = min(
        max(config.max_alerts_per_digest, 1),
        max(defaults.MAX_ACTIONABLE_PER_DIGEST, 1),
    )

    selected_actionable = actionable_ranked[:max_alerts]
    if actionable_alerts:
        _enqueue_ai_recommendations(
            db,
            config,
            actionable_alerts,
            classifier,
            allow_enqueue=config.ai_copilot_enabled,
        )
    selected_alerts = selected_actionable
    if not defaults.DIGEST_ACTIONABLE_ONLY:
        strong_alerts = [a for a in pyes_candidates if a.strength == AlertStrength.STRONG.value]
        medium_alerts = [a for a in pyes_candidates if a.strength == AlertStrength.MEDIUM.value]

        strong_ranked = _dedupe_by_market_id(_rank_alerts(strong_alerts))
        medium_ranked = _dedupe_by_market_id(
            _rank_alerts(medium_alerts),
            exclude_market_ids={alert.market_id for alert in strong_ranked},
        )
        selected_strong = strong_ranked[:max_alerts]
        remaining = max(max_alerts - len(selected_strong), 0)
        selected_medium = medium_ranked[:remaining] if remaining > 0 else []
        selected_alerts = selected_strong + selected_medium

    if not selected_alerts:
        _record_alert_deliveries(
            db,
            filtered_for_delivery,
            included_for_delivery,
            config.user_id,
            now_ts,
            sent_alert_ids=set(),
            skip_reason="no_selected_alerts",
        )
        return {"user_id": str(config.user_id), "sent": False, "reason": "no_selected_alerts"}

    counts = {"REPRICING": 0, "LIQUIDITY_SWEEP": 0, "NOISY": 0}
    for alert in selected_alerts:
        classification = classifier(alert)
        counts[classification.signal_type] = counts.get(classification.signal_type, 0) + 1
    logger.info(
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
        )
        return {"user_id": str(config.user_id), "sent": False, "reason": "empty_message"}

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
        )
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
            response = await client.post(url, json=payload)
        if response.is_success:
            _record_digest_sent(config.user_id, tenant_id, now_ts, window_minutes, actionable_alerts, [])
            _record_alert_deliveries(
                db,
                filtered_for_delivery,
                included_for_delivery,
                config.user_id,
                now_ts,
                sent_alert_ids={alert.id for alert in selected_alerts if alert.id is not None},
            )
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
        )
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
        )
        return {"user_id": str(config.user_id), "sent": False, "status_code": 0, "text": ""}


def _filter_alerts_for_user(
    alerts: list[Alert],
    config: UserDigestConfig,
) -> tuple[list[Alert], list[Alert]]:
    included: list[Alert] = []
    filtered: list[Alert] = []

    for alert in alerts:
        if alert.liquidity < config.min_liquidity:
            filtered.append(alert)
            continue
        if alert.volume_24h < config.min_volume_24h:
            filtered.append(alert)
            continue
        if _alert_abs_move(alert) < config.min_abs_price_move:
            filtered.append(alert)
            continue
        if alert.strength not in config.alert_strengths:
            filtered.append(alert)
            continue
        included.append(alert)

    return included, filtered


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
) -> None:
    rows: list[dict] = []
    for alert in filtered_alerts:
        if alert.id is None:
            continue
        rows.append(
            {
                "alert_id": alert.id,
                "user_id": user_id,
                "delivered_at": delivered_at,
                "delivery_status": DELIVERY_STATUS_FILTERED,
            }
        )

    for alert in included_alerts:
        if alert.id is None:
            continue
        status = DELIVERY_STATUS_SENT if alert.id in sent_alert_ids else DELIVERY_STATUS_SKIPPED
        rows.append(
            {
                "alert_id": alert.id,
                "user_id": user_id,
                "delivered_at": delivered_at,
                "delivery_status": status,
            }
        )

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
        },
    )
    db.execute(stmt)
    db.commit()


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
            response = await client.post(url, json=send_payload)
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
) -> str:
    if total_actionable < 1:
        return ""

    if defaults.THEME_GROUPING_ENABLED:
        return _format_grouped_digest_message(
            alerts,
            window_minutes,
            max_themes_per_digest,
            max_markets_per_theme,
            classifier,
        )

    header = f"<b>PMD - {total_actionable} actionable repricings ({window_minutes}m)</b>"
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

    lines.append("<i>Read-only analytics - Not financial advice</i>")
    return "\n".join(lines).strip()


def _append_fast_section(confirmed_text: str, fast_section: str) -> str:
    if not fast_section:
        return confirmed_text
    footer = "<i>Read-only analytics - Not financial advice</i>"
    if footer in confirmed_text:
        base, _ = confirmed_text.rsplit(footer, 1)
        base = base.rstrip()
        fast_section = fast_section.strip()
        return f"{base}\n\n{fast_section}\n\n{footer}"
    return f"{confirmed_text}\n\n{fast_section}"


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
        lines.append(_format_market_link(rep.market_id))
        lines.append("")

    if include_footer:
        lines.append("<i>Read-only analytics - Not financial advice</i>")
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
) -> str:
    themes = group_alerts_into_themes(alerts, classifier)
    max_themes = max(max_themes_per_digest or defaults.DEFAULT_MAX_THEMES_PER_DIGEST, 1)
    themes = themes[:max_themes]
    header = f"<b>PMD - {len(themes)} theme{'' if len(themes) == 1 else 's'} ({window_minutes}m)</b>"
    lines = [header, ""]

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
        related = [alert for alert in theme.alerts if alert is not rep]
        max_markets = max_markets_per_theme or defaults.DEFAULT_MAX_MARKETS_PER_THEME
        for related_alert in related[: max(int(max_markets), 1)]:
            bullet_title = html.escape(_short_title(related_alert, theme_hint=True))
            bullet_move = _format_compact_move(related_alert)
            bullet_p_yes = _format_p_yes_compact(related_alert)
            lines.append(f"- {bullet_title} | {bullet_move} | {bullet_p_yes}")
        lines.append(_format_market_link(rep.market_id))
        lines.append("")

    lines.append("<i>Read-only analytics - Not financial advice</i>")
    return "\n".join(lines).strip()


def _format_digest_alert(
    alert: Alert,
    idx: int,
    window_minutes: int,
    classifier=None,
    classification: AlertClassification | None = None,
) -> list[str]:
    title = html.escape(alert.title[:120])
    classification = classification or (classifier(alert) if classifier else classify_alert(alert))
    rank_label = "STRONG" if _is_actionable_classification(classification) else "NOTABLE"
    move_text = _format_move(alert)
    p_yes_text = _format_p_yes(alert)
    liquidity_text = _format_liquidity_volume(alert)
    return [
        f"<b>#{idx} {rank_label} - {classification.signal_type} ({classification.confidence})</b>",
        title,
        f"Move: {move_text} | {p_yes_text}",
        liquidity_text,
        f"Suggested action: {classification.suggested_action}",
        _format_market_link(alert.market_id),
    ]


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


def _is_actionable_classification(classification: AlertClassification) -> bool:
    return (
        classification.signal_type == "REPRICING"
        and classification.confidence == "HIGH"
        and classification.suggested_action == "FOLLOW"
    )


def _is_within_actionable_pyes(alert: Alert, p_min: float, p_max: float) -> bool:
    if alert.market_p_yes is None:
        return True
    return p_min <= alert.market_p_yes <= p_max


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


def _format_market_link(market_id: str) -> str:
    safe_id = quote(str(market_id).strip())
    return f"https://polymarket.com/market/{safe_id}"


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


def _missing_price_or_liquidity(alert: Alert) -> bool:
    missing_price = alert.old_price is None or alert.new_price is None
    missing_liquidity = alert.liquidity is None or alert.volume_24h is None
    return missing_price or missing_liquidity


def _label_mapping_unknown(alert: Alert) -> bool:
    mapping_confidence = getattr(alert, "mapping_confidence", None)
    return mapping_confidence != "verified"


def _copilot_daily_count_key(user_id: UUID, now_ts: datetime) -> str:
    date_key = now_ts.date().isoformat()
    return COPILOT_DAILY_COUNT_KEY.format(user_id=user_id, date=date_key)


def _get_copilot_daily_count(user_id: UUID, now_ts: datetime) -> int:
    key = _copilot_daily_count_key(user_id, now_ts)
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
                "reasons": evaluation.reasons,
            }
            for evaluation in sorted(evaluations, key=lambda item: item.theme_key)
        ],
    }
    logger.info(
        "copilot_theme_eval user_id=%s plan_name=%s themes_total=%s themes_eligible_count=%s "
        "daily_count=%s daily_limit=%s sent_this_digest=%s digest_limit=%s cap_reached=%s "
        "selected_themes=%s themes=%s",
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
) -> list[CopilotThemeEvaluation]:
    if not actionable_alerts:
        return []
    now_ts = datetime.now(timezone.utc)
    daily_limit = max(config.max_copilot_per_day, 0)
    digest_limit = max(config.max_copilot_per_digest, 1)
    daily_count = _get_copilot_daily_count(config.user_id, now_ts)
    remaining_daily = max(daily_limit - daily_count, 0)
    sent_this_digest = 0
    cap_reached_reason: str | None = None
    selected_theme_keys: list[str] = []
    if allow_enqueue is None:
        allow_enqueue = config.ai_copilot_enabled

    themes = group_alerts_into_themes(actionable_alerts, classifier)
    actionable_theme_count = len(themes)
    if not themes:
        logger.info(
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
        return []

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
        base_reasons: list[str] = []
        if not allow_enqueue:
            base_reasons.append(CopilotIneligibilityReason.USER_DISABLED.value)
        if theme.key in muted_theme_keys or rep.market_id in muted_market_ids:
            base_reasons.append(CopilotIneligibilityReason.MUTED.value)
        if not _is_within_actionable_pyes(rep, config.p_min, config.p_max):
            base_reasons.append(CopilotIneligibilityReason.P_OUT_OF_BAND.value)
        if rep_classification.signal_type != "REPRICING":
            base_reasons.append(CopilotIneligibilityReason.NOT_REPRICING.value)
        if rep_classification.confidence != "HIGH":
            base_reasons.append(CopilotIneligibilityReason.CONFIDENCE_NOT_HIGH.value)
        if rep_classification.suggested_action != "FOLLOW":
            base_reasons.append(CopilotIneligibilityReason.NOT_FOLLOW.value)

        reasons = list(base_reasons)
        if reasons:
            if _label_mapping_unknown(rep):
                reasons.append(CopilotIneligibilityReason.LABEL_MAPPING_UNKNOWN.value)
            if _missing_price_or_liquidity(rep):
                reasons.append(CopilotIneligibilityReason.MISSING_PRICE_OR_LIQUIDITY.value)
            if _count_snapshot_points(db, rep) < 3:
                reasons.append(CopilotIneligibilityReason.INSUFFICIENT_SNAPSHOTS.value)

        _sort_copilot_reasons(reasons)
        evaluation = CopilotThemeEvaluation(theme.key, rep.market_id, reasons)
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
        logger.info(
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
        return evaluations

    ranked = sorted(
        eligible_themes,
        key=lambda theme: (theme.representative.liquidity, theme.representative.volume_24h),
        reverse=True,
    )

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

    ttl_seconds = max(int(config.copilot_theme_ttl_minutes * 60), 60)
    enqueued = 0
    for idx, theme in enumerate(ranked):
        if remaining_daily <= 0:
            cap_reached_reason = "daily"
            logger.info(
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
        if sent_this_digest >= digest_limit:
            cap_reached_reason = "digest"
            logger.info(
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
        dedupe_key = COPILOT_THEME_DEDUPE_KEY.format(user_id=config.user_id, theme_key=theme.key)
        try:
            marked = redis_conn.set(dedupe_key, now_ts.isoformat(), nx=True, ex=ttl_seconds)
        except Exception:
            logger.exception(
                "copilot_theme_dedupe_failed user_id=%s theme_key=%s",
                config.user_id,
                theme.key,
            )
            marked = True
        if not marked:
            evaluations_by_key[theme.key].add_reason(
                CopilotIneligibilityReason.COPILOT_DEDUPE_ACTIVE.value
            )
            continue
        selected_theme_keys.append(theme.key)
        if allow_enqueue:
            queue.enqueue(ai_recommendation_job, str(config.user_id), alert.id)
            enqueued += 1
        sent_this_digest += 1
        remaining_daily -= 1

    logger.info(
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
    return evaluations


def _build_cap_reached_message(
    config: UserDigestConfig,
    daily_usage: str,
    digest_usage: str,
    cap_reached: str | None,
) -> str | None:
    if not cap_reached:
        return None
    plan_label = (config.plan_name or "basic").upper()
    usage_text = daily_usage if cap_reached == "daily" else digest_usage
    period_label = "today" if cap_reached == "daily" else "this digest"
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
