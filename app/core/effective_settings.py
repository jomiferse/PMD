import json
import logging
from dataclasses import dataclass
from uuid import UUID

import redis
from sqlalchemy.orm import Session

from ..models import Plan, User, UserAlertPreference
from ..settings import settings
from . import defaults
from .plans import plan_alert_rules

logger = logging.getLogger(__name__)
redis_conn = redis.from_url(settings.REDIS_URL)

EFFECTIVE_SETTINGS_CACHE_KEY = "user:effective_settings:{user_id}"
EFFECTIVE_SETTINGS_CACHE_TTL_SECONDS = 600

_CODE_DEFAULTS = {
    "max_copilot_per_day": defaults.DEFAULT_MAX_COPILOT_PER_DAY,
    "max_fast_copilot_per_day": defaults.DEFAULT_MAX_FAST_COPILOT_PER_DAY,
    "max_copilot_per_digest": defaults.DEFAULT_MAX_COPILOT_PER_DIGEST,
    "copilot_theme_ttl_minutes": defaults.DEFAULT_COPILOT_THEME_TTL_MINUTES,
    "digest_window_minutes": defaults.DEFAULT_DIGEST_WINDOW_MINUTES,
    "max_themes_per_digest": defaults.DEFAULT_MAX_THEMES_PER_DIGEST,
    "max_markets_per_theme": defaults.DEFAULT_MAX_MARKETS_PER_THEME,
    "max_alerts_per_digest": defaults.DEFAULT_MAX_ALERTS_PER_DIGEST,
    "min_liquidity": defaults.DEFAULT_MIN_LIQUIDITY,
    "min_volume_24h": defaults.DEFAULT_MIN_VOLUME_24H,
    "min_abs_move": defaults.DEFAULT_MIN_ABS_MOVE,
    "p_min": defaults.DEFAULT_P_MIN,
    "p_max": defaults.DEFAULT_P_MAX,
    "p_soft_min": defaults.DEFAULT_SOFT_P_MIN,
    "p_soft_max": defaults.DEFAULT_SOFT_P_MAX,
    "p_strict_min": defaults.DEFAULT_STRICT_P_MIN,
    "p_strict_max": defaults.DEFAULT_STRICT_P_MAX,
    "allowed_strengths": set(defaults.DEFAULT_ALLOWED_STRENGTHS),
    "fast_signals_enabled": defaults.DEFAULT_FAST_SIGNALS_ENABLED,
    "allow_info_alerts": defaults.DEFAULT_ALLOW_INFO_ALERTS,
    "allow_fast_alerts": defaults.DEFAULT_ALLOW_FAST_ALERTS,
    "fast_window_minutes": defaults.DEFAULT_FAST_WINDOW_MINUTES,
    "fast_max_themes_per_digest": defaults.DEFAULT_FAST_MAX_THEMES_PER_DIGEST,
    "fast_max_markets_per_theme": defaults.DEFAULT_FAST_MAX_MARKETS_PER_THEME,
}


@dataclass(frozen=True)
class EffectiveSettings:
    plan_name: str | None
    copilot_enabled: bool
    max_copilot_per_day: int
    max_fast_copilot_per_day: int
    max_copilot_per_digest: int
    copilot_theme_ttl_minutes: int
    digest_window_minutes: int
    max_themes_per_digest: int
    max_markets_per_theme: int
    max_alerts_per_digest: int
    min_liquidity: float
    min_volume_24h: float
    min_abs_move: float
    p_min: float
    p_max: float
    p_soft_min: float
    p_soft_max: float
    p_strict_min: float
    p_strict_max: float
    allowed_strengths: set[str]
    fast_signals_enabled: bool
    allow_info_alerts: bool
    allow_fast_alerts: bool
    fast_window_minutes: int
    fast_max_themes_per_digest: int
    fast_max_markets_per_theme: int


def get_effective_settings(db: Session, user_id: UUID) -> EffectiveSettings:
    cached = _load_cached(user_id)
    if cached:
        return cached

    user = (
        db.query(User)
        .filter(User.user_id == user_id)
        .one_or_none()
    )
    if not user:
        raise ValueError(f"user not found: {user_id}")
    alert_pref = (
        db.query(UserAlertPreference)
        .filter(UserAlertPreference.user_id == user_id)
        .one_or_none()
    )
    effective = resolve_effective_settings(user, alert_pref)
    _store_cached(user.user_id, effective)
    return effective


def get_effective_settings_for_user(
    user: User,
    pref: UserAlertPreference | None = None,
    db: Session | None = None,
) -> EffectiveSettings:
    cached = _load_cached(user.user_id)
    if cached:
        return cached

    if pref is None and db is not None:
        pref = (
            db.query(UserAlertPreference)
            .filter(UserAlertPreference.user_id == user.user_id)
            .one_or_none()
        )
    effective = resolve_effective_settings(user, pref)
    _store_cached(user.user_id, effective)
    return effective


def resolve_effective_settings(
    user: User,
    pref: UserAlertPreference | None = None,
) -> EffectiveSettings:
    effective = dict(_CODE_DEFAULTS)
    plan = getattr(user, "plan", None)
    rules = plan_alert_rules(getattr(plan, "name", None))
    effective["allow_info_alerts"] = rules.allow_info_alerts
    effective["allow_fast_alerts"] = rules.allow_fast_alerts
    effective["p_soft_min"], effective["p_soft_max"] = rules.soft_band
    effective["p_strict_min"], effective["p_strict_max"] = rules.strict_band
    plan_copilot_enabled = True
    if plan is not None and getattr(plan, "copilot_enabled", None) is not None:
        plan_copilot_enabled = bool(plan.copilot_enabled)
    if plan is not None:
        _apply_plan_overrides(effective, plan)

    if pref is not None:
        _apply_user_alert_preferences(effective, pref)
    overrides = _load_overrides(user)
    if overrides:
        _apply_overrides(effective, overrides)

    return EffectiveSettings(
        plan_name=getattr(plan, "name", None),
        copilot_enabled=bool(getattr(user, "copilot_enabled", False)) and plan_copilot_enabled,
        max_copilot_per_day=int(effective["max_copilot_per_day"]),
        max_fast_copilot_per_day=int(effective["max_fast_copilot_per_day"]),
        max_copilot_per_digest=int(effective["max_copilot_per_digest"]),
        copilot_theme_ttl_minutes=int(effective["copilot_theme_ttl_minutes"]),
        digest_window_minutes=int(effective["digest_window_minutes"]),
        max_themes_per_digest=int(effective["max_themes_per_digest"]),
        max_markets_per_theme=int(effective["max_markets_per_theme"]),
        max_alerts_per_digest=int(effective["max_alerts_per_digest"]),
        min_liquidity=float(effective["min_liquidity"]),
        min_volume_24h=float(effective["min_volume_24h"]),
        min_abs_move=float(effective["min_abs_move"]),
        p_min=float(effective["p_min"]),
        p_max=float(effective["p_max"]),
        p_soft_min=float(effective["p_soft_min"]),
        p_soft_max=float(effective["p_soft_max"]),
        p_strict_min=float(effective["p_strict_min"]),
        p_strict_max=float(effective["p_strict_max"]),
        allowed_strengths=set(effective["allowed_strengths"]),
        fast_signals_enabled=bool(effective["fast_signals_enabled"]),
        allow_info_alerts=bool(effective["allow_info_alerts"]),
        allow_fast_alerts=bool(effective["allow_fast_alerts"]),
        fast_window_minutes=int(effective["fast_window_minutes"]),
        fast_max_themes_per_digest=int(effective["fast_max_themes_per_digest"]),
        fast_max_markets_per_theme=int(effective["fast_max_markets_per_theme"]),
    )


def invalidate_effective_settings_cache(user_id: UUID) -> None:
    key = EFFECTIVE_SETTINGS_CACHE_KEY.format(user_id=user_id)
    try:
        redis_conn.delete(key)
    except Exception:
        logger.exception("effective_settings_cache_invalidate_failed user_id=%s", user_id)


def _apply_plan_overrides(effective: dict, plan: Plan) -> None:
    for key in _CODE_DEFAULTS:
        value = getattr(plan, key, None)
        if value is None:
            continue
        if key == "allowed_strengths":
            parsed = _parse_strengths(value)
            if parsed:
                effective[key] = parsed
            continue
        effective[key] = value


def _apply_user_alert_preferences(effective: dict, pref: UserAlertPreference) -> None:
    if pref.min_liquidity is not None:
        effective["min_liquidity"] = pref.min_liquidity
    if pref.min_volume_24h is not None:
        effective["min_volume_24h"] = pref.min_volume_24h
    if pref.min_abs_price_move is not None:
        effective["min_abs_move"] = pref.min_abs_price_move
    if pref.digest_window_minutes is not None:
        effective["digest_window_minutes"] = pref.digest_window_minutes
    if pref.max_alerts_per_digest is not None:
        effective["max_alerts_per_digest"] = pref.max_alerts_per_digest
    if getattr(pref, "max_themes_per_digest", None) is not None:
        effective["max_themes_per_digest"] = pref.max_themes_per_digest
    if getattr(pref, "max_markets_per_theme", None) is not None:
        effective["max_markets_per_theme"] = pref.max_markets_per_theme
    if getattr(pref, "p_min", None) is not None:
        effective["p_min"] = pref.p_min
    if getattr(pref, "p_max", None) is not None:
        effective["p_max"] = pref.p_max
    if getattr(pref, "fast_window_minutes", None) is not None:
        effective["fast_window_minutes"] = pref.fast_window_minutes
    if getattr(pref, "fast_max_themes_per_digest", None) is not None:
        effective["fast_max_themes_per_digest"] = pref.fast_max_themes_per_digest
    if getattr(pref, "fast_max_markets_per_theme", None) is not None:
        effective["fast_max_markets_per_theme"] = pref.fast_max_markets_per_theme
    if pref.alert_strengths:
        parsed = _parse_strengths(pref.alert_strengths)
        if parsed:
            effective["allowed_strengths"] = parsed
    if pref.fast_signals_enabled is not None:
        effective["fast_signals_enabled"] = bool(pref.fast_signals_enabled)


def _load_overrides(user: User) -> dict | None:
    raw = getattr(user, "overrides_json", None)
    if not raw:
        return None
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            return None
        return parsed if isinstance(parsed, dict) else None
    return None


def _apply_overrides(effective: dict, overrides: dict) -> None:
    for key in _CODE_DEFAULTS:
        if key not in overrides:
            continue
        raw_value = overrides.get(key)
        if raw_value is None:
            continue
        if key == "allowed_strengths":
            parsed = _parse_strengths(raw_value)
            if parsed:
                effective[key] = parsed
            continue
        if key in {"fast_signals_enabled"}:
            parsed = _coerce_bool(raw_value)
            if parsed is None:
                continue
            effective[key] = parsed
            continue
        if key in {"allow_info_alerts", "allow_fast_alerts"}:
            parsed = _coerce_bool(raw_value)
            if parsed is None:
                continue
            effective[key] = parsed
            continue
        if key in {
            "max_copilot_per_day",
            "max_fast_copilot_per_day",
            "max_copilot_per_digest",
            "copilot_theme_ttl_minutes",
            "digest_window_minutes",
            "max_themes_per_digest",
            "max_markets_per_theme",
            "max_alerts_per_digest",
            "fast_window_minutes",
            "fast_max_themes_per_digest",
            "fast_max_markets_per_theme",
        }:
            parsed = _coerce_int(raw_value)
            if parsed is None:
                continue
            effective[key] = parsed
            continue
        parsed = _coerce_float(raw_value)
        if parsed is None:
            continue
        effective[key] = parsed


def _coerce_int(value) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _coerce_float(value) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _coerce_bool(value) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and value in {0, 1}:
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes", "y"}:
            return True
        if normalized in {"false", "0", "no", "n"}:
            return False
    return None


def _parse_strengths(value) -> set[str]:
    if value is None:
        return set()
    if isinstance(value, (list, tuple, set)):
        parts = {str(part).strip().upper() for part in value if str(part).strip()}
        return {part for part in parts if part}
    if isinstance(value, str):
        raw = value.strip()
        try:
            decoded = json.loads(raw)
            if isinstance(decoded, (list, tuple, set)):
                return {str(part).strip().upper() for part in decoded if str(part).strip()}
            if isinstance(decoded, str):
                raw = decoded
        except json.JSONDecodeError:
            pass

        parts = {part.strip().upper() for part in raw.split(",") if part.strip()}
        return parts
    return set()


def _load_cached(user_id: UUID) -> EffectiveSettings | None:
    key = EFFECTIVE_SETTINGS_CACHE_KEY.format(user_id=user_id)
    try:
        raw = redis_conn.get(key)
    except Exception:
        logger.exception("effective_settings_cache_read_failed user_id=%s", user_id)
        return None
    if not raw:
        return None
    try:
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode()
        payload = json.loads(raw)
    except Exception:
        return None
    return _deserialize_effective_settings(payload)


def _store_cached(user_id: UUID, settings_obj: EffectiveSettings) -> None:
    key = EFFECTIVE_SETTINGS_CACHE_KEY.format(user_id=user_id)
    payload = _serialize_effective_settings(settings_obj)
    try:
        redis_conn.set(
            key,
            json.dumps(payload, ensure_ascii=True),
            ex=EFFECTIVE_SETTINGS_CACHE_TTL_SECONDS,
        )
    except Exception:
        logger.exception("effective_settings_cache_write_failed user_id=%s", user_id)


def _serialize_effective_settings(settings_obj: EffectiveSettings) -> dict:
    return {
        "plan_name": settings_obj.plan_name,
        "copilot_enabled": settings_obj.copilot_enabled,
        "max_copilot_per_day": settings_obj.max_copilot_per_day,
        "max_fast_copilot_per_day": settings_obj.max_fast_copilot_per_day,
        "max_copilot_per_digest": settings_obj.max_copilot_per_digest,
        "copilot_theme_ttl_minutes": settings_obj.copilot_theme_ttl_minutes,
        "digest_window_minutes": settings_obj.digest_window_minutes,
        "max_themes_per_digest": settings_obj.max_themes_per_digest,
        "max_markets_per_theme": settings_obj.max_markets_per_theme,
        "max_alerts_per_digest": settings_obj.max_alerts_per_digest,
        "min_liquidity": settings_obj.min_liquidity,
        "min_volume_24h": settings_obj.min_volume_24h,
        "min_abs_move": settings_obj.min_abs_move,
        "p_min": settings_obj.p_min,
        "p_max": settings_obj.p_max,
        "p_soft_min": settings_obj.p_soft_min,
        "p_soft_max": settings_obj.p_soft_max,
        "p_strict_min": settings_obj.p_strict_min,
        "p_strict_max": settings_obj.p_strict_max,
        "allowed_strengths": sorted(settings_obj.allowed_strengths),
        "fast_signals_enabled": settings_obj.fast_signals_enabled,
        "allow_info_alerts": settings_obj.allow_info_alerts,
        "allow_fast_alerts": settings_obj.allow_fast_alerts,
        "fast_window_minutes": settings_obj.fast_window_minutes,
        "fast_max_themes_per_digest": settings_obj.fast_max_themes_per_digest,
        "fast_max_markets_per_theme": settings_obj.fast_max_markets_per_theme,
    }


def _deserialize_effective_settings(payload: dict) -> EffectiveSettings | None:
    try:
        return EffectiveSettings(
            plan_name=payload.get("plan_name"),
            copilot_enabled=bool(payload.get("copilot_enabled", False)),
            max_copilot_per_day=int(payload.get("max_copilot_per_day", 0)),
            max_fast_copilot_per_day=int(payload.get("max_fast_copilot_per_day", 0)),
            max_copilot_per_digest=int(payload.get("max_copilot_per_digest", 1)),
            copilot_theme_ttl_minutes=int(payload.get("copilot_theme_ttl_minutes", 0)),
            digest_window_minutes=int(payload.get("digest_window_minutes", 0)),
            max_themes_per_digest=int(payload.get("max_themes_per_digest", 0)),
            max_markets_per_theme=int(payload.get("max_markets_per_theme", 0)),
            max_alerts_per_digest=int(payload.get("max_alerts_per_digest", 0)),
            min_liquidity=float(payload.get("min_liquidity", 0.0)),
            min_volume_24h=float(payload.get("min_volume_24h", 0.0)),
            min_abs_move=float(payload.get("min_abs_move", 0.0)),
            p_min=float(payload.get("p_min", 0.0)),
            p_max=float(payload.get("p_max", 0.0)),
            p_soft_min=float(payload.get("p_soft_min", defaults.DEFAULT_SOFT_P_MIN)),
            p_soft_max=float(payload.get("p_soft_max", defaults.DEFAULT_SOFT_P_MAX)),
            p_strict_min=float(payload.get("p_strict_min", defaults.DEFAULT_STRICT_P_MIN)),
            p_strict_max=float(payload.get("p_strict_max", defaults.DEFAULT_STRICT_P_MAX)),
            allowed_strengths=set(payload.get("allowed_strengths", [])),
            fast_signals_enabled=bool(payload.get("fast_signals_enabled", False)),
            allow_info_alerts=bool(payload.get("allow_info_alerts", defaults.DEFAULT_ALLOW_INFO_ALERTS)),
            allow_fast_alerts=bool(payload.get("allow_fast_alerts", defaults.DEFAULT_ALLOW_FAST_ALERTS)),
            fast_window_minutes=int(payload.get("fast_window_minutes", 0)),
            fast_max_themes_per_digest=int(payload.get("fast_max_themes_per_digest", 0)),
            fast_max_markets_per_theme=int(payload.get("fast_max_markets_per_theme", 0)),
        )
    except Exception:
        return None
