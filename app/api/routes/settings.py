from typing import Any
import json
import uuid

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel
from sqlalchemy.orm import Session

from ...core.effective_settings import invalidate_effective_settings_cache, resolve_effective_settings
from ...core.user_settings import get_effective_user_settings
from ...db import get_db
from ...deps import _require_session_user
from ...models import UserAlertPreference
from ...services.entitlements_service import _build_plan_features, _build_settings_limits

router = APIRouter()

_ALLOWED_STRENGTHS = {"LOW", "MEDIUM", "HIGH", "STRONG"}


def _normalize_strengths_input(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, (list, tuple, set)):
        parts = {str(part).strip().upper() for part in value if str(part).strip()}
    elif isinstance(value, str):
        parts = {part.strip().upper() for part in value.split(",") if part.strip()}
    else:
        return None
    filtered = [part for part in parts if part in _ALLOWED_STRENGTHS]
    return ",".join(sorted(filtered)) if filtered else None


def _strengths_to_list(value: str | None) -> list[str]:
    if not value:
        return []
    return [part.strip().upper() for part in value.split(",") if part.strip()]


class SettingsPatch(BaseModel):
    copilot_enabled: bool | None = None
    developer_mode: bool | None = None
    min_liquidity: float | None = None
    min_volume_24h: float | None = None
    min_abs_price_move: float | None = None
    alert_strengths: list[str] | str | None = None
    digest_window_minutes: int | None = None
    max_alerts_per_digest: int | None = None
    max_themes_per_digest: int | None = None
    max_markets_per_theme: int | None = None
    p_min: float | None = None
    p_max: float | None = None
    fast_signals_enabled: bool | None = None
    fast_window_minutes: int | None = None
    fast_max_themes_per_digest: int | None = None
    fast_max_markets_per_theme: int | None = None


def _build_preferences_payload(pref: UserAlertPreference | None) -> dict[str, object]:
    return {
        "min_liquidity": pref.min_liquidity if pref else None,
        "min_volume_24h": pref.min_volume_24h if pref else None,
        "min_abs_price_move": pref.min_abs_price_move if pref else None,
        "alert_strengths": _strengths_to_list(pref.alert_strengths if pref else None),
        "digest_window_minutes": pref.digest_window_minutes if pref else None,
        "max_alerts_per_digest": pref.max_alerts_per_digest if pref else None,
        "max_themes_per_digest": pref.max_themes_per_digest if pref else None,
        "max_markets_per_theme": pref.max_markets_per_theme if pref else None,
        "p_min": pref.p_min if pref else None,
        "p_max": pref.p_max if pref else None,
        "fast_signals_enabled": pref.fast_signals_enabled if pref else None,
        "fast_window_minutes": pref.fast_window_minutes if pref else None,
        "fast_max_themes_per_digest": pref.fast_max_themes_per_digest if pref else None,
        "fast_max_markets_per_theme": pref.fast_max_markets_per_theme if pref else None,
    }


def _build_effective_payload(effective) -> dict[str, object]:
    return {
        "plan_name": effective.plan_name,
        "copilot_enabled": effective.copilot_enabled,
        "allowed_strengths": sorted(effective.allowed_strengths),
        "digest_window_minutes": effective.digest_window_minutes,
        "max_alerts_per_digest": effective.max_alerts_per_digest,
        "max_themes_per_digest": effective.max_themes_per_digest,
        "max_markets_per_theme": effective.max_markets_per_theme,
        "min_liquidity": effective.min_liquidity,
        "min_volume_24h": effective.min_volume_24h,
        "min_abs_price_move": effective.min_abs_move,
        "p_min": effective.p_min,
        "p_max": effective.p_max,
        "fast_signals_enabled": effective.fast_signals_enabled,
        "fast_window_minutes": effective.fast_window_minutes,
        "fast_max_themes_per_digest": effective.fast_max_themes_per_digest,
        "fast_max_markets_per_theme": effective.fast_max_markets_per_theme,
        "allow_fast_alerts": effective.allow_fast_alerts,
        "fast_mode": effective.fast_mode,
    }


@router.get("/settings/me")
def settings_me(request: Request, db: Session = Depends(get_db)):
    user, _ = _require_session_user(request, db)
    pref = (
        db.query(UserAlertPreference)
        .filter(UserAlertPreference.user_id == user.user_id)
        .one_or_none()
    )
    effective = get_effective_user_settings(user, pref=pref, db=db)
    baseline = resolve_effective_settings(user, pref=None)
    return {
        "user_id": str(user.user_id),
        "user": {
            "copilot_enabled": bool(getattr(user, "copilot_enabled", False)),
            "developer_mode": _get_developer_mode(user),
        },
        "preferences": _build_preferences_payload(pref),
        "effective": _build_effective_payload(effective),
        "baseline": _build_effective_payload(baseline),
    }


@router.patch("/settings/me")
async def settings_update(
    request: Request,
    payload: SettingsPatch,
    db: Session = Depends(get_db),
):
    user, _ = _require_session_user(request, db)
    pref = (
        db.query(UserAlertPreference)
        .filter(UserAlertPreference.user_id == user.user_id)
        .one_or_none()
    )
    provided = payload.model_dump(exclude_unset=True)
    if not provided:
        raise HTTPException(status_code=400, detail="no_updates")

    effective = get_effective_user_settings(user, pref=pref, db=db)
    baseline = resolve_effective_settings(user, pref=None)
    limits = _build_settings_limits(baseline)
    features = _build_plan_features(getattr(user, "plan", None), baseline)
    errors: dict[str, str] = {}

    def _get_limit(field: str, key: str):
        return limits.get(field, {}).get(key)

    def _ensure_min(field: str, value: float | int | None, minimum: float | int | None):
        if value is None or minimum is None:
            return
        if value < minimum:
            errors[field] = f"Must be >= {minimum}."

    def _ensure_max(field: str, value: float | int | None, maximum: float | int | None):
        if value is None or maximum is None:
            return
        if value > maximum:
            errors[field] = f"Must be <= {maximum}."

    def _ensure_allowed(field: str, value: float | int | None, allowed: list[int] | None):
        if value is None or not allowed:
            return
        if value not in allowed:
            allowed_text = ", ".join(str(item) for item in sorted(allowed))
            errors[field] = f"Allowed values: {allowed_text}."

    def _parse_strengths(value: Any) -> list[str]:
        if value is None:
            return []
        if isinstance(value, (list, tuple, set)):
            parts = {str(part).strip().upper() for part in value if str(part).strip()}
        elif isinstance(value, str):
            parts = {part.strip().upper() for part in value.split(",") if part.strip()}
        else:
            return []
        return sorted(parts)

    for field in ("min_liquidity", "min_volume_24h", "min_abs_price_move"):
        if field in provided and provided[field] is not None:
            if provided[field] < 0:
                errors[field] = "Must be zero or greater."
            _ensure_min(field, provided[field], _get_limit(field, "min"))

    if "digest_window_minutes" in provided and provided["digest_window_minutes"] is not None:
        if provided["digest_window_minutes"] <= 0:
            errors["digest_window_minutes"] = "Must be greater than zero."
        _ensure_min("digest_window_minutes", provided["digest_window_minutes"], _get_limit("digest_window_minutes", "min"))
        _ensure_allowed(
            "digest_window_minutes",
            provided["digest_window_minutes"],
            _get_limit("digest_window_minutes", "allowed_values"),
        )

    for field in ("max_alerts_per_digest", "max_themes_per_digest", "max_markets_per_theme"):
        if field in provided and provided[field] is not None:
            if provided[field] < 0:
                errors[field] = "Must be zero or greater."
            _ensure_max(field, provided[field], _get_limit(field, "max"))

    if "fast_window_minutes" in provided and provided["fast_window_minutes"] is not None:
        if provided["fast_window_minutes"] <= 0:
            errors["fast_window_minutes"] = "Must be greater than zero."
        _ensure_min("fast_window_minutes", provided["fast_window_minutes"], _get_limit("fast_window_minutes", "min"))

    for field in ("fast_max_themes_per_digest", "fast_max_markets_per_theme"):
        if field in provided and provided[field] is not None:
            if provided[field] < 0:
                errors[field] = "Must be zero or greater."
            _ensure_max(field, provided[field], _get_limit(field, "max"))

    if "p_min" in provided or "p_max" in provided:
        p_min = provided.get("p_min", effective.p_min)
        p_max = provided.get("p_max", effective.p_max)
        if p_min is not None and (p_min < 0 or p_min > 1):
            errors["p_min"] = "Must be between 0 and 1."
        if p_max is not None and (p_max < 0 or p_max > 1):
            errors["p_max"] = "Must be between 0 and 1."
        _ensure_min("p_min", p_min, _get_limit("p_min", "min"))
        _ensure_max("p_min", p_min, _get_limit("p_min", "max"))
        _ensure_min("p_max", p_max, _get_limit("p_max", "min"))
        _ensure_max("p_max", p_max, _get_limit("p_max", "max"))
        if p_min is not None and p_max is not None and p_min >= p_max:
            errors["p_range"] = "Use values between 0 and 1 with min < max."

    if "alert_strengths" in provided:
        allowed_strengths = set(_get_limit("alert_strengths", "allowed_values") or [])
        strengths = _parse_strengths(provided.get("alert_strengths"))
        if not strengths:
            errors["alert_strengths"] = "Select at least one strength."
        else:
            invalid = [strength for strength in strengths if strength not in allowed_strengths]
            if invalid:
                allowed_text = ", ".join(sorted(allowed_strengths)) if allowed_strengths else "none"
                errors["alert_strengths"] = f"Allowed strengths: {allowed_text}."

    if "copilot_enabled" in provided and provided["copilot_enabled"]:
        if not features.get("copilot_enabled"):
            errors["copilot_enabled"] = "Copilot is not available on your plan."

    fast_fields = {
        "fast_signals_enabled",
        "fast_window_minutes",
        "fast_max_themes_per_digest",
        "fast_max_markets_per_theme",
    }
    if any(field in provided and provided[field] not in (None, False) for field in fast_fields):
        if not features.get("fast_signals_enabled"):
            errors["fast_signals_enabled"] = "FAST signals are not available on your plan."

    if errors:
        raise HTTPException(status_code=422, detail={"errors": errors, "code": "validation_failed"})

    if "copilot_enabled" in provided:
        user.copilot_enabled = bool(provided["copilot_enabled"])
    if "developer_mode" in provided:
        _set_developer_mode(user, provided.get("developer_mode"))

    if pref is None:
        pref = UserAlertPreference(user_id=uuid.UUID(str(user.user_id)))
        db.add(pref)

    if "alert_strengths" in provided:
        normalized = _normalize_strengths_input(provided.get("alert_strengths"))
        pref.alert_strengths = normalized

    for key in (
        "min_liquidity",
        "min_volume_24h",
        "min_abs_price_move",
        "digest_window_minutes",
        "max_alerts_per_digest",
        "max_themes_per_digest",
        "max_markets_per_theme",
        "p_min",
        "p_max",
        "fast_signals_enabled",
        "fast_window_minutes",
        "fast_max_themes_per_digest",
        "fast_max_markets_per_theme",
    ):
        if key in provided:
            setattr(pref, key, provided[key])

    db.commit()
    invalidate_effective_settings_cache(user.user_id)

    refreshed_pref = (
        db.query(UserAlertPreference)
        .filter(UserAlertPreference.user_id == user.user_id)
        .one_or_none()
    )
    effective = get_effective_user_settings(user, pref=refreshed_pref, db=db)
    baseline = resolve_effective_settings(user, pref=None)
    return {
        "user_id": str(user.user_id),
        "user": {
            "copilot_enabled": bool(getattr(user, "copilot_enabled", False)),
            "developer_mode": _get_developer_mode(user),
        },
        "preferences": _build_preferences_payload(refreshed_pref),
        "effective": _build_effective_payload(effective),
        "baseline": _build_effective_payload(baseline),
    }
def _load_overrides(user) -> dict:
    raw = getattr(user, "overrides_json", None)
    if not raw:
        return {}
    if isinstance(raw, dict):
        return dict(raw)
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            return {}
        return dict(parsed) if isinstance(parsed, dict) else {}
    return {}


def _get_developer_mode(user) -> bool:
    overrides = _load_overrides(user)
    return bool(overrides.get("developer_mode", False))


def _set_developer_mode(user, value: bool | None) -> None:
    overrides = _load_overrides(user)
    if value is None:
        overrides.pop("developer_mode", None)
    else:
        overrides["developer_mode"] = bool(value)
    user.overrides_json = overrides or None
