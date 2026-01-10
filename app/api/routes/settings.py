from typing import Any
import uuid

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel
from sqlalchemy.orm import Session

from ...core.effective_settings import invalidate_effective_settings_cache, resolve_effective_settings
from ...core.user_settings import get_effective_user_settings
from ...db import get_db
from ...deps import _require_session_user
from ...models import UserAlertPreference

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
    if "min_liquidity" in provided and provided["min_liquidity"] is not None:
        if provided["min_liquidity"] < 0:
            raise HTTPException(status_code=400, detail="min_liquidity_invalid")
    if "min_volume_24h" in provided and provided["min_volume_24h"] is not None:
        if provided["min_volume_24h"] < 0:
            raise HTTPException(status_code=400, detail="min_volume_24h_invalid")
    if "min_abs_price_move" in provided and provided["min_abs_price_move"] is not None:
        if provided["min_abs_price_move"] < 0:
            raise HTTPException(status_code=400, detail="min_abs_price_move_invalid")
    if "digest_window_minutes" in provided and provided["digest_window_minutes"] is not None:
        if provided["digest_window_minutes"] <= 0:
            raise HTTPException(status_code=400, detail="digest_window_minutes_invalid")
    for key in ("max_alerts_per_digest", "max_themes_per_digest", "max_markets_per_theme"):
        if key in provided and provided[key] is not None and provided[key] < 0:
            raise HTTPException(status_code=400, detail=f"{key}_invalid")
    if "fast_window_minutes" in provided and provided["fast_window_minutes"] is not None:
        if provided["fast_window_minutes"] <= 0:
            raise HTTPException(status_code=400, detail="fast_window_minutes_invalid")
    for key in ("fast_max_themes_per_digest", "fast_max_markets_per_theme"):
        if key in provided and provided[key] is not None and provided[key] < 0:
            raise HTTPException(status_code=400, detail=f"{key}_invalid")

    if "p_min" in provided or "p_max" in provided:
        p_min = provided.get("p_min", effective.p_min)
        p_max = provided.get("p_max", effective.p_max)
        if p_min is not None and (p_min < 0 or p_min > 1):
            raise HTTPException(status_code=400, detail="p_min_invalid")
        if p_max is not None and (p_max < 0 or p_max > 1):
            raise HTTPException(status_code=400, detail="p_max_invalid")
        if p_min is not None and p_max is not None and p_min >= p_max:
            raise HTTPException(status_code=400, detail="p_range_invalid")

    if "copilot_enabled" in provided:
        plan_name = (effective.plan_name or "").strip().lower()
        if plan_name not in {"pro", "elite"}:
            raise HTTPException(status_code=403, detail="copilot_not_available")
        user.copilot_enabled = bool(provided["copilot_enabled"])

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
        },
        "preferences": _build_preferences_payload(refreshed_pref),
        "effective": _build_effective_payload(effective),
        "baseline": _build_effective_payload(baseline),
    }
