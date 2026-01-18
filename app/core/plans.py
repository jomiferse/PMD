from __future__ import annotations

from dataclasses import dataclass

from . import defaults
from .alert_classification import AlertClass


DEFAULT_PLAN_NAME = "basic"
RECOMMENDED_PLAN_NAME = "pro"
UPGRADE_PATH = {
    "basic": "pro",
    "pro": "elite",
    "elite": None,
}


def recommended_plan_name() -> str:
    return RECOMMENDED_PLAN_NAME


def upgrade_target_name(current_plan: str | None) -> str | None:
    if not current_plan:
        return RECOMMENDED_PLAN_NAME
    normalized = str(current_plan).strip().lower()
    if normalized in UPGRADE_PATH:
        return UPGRADE_PATH[normalized]
    return RECOMMENDED_PLAN_NAME


@dataclass(frozen=True)
class PlanAlertRules:
    allow_info_alerts: bool
    allow_fast_alerts: bool
    soft_band: tuple[float, float]
    strict_band: tuple[float, float]
    allowed_classes: tuple[AlertClass, ...]


PLAN_ALERT_RULES: dict[str, PlanAlertRules] = {
    "basic": PlanAlertRules(
        allow_info_alerts=True,
        allow_fast_alerts=False,
        soft_band=(defaults.DEFAULT_SOFT_P_MIN, defaults.DEFAULT_SOFT_P_MAX),
        strict_band=(defaults.DEFAULT_STRICT_P_MIN, defaults.DEFAULT_STRICT_P_MAX),
        allowed_classes=(AlertClass.ACTIONABLE_STANDARD, AlertClass.INFO_ONLY),
    ),
    "pro": PlanAlertRules(
        allow_info_alerts=True,
        allow_fast_alerts=False,
        soft_band=(defaults.DEFAULT_SOFT_P_MIN, defaults.DEFAULT_SOFT_P_MAX),
        strict_band=(defaults.DEFAULT_STRICT_P_MIN, defaults.DEFAULT_STRICT_P_MAX),
        allowed_classes=(AlertClass.ACTIONABLE_STANDARD, AlertClass.INFO_ONLY),
    ),
    "elite": PlanAlertRules(
        allow_info_alerts=True,
        allow_fast_alerts=True,
        soft_band=(defaults.DEFAULT_SOFT_P_MIN, defaults.DEFAULT_SOFT_P_MAX),
        strict_band=(defaults.DEFAULT_STRICT_P_MIN, defaults.DEFAULT_STRICT_P_MAX),
        allowed_classes=(
            AlertClass.ACTIONABLE_FAST,
            AlertClass.ACTIONABLE_STANDARD,
            AlertClass.INFO_ONLY,
        ),
    ),
}
PLAN_ALERT_RULES["_default"] = PLAN_ALERT_RULES["pro"]


def plan_alert_rules(plan_name: str | None) -> PlanAlertRules:
    normalized = (plan_name or "").strip().lower()
    return PLAN_ALERT_RULES.get(normalized, PLAN_ALERT_RULES["_default"])
