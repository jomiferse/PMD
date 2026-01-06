from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from . import defaults
from .alert_classification import AlertClass


DEFAULT_PLAN_NAME = "basic"
RECOMMENDED_PLAN_NAME = "pro"
UPGRADE_PATH = {
    "basic": "pro",
    "pro": "elite",
    "elite": None,
}


@dataclass(frozen=True)
class PlanSeed:
    name: str
    price_monthly: float | None
    copilot_enabled: bool
    max_copilot_per_day: int
    max_fast_copilot_per_day: int
    max_copilot_per_hour: int
    max_copilot_per_digest: int
    copilot_theme_ttl_minutes: int
    digest_window_minutes: int
    max_themes_per_digest: int
    max_alerts_per_digest: int
    max_markets_per_theme: int
    min_liquidity: float
    min_volume_24h: float
    min_abs_move: float
    p_min: float
    p_max: float
    allowed_strengths: str
    fast_signals_enabled: bool
    fast_mode: str
    fast_window_minutes: int
    fast_max_themes_per_digest: int
    fast_max_markets_per_theme: int

    def as_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "price_monthly": self.price_monthly,
            "copilot_enabled": self.copilot_enabled,
            "max_copilot_per_day": self.max_copilot_per_day,
            "max_fast_copilot_per_day": self.max_fast_copilot_per_day,
            "max_copilot_per_hour": self.max_copilot_per_hour,
            "max_copilot_per_digest": self.max_copilot_per_digest,
            "copilot_theme_ttl_minutes": self.copilot_theme_ttl_minutes,
            "digest_window_minutes": self.digest_window_minutes,
            "max_themes_per_digest": self.max_themes_per_digest,
            "max_alerts_per_digest": self.max_alerts_per_digest,
            "max_markets_per_theme": self.max_markets_per_theme,
            "min_liquidity": self.min_liquidity,
            "min_volume_24h": self.min_volume_24h,
            "min_abs_move": self.min_abs_move,
            "p_min": self.p_min,
            "p_max": self.p_max,
            "allowed_strengths": self.allowed_strengths,
            "fast_signals_enabled": self.fast_signals_enabled,
            "fast_mode": self.fast_mode,
            "fast_window_minutes": self.fast_window_minutes,
            "fast_max_themes_per_digest": self.fast_max_themes_per_digest,
            "fast_max_markets_per_theme": self.fast_max_markets_per_theme,
        }


PLAN_SEEDS = [
    PlanSeed(
        name="basic",
        price_monthly=10.0,
        copilot_enabled=False,
        max_copilot_per_day=0,
        max_fast_copilot_per_day=0,
        max_copilot_per_hour=0,
        max_copilot_per_digest=0,
        copilot_theme_ttl_minutes=360,
        digest_window_minutes=60,
        max_themes_per_digest=3,
        max_alerts_per_digest=3,
        max_markets_per_theme=3,
        min_liquidity=5000.0,
        min_volume_24h=5000.0,
        min_abs_move=0.01,
        p_min=0.15,
        p_max=0.85,
        allowed_strengths="STRONG",
        fast_signals_enabled=False,
        fast_mode="WATCH_ONLY",
        fast_window_minutes=15,
        fast_max_themes_per_digest=2,
        fast_max_markets_per_theme=2,
    ),
    PlanSeed(
        name="pro",
        price_monthly=29.0,
        copilot_enabled=True,
        max_copilot_per_day=30,
        max_fast_copilot_per_day=30,
        max_copilot_per_hour=3,
        max_copilot_per_digest=1,
        copilot_theme_ttl_minutes=360,
        digest_window_minutes=30,
        max_themes_per_digest=5,
        max_alerts_per_digest=7,
        max_markets_per_theme=3,
        min_liquidity=3000.0,
        min_volume_24h=3000.0,
        min_abs_move=0.01,
        p_min=0.15,
        p_max=0.85,
        allowed_strengths="STRONG,MEDIUM",
        fast_signals_enabled=True,
        fast_mode="WATCH_ONLY",
        fast_window_minutes=10,
        fast_max_themes_per_digest=2,
        fast_max_markets_per_theme=2,
    ),
    PlanSeed(
        name="elite",
        price_monthly=99.0,
        copilot_enabled=True,
        max_copilot_per_day=200,
        max_fast_copilot_per_day=200,
        max_copilot_per_hour=12,
        max_copilot_per_digest=1,
        copilot_theme_ttl_minutes=120,
        digest_window_minutes=15,
        max_themes_per_digest=10,
        max_alerts_per_digest=10,
        max_markets_per_theme=3,
        min_liquidity=1000.0,
        min_volume_24h=1000.0,
        min_abs_move=0.01,
        p_min=0.15,
        p_max=0.85,
        allowed_strengths="STRONG,MEDIUM",
        fast_signals_enabled=True,
        fast_mode="FULL",
        fast_window_minutes=5,
        fast_max_themes_per_digest=2,
        fast_max_markets_per_theme=2,
    ),
]


def get_plan_seeds() -> list[PlanSeed]:
    return list(PLAN_SEEDS)


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
        allow_info_alerts=False,
        allow_fast_alerts=False,
        soft_band=(defaults.DEFAULT_SOFT_P_MIN, defaults.DEFAULT_SOFT_P_MAX),
        strict_band=(defaults.DEFAULT_STRICT_P_MIN, defaults.DEFAULT_STRICT_P_MAX),
        allowed_classes=(AlertClass.ACTIONABLE_STANDARD,),
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
