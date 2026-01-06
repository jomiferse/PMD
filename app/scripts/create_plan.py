import argparse

from app.db import SessionLocal
from app.models import Plan


def _normalize_strengths(value: str | None) -> str | None:
    if not value:
        return None
    parts = [part.strip().upper() for part in value.split(",") if part.strip()]
    return ",".join(parts) if parts else None


def upsert_plan(args: argparse.Namespace) -> None:
    db = SessionLocal()
    try:
        plan = db.query(Plan).filter(Plan.name == args.name).one_or_none()
        if plan is None:
            plan = Plan(name=args.name)

        for field in (
            "price_monthly",
            "is_active",
            "copilot_enabled",
            "max_copilot_per_day",
            "max_fast_copilot_per_day",
            "max_copilot_per_digest",
            "copilot_theme_ttl_minutes",
            "fast_signals_enabled",
            "digest_window_minutes",
            "max_themes_per_digest",
            "max_alerts_per_digest",
            "max_markets_per_theme",
            "min_liquidity",
            "min_volume_24h",
            "min_abs_move",
            "p_min",
            "p_max",
            "fast_window_minutes",
            "fast_max_themes_per_digest",
            "fast_max_markets_per_theme",
        ):
            value = getattr(args, field)
            if value is None:
                continue
            setattr(plan, field, value)

        if args.allowed_strengths is not None:
            plan.allowed_strengths = _normalize_strengths(args.allowed_strengths)

        db.add(plan)
        db.commit()
        db.refresh(plan)
        print(f"plan_id={plan.id} name={plan.name}")
    finally:
        db.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Create or update a plan")
    parser.add_argument("--name", required=True)
    parser.add_argument("--price-monthly", type=float)
    parser.add_argument("--is-active", type=lambda v: v.lower() in {"true", "1", "yes", "y"})
    parser.add_argument("--copilot-enabled", type=lambda v: v.lower() in {"true", "1", "yes", "y"})
    parser.add_argument("--max-copilot-per-day", type=int)
    parser.add_argument("--max-fast-copilot-per-day", type=int)
    parser.add_argument("--max-copilot-per-digest", type=int)
    parser.add_argument("--copilot-theme-ttl-minutes", type=int)
    parser.add_argument("--fast-signals-enabled", type=lambda v: v.lower() in {"true", "1", "yes", "y"})
    parser.add_argument("--digest-window-minutes", type=int)
    parser.add_argument("--max-themes-per-digest", type=int)
    parser.add_argument("--max-alerts-per-digest", type=int)
    parser.add_argument("--max-markets-per-theme", type=int)
    parser.add_argument("--min-liquidity", type=float)
    parser.add_argument("--min-volume-24h", type=float)
    parser.add_argument("--min-abs-move", type=float)
    parser.add_argument("--p-min", type=float)
    parser.add_argument("--p-max", type=float)
    parser.add_argument("--allowed-strengths", help="Comma list: STRONG or STRONG,MEDIUM")
    parser.add_argument("--fast-window-minutes", type=int)
    parser.add_argument("--fast-max-themes-per-digest", type=int)
    parser.add_argument("--fast-max-markets-per-theme", type=int)
    parser.set_defaults(func=upsert_plan)
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
