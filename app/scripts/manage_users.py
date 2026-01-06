import argparse
import json
import uuid
from datetime import datetime, timezone

import httpx

from app.db import SessionLocal
from app.models import Plan, User, UserAlertPreference
from app.settings import settings
from app.core.effective_settings import invalidate_effective_settings_cache
from app.core.plans import DEFAULT_PLAN_NAME


def _resolve_user(db, identifier: str) -> User:
    try:
        user_id = uuid.UUID(identifier)
        user = db.query(User).filter(User.user_id == user_id).one_or_none()
    except ValueError:
        user = db.query(User).filter(User.name == identifier).one_or_none()
    if not user:
        raise SystemExit(f"User not found: {identifier}")
    return user


def _normalize_alert_strengths(value: str | None) -> str | None:
    if not value:
        return None
    parts = [part.strip().upper() for part in value.split(",") if part.strip()]
    return ",".join(parts) if parts else None


def _parse_bool(value: str | None) -> bool | None:
    if value is None:
        return None
    normalized = value.strip().lower()
    if normalized in {"true", "1", "yes", "y"}:
        return True
    if normalized in {"false", "0", "no", "n"}:
        return False
    raise SystemExit(f"invalid boolean value: {value}")


def add_user(args: argparse.Namespace) -> None:
    db = SessionLocal()
    try:
        default_plan = db.query(Plan).filter(Plan.name == DEFAULT_PLAN_NAME).one_or_none()
        user = User(
            name=args.name,
            telegram_chat_id=args.chat_id,
            is_active=True,
            plan_id=default_plan.id if default_plan else None,
            created_at=datetime.now(timezone.utc),
        )
        db.add(user)
        db.commit()
        db.refresh(user)
        print(str(user.user_id))
    finally:
        db.close()


def disable_user(args: argparse.Namespace) -> None:
    db = SessionLocal()
    try:
        user = _resolve_user(db, args.user)
        user.is_active = False
        db.commit()
        print(f"disabled {user.user_id}")
    finally:
        db.close()


def set_preferences(args: argparse.Namespace) -> None:
    db = SessionLocal()
    try:
        user = _resolve_user(db, args.user)
        pref = db.query(UserAlertPreference).filter(UserAlertPreference.user_id == user.user_id).one_or_none()
        if not pref:
            pref = UserAlertPreference(user_id=user.user_id, created_at=datetime.now(timezone.utc))
            db.add(pref)
        overrides = _load_overrides(user)
        overrides_updated = False

        if args.min_liquidity is not None:
            pref.min_liquidity = args.min_liquidity
        if args.min_volume_24h is not None:
            pref.min_volume_24h = args.min_volume_24h
        if args.min_abs_price_move is not None:
            pref.min_abs_price_move = args.min_abs_price_move
        if args.alert_strengths is not None:
            pref.alert_strengths = _normalize_alert_strengths(args.alert_strengths)
        if args.digest_window_minutes is not None:
            pref.digest_window_minutes = args.digest_window_minutes
            overrides["digest_window_minutes"] = args.digest_window_minutes
            overrides_updated = True
        if args.max_alerts_per_digest is not None:
            pref.max_alerts_per_digest = args.max_alerts_per_digest
        if args.max_themes_per_digest is not None:
            pref.max_themes_per_digest = args.max_themes_per_digest
        if args.max_markets_per_theme is not None:
            pref.max_markets_per_theme = args.max_markets_per_theme
        if args.p_min is not None:
            pref.p_min = args.p_min
        if args.p_max is not None:
            pref.p_max = args.p_max
        if args.ai_copilot_enabled is not None:
            user.copilot_enabled = args.ai_copilot_enabled
        if args.fast_signals_enabled is not None:
            pref.fast_signals_enabled = args.fast_signals_enabled
            overrides["fast_signals_enabled"] = args.fast_signals_enabled
            overrides_updated = True
        if args.fast_window_minutes is not None:
            pref.fast_window_minutes = args.fast_window_minutes
        if args.fast_max_themes_per_digest is not None:
            pref.fast_max_themes_per_digest = args.fast_max_themes_per_digest
        if args.fast_max_markets_per_theme is not None:
            pref.fast_max_markets_per_theme = args.fast_max_markets_per_theme

        if overrides_updated:
            user.overrides_json = overrides
        db.commit()
        invalidate_effective_settings_cache(user.user_id)
        print(f"updated preferences for {user.user_id}")
    finally:
        db.close()


def _load_overrides(user: User) -> dict:
    raw = user.overrides_json
    if isinstance(raw, dict):
        return dict(raw)
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            return {}
        if isinstance(parsed, dict):
            return dict(parsed)
    return {}


def test_delivery(args: argparse.Namespace) -> None:
    if not settings.TELEGRAM_BOT_TOKEN:
        raise SystemExit("TELEGRAM_BOT_TOKEN is not configured")

    db = SessionLocal()
    try:
        user = _resolve_user(db, args.user)
        if not user.telegram_chat_id:
            raise SystemExit("User does not have a telegram_chat_id")
        message = f"PMD test delivery for {user.name} at {datetime.now(timezone.utc).isoformat()}"
        url = f"https://api.telegram.org/bot{settings.TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {
            "chat_id": user.telegram_chat_id,
            "text": message,
            "disable_web_page_preview": True,
        }
        with httpx.Client(timeout=10) as client:
            response = client.post(url, json=payload)
        if response.is_success:
            print("ok")
        else:
            raise SystemExit(f"telegram failed: {response.status_code} {response.text[:200]}")
    finally:
        db.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Manage PMD users")
    subparsers = parser.add_subparsers(dest="command", required=True)

    add = subparsers.add_parser("add", help="Create a new user")
    add.add_argument("--name", required=True)
    add.add_argument("--chat-id", dest="chat_id")
    add.set_defaults(func=add_user)

    disable = subparsers.add_parser("disable", help="Disable a user")
    disable.add_argument("--user", required=True, help="User ID or name")
    disable.set_defaults(func=disable_user)

    pref = subparsers.add_parser("set-pref", help="Update user alert preferences")
    pref.add_argument("--user", required=True, help="User ID or name")
    pref.add_argument("--min-liquidity", type=float)
    pref.add_argument("--min-volume-24h", type=float)
    pref.add_argument("--min-abs-price-move", type=float)
    pref.add_argument("--alert-strengths", help="Comma list: STRONG or STRONG,MEDIUM")
    pref.add_argument("--digest-window-minutes", type=int)
    pref.add_argument("--max-alerts-per-digest", type=int)
    pref.add_argument("--max-themes-per-digest", type=int)
    pref.add_argument("--max-markets-per-theme", type=int)
    pref.add_argument("--p-min", type=float)
    pref.add_argument("--p-max", type=float)
    pref.add_argument("--ai-copilot-enabled", type=_parse_bool)
    pref.add_argument("--fast-signals-enabled", type=_parse_bool)
    pref.add_argument("--fast-window-minutes", type=int)
    pref.add_argument("--fast-max-themes-per-digest", type=int)
    pref.add_argument("--fast-max-markets-per-theme", type=int)
    pref.set_defaults(func=set_preferences)

    test = subparsers.add_parser("test", help="Send a test Telegram message to a user")
    test.add_argument("--user", required=True, help="User ID or name")
    test.set_defaults(func=test_delivery)

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
