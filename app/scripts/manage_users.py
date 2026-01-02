import argparse
import uuid
from datetime import datetime, timezone

import httpx

from app.db import SessionLocal
from app.models import User, UserAlertPreference
from app.settings import settings


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


def add_user(args: argparse.Namespace) -> None:
    db = SessionLocal()
    try:
        user = User(
            name=args.name,
            telegram_chat_id=args.chat_id,
            is_active=True,
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
        if args.max_alerts_per_digest is not None:
            pref.max_alerts_per_digest = args.max_alerts_per_digest

        db.commit()
        print(f"updated preferences for {user.user_id}")
    finally:
        db.close()


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
