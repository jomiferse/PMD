import argparse
import uuid

from app.db import SessionLocal
from app.models import Plan, User
from app.core.effective_settings import invalidate_effective_settings_cache


def _resolve_user(db, identifier: str) -> User:
    try:
        user_id = uuid.UUID(identifier)
        user = db.query(User).filter(User.user_id == user_id).one_or_none()
    except ValueError:
        user = db.query(User).filter(User.name == identifier).one_or_none()
    if not user:
        raise SystemExit(f"User not found: {identifier}")
    return user


def assign_plan(args: argparse.Namespace) -> None:
    db = SessionLocal()
    try:
        user = _resolve_user(db, args.user)
        plan = None
        if args.plan_id is not None:
            plan = db.query(Plan).filter(Plan.id == args.plan_id).one_or_none()
        if plan is None and args.plan_name:
            plan = db.query(Plan).filter(Plan.name == args.plan_name).one_or_none()
        if not plan:
            raise SystemExit("Plan not found")

        user.plan_id = plan.id
        db.commit()
        invalidate_effective_settings_cache(user.user_id)
        print(f"assigned plan {plan.name} to {user.user_id}")
    finally:
        db.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Assign a plan to a user")
    parser.add_argument("--user", required=True, help="User ID or name")
    parser.add_argument("--plan-id", type=int)
    parser.add_argument("--plan-name")
    parser.set_defaults(func=assign_plan)
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
