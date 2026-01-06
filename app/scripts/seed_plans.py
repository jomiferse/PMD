import argparse
from datetime import datetime, timezone

from app.core.plans import DEFAULT_PLAN_NAME, get_plan_seeds
from app.db import SessionLocal
from app.models import Plan, User


def seed_plans() -> None:
    db = SessionLocal()
    try:
        now_ts = datetime.now(timezone.utc)
        for seed in get_plan_seeds():
            plan = db.query(Plan).filter(Plan.name == seed.name).one_or_none()
            if plan is None:
                plan = Plan(name=seed.name, created_at=now_ts)
            for key, value in seed.as_dict().items():
                setattr(plan, key, value)
            if plan.created_at is None:
                plan.created_at = now_ts
            db.add(plan)

        db.commit()

        default_plan = db.query(Plan).filter(Plan.name == DEFAULT_PLAN_NAME).one_or_none()
        if default_plan is None:
            raise SystemExit(f"missing default plan: {DEFAULT_PLAN_NAME}")
        db.query(User).update({User.plan_id: default_plan.id})
        db.commit()
        print("seeded plans and assigned users to basic")
    finally:
        db.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Seed PMD pricing plans")
    parser.parse_args()
    seed_plans()


if __name__ == "__main__":
    main()
