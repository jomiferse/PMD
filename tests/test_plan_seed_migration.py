from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from uuid import uuid4

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db import Base
from app.models import Plan, User


def _load_seed_migration():
    migration_path = (
        Path(__file__).resolve().parents[1]
        / "alembic"
        / "versions"
        / "20260105_baseline.py"
    )
    spec = spec_from_file_location("baseline_migration", migration_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("failed to load seed migration module")
    module = module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_seed_plans_migration_upserts_and_assigns_default():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)

    module = _load_seed_migration()

    SessionLocal = sessionmaker(bind=engine)
    session = SessionLocal()
    try:
        user = User(user_id=uuid4(), name="Seed User", plan_id=None)
        session.add(user)
        session.commit()

        with engine.begin() as connection:
            module._upsert_plans(connection)
            module._upsert_plans(connection)
            module._assign_default_plan(connection)
            module._verify_plans(connection)

        session.refresh(user)
        basic = session.query(Plan).filter(Plan.name == "basic").one()
        assert user.plan_id == basic.id
        assert basic.stripe_price_lookup_key == "STRIPE_BASIC_PRICE_ID"
        assert session.query(Plan).count() == 3
    finally:
        session.close()
