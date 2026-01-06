from uuid import uuid4

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.core.user_settings import get_effective_user_settings
from app.db import Base
from app.models import User, UserPreference
from app.trading.sizing import DraftUnavailable, compute_draft_size


@pytest.fixture()
def db_session():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(bind=engine)
    SessionLocal = sessionmaker(bind=engine)
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()


def test_user_preferences_drive_risk_limits(db_session):
    user = User(user_id=uuid4(), name="Risky", copilot_enabled=True)
    pref = UserPreference(
        user_id=user.user_id,
        risk_budget_usd_per_day=200.0,
        max_usd_per_trade=50.0,
        max_liquidity_fraction=0.1,
    )
    db_session.add_all([user, pref])
    db_session.commit()

    effective = get_effective_user_settings(user, db=db_session)
    result = compute_draft_size(
        risk_budget_usd_per_day=effective.risk_budget_usd_per_day,
        max_usd_per_trade=effective.max_usd_per_trade,
        max_liquidity_fraction=effective.max_liquidity_fraction,
        risk_budget_remaining=effective.risk_budget_usd_per_day,
        liquidity=10000.0,
        price=0.5,
    )
    assert not isinstance(result, DraftUnavailable)
    assert result.notional_usd == pytest.approx(50.0)


def test_missing_preferences_defaults_to_zero_limits(db_session):
    user = User(user_id=uuid4(), name="NoPrefs", copilot_enabled=True)
    db_session.add(user)
    db_session.commit()

    effective = get_effective_user_settings(user, db=db_session)
    result = compute_draft_size(
        risk_budget_usd_per_day=effective.risk_budget_usd_per_day,
        max_usd_per_trade=effective.max_usd_per_trade,
        max_liquidity_fraction=effective.max_liquidity_fraction,
        risk_budget_remaining=effective.risk_budget_usd_per_day,
        liquidity=5000.0,
        price=0.5,
    )
    assert isinstance(result, DraftUnavailable)
    assert "risk_budget_usd_per_day is 0 (or missing)" in result.reasons
    assert "max_usd_per_trade is 0 (or missing)" in result.reasons


def test_max_liquidity_fraction_clamps_notional(db_session):
    user = User(user_id=uuid4(), name="Clamped", copilot_enabled=True)
    pref = UserPreference(
        user_id=user.user_id,
        risk_budget_usd_per_day=1000.0,
        max_usd_per_trade=500.0,
        max_liquidity_fraction=0.01,
    )
    db_session.add_all([user, pref])
    db_session.commit()

    effective = get_effective_user_settings(user, db=db_session)
    result = compute_draft_size(
        risk_budget_usd_per_day=effective.risk_budget_usd_per_day,
        max_usd_per_trade=effective.max_usd_per_trade,
        max_liquidity_fraction=effective.max_liquidity_fraction,
        risk_budget_remaining=effective.risk_budget_usd_per_day,
        liquidity=1000.0,
        price=0.5,
    )
    assert not isinstance(result, DraftUnavailable)
    assert result.notional_usd == pytest.approx(10.0)
    assert result.size_shares == pytest.approx(20.0)
