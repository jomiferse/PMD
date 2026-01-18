"""Seed pricing plans.

Revision ID: 20260118_seed_plans
Revises: 20260118_plan_stripe_lookup
Create Date: 2026-01-18 00:00:00.000000
"""

import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "20260118_seed_plans"
down_revision = "20260118_plan_stripe_lookup"
branch_labels = None
depends_on = None


PLAN_SEEDS = [
    {
        "name": "basic",
        "stripe_price_lookup_key": "STRIPE_BASIC_PRICE_ID",
        "price_monthly": 10.0,
        "is_active": True,
        "copilot_enabled": False,
        "max_copilot_per_day": 0,
        "max_fast_copilot_per_day": 0,
        "max_copilot_per_hour": 0,
        "max_copilot_per_digest": 0,
        "copilot_theme_ttl_minutes": 360,
        "digest_window_minutes": 60,
        "max_themes_per_digest": 3,
        "max_alerts_per_digest": 3,
        "max_markets_per_theme": 3,
        "min_liquidity": 5000.0,
        "min_volume_24h": 5000.0,
        "min_abs_move": 0.01,
        "p_min": 0.15,
        "p_max": 0.85,
        "allowed_strengths": "STRONG",
        "fast_signals_enabled": False,
        "fast_mode": "WATCH_ONLY",
        "fast_window_minutes": 15,
        "fast_max_themes_per_digest": 2,
        "fast_max_markets_per_theme": 2,
    },
    {
        "name": "pro",
        "stripe_price_lookup_key": "STRIPE_PRO_PRICE_ID",
        "price_monthly": 29.0,
        "is_active": True,
        "copilot_enabled": True,
        "max_copilot_per_day": 30,
        "max_fast_copilot_per_day": 30,
        "max_copilot_per_hour": 3,
        "max_copilot_per_digest": 1,
        "copilot_theme_ttl_minutes": 360,
        "digest_window_minutes": 30,
        "max_themes_per_digest": 5,
        "max_alerts_per_digest": 7,
        "max_markets_per_theme": 3,
        "min_liquidity": 3000.0,
        "min_volume_24h": 3000.0,
        "min_abs_move": 0.01,
        "p_min": 0.15,
        "p_max": 0.85,
        "allowed_strengths": "STRONG,MEDIUM",
        "fast_signals_enabled": True,
        "fast_mode": "WATCH_ONLY",
        "fast_window_minutes": 10,
        "fast_max_themes_per_digest": 2,
        "fast_max_markets_per_theme": 2,
    },
    {
        "name": "elite",
        "stripe_price_lookup_key": "STRIPE_ELITE_PRICE_ID",
        "price_monthly": 99.0,
        "is_active": True,
        "copilot_enabled": True,
        "max_copilot_per_day": 200,
        "max_fast_copilot_per_day": 200,
        "max_copilot_per_hour": 12,
        "max_copilot_per_digest": 1,
        "copilot_theme_ttl_minutes": 120,
        "digest_window_minutes": 15,
        "max_themes_per_digest": 10,
        "max_alerts_per_digest": 10,
        "max_markets_per_theme": 3,
        "min_liquidity": 1000.0,
        "min_volume_24h": 1000.0,
        "min_abs_move": 0.01,
        "p_min": 0.15,
        "p_max": 0.85,
        "allowed_strengths": "STRONG,MEDIUM",
        "fast_signals_enabled": True,
        "fast_mode": "FULL",
        "fast_window_minutes": 5,
        "fast_max_themes_per_digest": 2,
        "fast_max_markets_per_theme": 2,
    },
]


UPSERT_SQL = sa.text(
    """
    INSERT INTO plans (
        name,
        stripe_price_lookup_key,
        price_monthly,
        is_active,
        copilot_enabled,
        max_copilot_per_day,
        max_fast_copilot_per_day,
        max_copilot_per_hour,
        max_copilot_per_digest,
        copilot_theme_ttl_minutes,
        digest_window_minutes,
        max_themes_per_digest,
        max_alerts_per_digest,
        max_markets_per_theme,
        min_liquidity,
        min_volume_24h,
        min_abs_move,
        p_min,
        p_max,
        allowed_strengths,
        fast_signals_enabled,
        fast_mode,
        fast_window_minutes,
        fast_max_themes_per_digest,
        fast_max_markets_per_theme,
        created_at
    )
    VALUES (
        :name,
        :stripe_price_lookup_key,
        :price_monthly,
        :is_active,
        :copilot_enabled,
        :max_copilot_per_day,
        :max_fast_copilot_per_day,
        :max_copilot_per_hour,
        :max_copilot_per_digest,
        :copilot_theme_ttl_minutes,
        :digest_window_minutes,
        :max_themes_per_digest,
        :max_alerts_per_digest,
        :max_markets_per_theme,
        :min_liquidity,
        :min_volume_24h,
        :min_abs_move,
        :p_min,
        :p_max,
        :allowed_strengths,
        :fast_signals_enabled,
        :fast_mode,
        :fast_window_minutes,
        :fast_max_themes_per_digest,
        :fast_max_markets_per_theme,
        CURRENT_TIMESTAMP
    )
    ON CONFLICT (name) DO UPDATE SET
        stripe_price_lookup_key = EXCLUDED.stripe_price_lookup_key,
        price_monthly = EXCLUDED.price_monthly,
        is_active = EXCLUDED.is_active,
        copilot_enabled = EXCLUDED.copilot_enabled,
        max_copilot_per_day = EXCLUDED.max_copilot_per_day,
        max_fast_copilot_per_day = EXCLUDED.max_fast_copilot_per_day,
        max_copilot_per_hour = EXCLUDED.max_copilot_per_hour,
        max_copilot_per_digest = EXCLUDED.max_copilot_per_digest,
        copilot_theme_ttl_minutes = EXCLUDED.copilot_theme_ttl_minutes,
        digest_window_minutes = EXCLUDED.digest_window_minutes,
        max_themes_per_digest = EXCLUDED.max_themes_per_digest,
        max_alerts_per_digest = EXCLUDED.max_alerts_per_digest,
        max_markets_per_theme = EXCLUDED.max_markets_per_theme,
        min_liquidity = EXCLUDED.min_liquidity,
        min_volume_24h = EXCLUDED.min_volume_24h,
        min_abs_move = EXCLUDED.min_abs_move,
        p_min = EXCLUDED.p_min,
        p_max = EXCLUDED.p_max,
        allowed_strengths = EXCLUDED.allowed_strengths,
        fast_signals_enabled = EXCLUDED.fast_signals_enabled,
        fast_mode = EXCLUDED.fast_mode,
        fast_window_minutes = EXCLUDED.fast_window_minutes,
        fast_max_themes_per_digest = EXCLUDED.fast_max_themes_per_digest,
        fast_max_markets_per_theme = EXCLUDED.fast_max_markets_per_theme
    """
)


def _upsert_plans(connection: sa.Connection) -> None:
    for seed in PLAN_SEEDS:
        connection.execute(UPSERT_SQL, seed)


def _assign_default_plan(connection: sa.Connection) -> None:
    result = connection.execute(
        sa.text("SELECT id FROM plans WHERE name = :name"),
        {"name": "basic"},
    ).scalar()
    if result is None:
        raise RuntimeError("missing basic plan after seeding")
    connection.execute(
        sa.text(
            "UPDATE users SET plan_id = :plan_id WHERE plan_id IS NULL"
        ),
        {"plan_id": result},
    )


def _verify_plans(connection: sa.Connection) -> None:
    required = {"basic", "pro", "elite"}
    rows = connection.execute(
        sa.text(
            """
            SELECT name
            FROM plans
            WHERE name IN ('basic', 'pro', 'elite')
            """
        )
    ).fetchall()
    found = {row[0] for row in rows}
    if found != required:
        missing = sorted(required - found)
        raise RuntimeError(f"plan seed verification failed: missing {missing}")

    missing_fields = connection.execute(
        sa.text(
            """
            SELECT name
            FROM plans
            WHERE name IN ('basic', 'pro', 'elite')
              AND (
                stripe_price_lookup_key IS NULL
                OR digest_window_minutes IS NULL
                OR max_themes_per_digest IS NULL
                OR max_alerts_per_digest IS NULL
                OR min_liquidity IS NULL
                OR min_volume_24h IS NULL
                OR min_abs_move IS NULL
                OR p_min IS NULL
                OR p_max IS NULL
                OR allowed_strengths IS NULL
                OR fast_mode IS NULL
              )
            """
        )
    ).fetchall()
    if missing_fields:
        names = sorted({row[0] for row in missing_fields})
        raise RuntimeError(f"plan seed verification failed: missing fields for {names}")


def upgrade() -> None:
    from alembic import op

    connection = op.get_bind()
    _upsert_plans(connection)
    _assign_default_plan(connection)
    _verify_plans(connection)


def downgrade() -> None:
    from alembic import op  # noqa: F401

    # Leave seeded plans in place to avoid deleting production data.
    pass
