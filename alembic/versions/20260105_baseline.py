
"""Baseline schema.

Revision ID: 20260105_baseline
Revises:
Create Date: 2026-01-05 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision = "20260105_baseline"
down_revision = None
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


def _seed_plans(connection: sa.Connection) -> None:
    _upsert_plans(connection)
    _assign_default_plan(connection)
    _verify_plans(connection)


def upgrade() -> None:
    op.create_table(
        "plans",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("name", sa.String(length=64), nullable=False),
        sa.Column("stripe_price_lookup_key", sa.String(length=64), nullable=True),
        sa.Column("price_monthly", sa.Float(), nullable=True),
        sa.Column("is_active", sa.Boolean(), nullable=False, default=True),
        sa.Column("copilot_enabled", sa.Boolean(), nullable=True),
        sa.Column("max_copilot_per_day", sa.Integer(), nullable=True),
        sa.Column("max_fast_copilot_per_day", sa.Integer(), nullable=True),
        sa.Column("max_copilot_per_hour", sa.Integer(), nullable=True),
        sa.Column("max_copilot_per_digest", sa.Integer(), nullable=True),
        sa.Column("copilot_theme_ttl_minutes", sa.Integer(), nullable=True),
        sa.Column("fast_signals_enabled", sa.Boolean(), nullable=True),
        sa.Column("digest_window_minutes", sa.Integer(), nullable=True),
        sa.Column("max_themes_per_digest", sa.Integer(), nullable=True),
        sa.Column("max_alerts_per_digest", sa.Integer(), nullable=True),
        sa.Column("max_markets_per_theme", sa.Integer(), nullable=True),
        sa.Column("min_liquidity", sa.Float(), nullable=True),
        sa.Column("min_volume_24h", sa.Float(), nullable=True),
        sa.Column("min_abs_move", sa.Float(), nullable=True),
        sa.Column("p_min", sa.Float(), nullable=True),
        sa.Column("p_max", sa.Float(), nullable=True),
        sa.Column("allowed_strengths", sa.String(length=64), nullable=True),
        sa.Column("fast_mode", sa.String(length=16), nullable=True),
        sa.Column("fast_window_minutes", sa.Integer(), nullable=True),
        sa.Column("fast_max_themes_per_digest", sa.Integer(), nullable=True),
        sa.Column("fast_max_markets_per_theme", sa.Integer(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.UniqueConstraint("name", name="uq_plans_name"),
        sa.CheckConstraint(
            "price_monthly IS NULL OR price_monthly >= 0",
            name="ck_plans_price_monthly_non_negative",
        ),
        sa.CheckConstraint(
            "max_copilot_per_day IS NULL OR max_copilot_per_day >= 0",
            name="ck_plans_max_copilot_per_day_non_negative",
        ),
        sa.CheckConstraint(
            "max_fast_copilot_per_day IS NULL OR max_fast_copilot_per_day >= 0",
            name="ck_plans_max_fast_copilot_per_day_non_negative",
        ),
        sa.CheckConstraint(
            "max_copilot_per_hour IS NULL OR max_copilot_per_hour >= 0",
            name="ck_plans_max_copilot_per_hour_non_negative",
        ),
        sa.CheckConstraint(
            "max_copilot_per_digest IS NULL OR max_copilot_per_digest >= 0",
            name="ck_plans_max_copilot_per_digest_non_negative",
        ),
        sa.CheckConstraint(
            "copilot_theme_ttl_minutes IS NULL OR copilot_theme_ttl_minutes > 0",
            name="ck_plans_copilot_theme_ttl_minutes_positive",
        ),
        sa.CheckConstraint(
            "digest_window_minutes IS NULL OR digest_window_minutes > 0",
            name="ck_plans_digest_window_minutes_positive",
        ),
        sa.CheckConstraint(
            "max_themes_per_digest IS NULL OR max_themes_per_digest >= 0",
            name="ck_plans_max_themes_per_digest_non_negative",
        ),
        sa.CheckConstraint(
            "max_alerts_per_digest IS NULL OR max_alerts_per_digest >= 0",
            name="ck_plans_max_alerts_per_digest_non_negative",
        ),
        sa.CheckConstraint(
            "max_markets_per_theme IS NULL OR max_markets_per_theme >= 0",
            name="ck_plans_max_markets_per_theme_non_negative",
        ),
        sa.CheckConstraint(
            "min_liquidity IS NULL OR min_liquidity >= 0",
            name="ck_plans_min_liquidity_non_negative",
        ),
        sa.CheckConstraint(
            "min_volume_24h IS NULL OR min_volume_24h >= 0",
            name="ck_plans_min_volume_24h_non_negative",
        ),
        sa.CheckConstraint(
            "min_abs_move IS NULL OR min_abs_move >= 0",
            name="ck_plans_min_abs_move_non_negative",
        ),
        sa.CheckConstraint(
            "p_min IS NULL OR (p_min >= 0 AND p_min <= 1)",
            name="ck_plans_p_min_range",
        ),
        sa.CheckConstraint(
            "p_max IS NULL OR (p_max >= 0 AND p_max <= 1)",
            name="ck_plans_p_max_range",
        ),
        sa.CheckConstraint(
            "p_min IS NULL OR p_max IS NULL OR p_min < p_max",
            name="ck_plans_p_range_order",
        ),
        sa.CheckConstraint(
            "fast_window_minutes IS NULL OR fast_window_minutes > 0",
            name="ck_plans_fast_window_minutes_positive",
        ),
        sa.CheckConstraint(
            "fast_max_themes_per_digest IS NULL OR fast_max_themes_per_digest >= 0",
            name="ck_plans_fast_max_themes_per_digest_non_negative",
        ),
        sa.CheckConstraint(
            "fast_max_markets_per_theme IS NULL OR fast_max_markets_per_theme >= 0",
            name="ck_plans_fast_max_markets_per_theme_non_negative",
        ),
    )

    op.create_table(
        "users",
        sa.Column("user_id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("name", sa.String(length=128), nullable=False),
        sa.Column("telegram_chat_id", sa.BigInteger(), nullable=True),
        sa.Column("is_active", sa.Boolean(), nullable=False, default=True),
        sa.Column("plan_id", sa.Integer(), sa.ForeignKey("plans.id"), nullable=True),
        sa.Column("copilot_enabled", sa.Boolean(), nullable=False, default=False),
        sa.Column(
            "overrides_json",
            sa.JSON().with_variant(postgresql.JSONB, "postgresql"),
            nullable=True,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.UniqueConstraint("telegram_chat_id", name="uq_users_telegram_chat_id"),
    )

    op.create_table(
        "pending_telegram_chats",
        sa.Column("telegram_chat_id", sa.BigInteger(), primary_key=True),
        sa.Column("first_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False, server_default=sa.text("'pending'")),
    )

    op.create_table(
        "user_auth",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column(
            "user_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("users.user_id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("email", sa.String(length=320), nullable=False),
        sa.Column("password_hash", sa.String(length=256), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.UniqueConstraint("email", name="uq_user_auth_email"),
    )
    op.create_index("ix_user_auth_email", "user_auth", ["email"])

    op.create_table(
        "user_sessions",
        sa.Column("token", sa.String(length=64), primary_key=True),
        sa.Column(
            "user_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("users.user_id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("revoked_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "expires_at > created_at",
            name="ck_user_sessions_expires_after_created",
        ),
    )
    op.create_index("ix_user_sessions_user_id", "user_sessions", ["user_id"])
    op.create_index("ix_user_sessions_expires_at", "user_sessions", ["expires_at"])

    op.create_table(
        "subscriptions",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column(
            "user_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("users.user_id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("plan_id", sa.Integer(), sa.ForeignKey("plans.id"), nullable=True),
        sa.Column("status", sa.String(length=32), nullable=False, server_default=sa.text("'incomplete'")),
        sa.Column("current_period_end", sa.DateTime(timezone=True), nullable=True),
        sa.Column("stripe_customer_id", sa.String(length=128), nullable=True),
        sa.Column("stripe_subscription_id", sa.String(length=128), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.UniqueConstraint(
            "stripe_subscription_id",
            name="uq_subscriptions_stripe_subscription_id",
        ),
    )
    op.create_index(
        "ix_subscriptions_user_created_desc",
        "subscriptions",
        ["user_id", sa.text("created_at DESC")],
    )
    op.create_index("ix_subscriptions_customer_id", "subscriptions", ["stripe_customer_id"])

    op.create_table(
        "stripe_events",
        sa.Column("event_id", sa.String(length=128), primary_key=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
    )

    op.create_table(
        "user_alert_preferences",
        sa.Column(
            "user_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("users.user_id", ondelete="CASCADE"),
            primary_key=True,
        ),
        sa.Column("min_liquidity", sa.Float(), nullable=True),
        sa.Column("min_volume_24h", sa.Float(), nullable=True),
        sa.Column("min_abs_price_move", sa.Float(), nullable=True),
        sa.Column("alert_strengths", sa.String(length=32), nullable=True),
        sa.Column("digest_window_minutes", sa.Integer(), nullable=True),
        sa.Column("max_alerts_per_digest", sa.Integer(), nullable=True),
        sa.Column("max_themes_per_digest", sa.Integer(), nullable=True),
        sa.Column("max_markets_per_theme", sa.Integer(), nullable=True),
        sa.Column("p_min", sa.Float(), nullable=True),
        sa.Column("p_max", sa.Float(), nullable=True),
        sa.Column("fast_signals_enabled", sa.Boolean(), nullable=True),
        sa.Column("fast_window_minutes", sa.Integer(), nullable=True),
        sa.Column("fast_max_themes_per_digest", sa.Integer(), nullable=True),
        sa.Column("fast_max_markets_per_theme", sa.Integer(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.CheckConstraint(
            "min_liquidity IS NULL OR min_liquidity >= 0",
            name="ck_user_alert_preferences_min_liquidity_non_negative",
        ),
        sa.CheckConstraint(
            "min_volume_24h IS NULL OR min_volume_24h >= 0",
            name="ck_user_alert_preferences_min_volume_24h_non_negative",
        ),
        sa.CheckConstraint(
            "min_abs_price_move IS NULL OR min_abs_price_move >= 0",
            name="ck_user_alert_preferences_min_abs_price_move_non_negative",
        ),
        sa.CheckConstraint(
            "digest_window_minutes IS NULL OR digest_window_minutes > 0",
            name="ck_user_alert_preferences_digest_window_minutes_positive",
        ),
        sa.CheckConstraint(
            "max_alerts_per_digest IS NULL OR max_alerts_per_digest >= 0",
            name="ck_user_alert_preferences_max_alerts_per_digest_non_negative",
        ),
        sa.CheckConstraint(
            "max_themes_per_digest IS NULL OR max_themes_per_digest >= 0",
            name="ck_user_alert_preferences_max_themes_per_digest_non_negative",
        ),
        sa.CheckConstraint(
            "max_markets_per_theme IS NULL OR max_markets_per_theme >= 0",
            name="ck_user_alert_preferences_max_markets_per_theme_non_negative",
        ),
        sa.CheckConstraint(
            "p_min IS NULL OR (p_min >= 0 AND p_min <= 1)",
            name="ck_user_alert_preferences_p_min_range",
        ),
        sa.CheckConstraint(
            "p_max IS NULL OR (p_max >= 0 AND p_max <= 1)",
            name="ck_user_alert_preferences_p_max_range",
        ),
        sa.CheckConstraint(
            "p_min IS NULL OR p_max IS NULL OR p_min < p_max",
            name="ck_user_alert_preferences_p_range_order",
        ),
        sa.CheckConstraint(
            "fast_window_minutes IS NULL OR fast_window_minutes > 0",
            name="ck_user_alert_preferences_fast_window_minutes_positive",
        ),
        sa.CheckConstraint(
            "fast_max_themes_per_digest IS NULL OR fast_max_themes_per_digest >= 0",
            name="ck_uap_fast_max_themes_nonneg",
        ),
        sa.CheckConstraint(
            "fast_max_markets_per_theme IS NULL OR fast_max_markets_per_theme >= 0",
            name="ck_uap_fast_max_markets_nonneg",
        ),
    )

    op.create_table(
        "market_snapshots",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("market_id", sa.String(length=128), nullable=False),
        sa.Column("title", sa.String(length=512), nullable=False),
        sa.Column("category", sa.String(length=128), nullable=False, default="unknown"),
        sa.Column("slug", sa.Text(), nullable=True),
        sa.Column("market_p_yes", sa.Float(), nullable=False),
        sa.Column("market_p_no", sa.Float(), nullable=True),
        sa.Column(
            "market_p_no_derived",
            sa.Boolean(),
            nullable=True,
            server_default=sa.text("true"),
        ),
        sa.Column("primary_outcome_label", sa.String(length=64), nullable=True),
        sa.Column("is_yesno", sa.Boolean(), nullable=True),
        sa.Column("mapping_confidence", sa.String(length=16), nullable=True),
        sa.Column("market_kind", sa.String(length=16), nullable=True),
        sa.Column("liquidity", sa.Float(), nullable=False, default=0.0),
        sa.Column("volume_24h", sa.Float(), nullable=False, default=0.0),
        sa.Column("volume_1w", sa.Float(), nullable=False, default=0.0),
        sa.Column("best_ask", sa.Float(), nullable=False, default=0.0),
        sa.Column("last_trade_price", sa.Float(), nullable=False, default=0.0),
        sa.Column("model_p_yes", sa.Float(), nullable=False),
        sa.Column("edge", sa.Float(), nullable=False),
        sa.Column("source_ts", sa.DateTime(timezone=True), nullable=True),
        sa.Column("snapshot_bucket", sa.DateTime(timezone=True), nullable=False),
        sa.Column("asof_ts", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.UniqueConstraint("market_id", "snapshot_bucket", name="uq_market_bucket"),
        sa.CheckConstraint(
            "market_p_yes >= 0 AND market_p_yes <= 1",
            name="ck_market_snapshots_market_p_yes",
        ),
        sa.CheckConstraint(
            "market_p_no IS NULL OR (market_p_no >= 0 AND market_p_no <= 1)",
            name="ck_market_snapshots_market_p_no",
        ),
        sa.CheckConstraint(
            "model_p_yes >= 0 AND model_p_yes <= 1",
            name="ck_market_snapshots_model_p_yes",
        ),
        sa.CheckConstraint("liquidity >= 0", name="ck_market_snapshots_liquidity_non_negative"),
        sa.CheckConstraint("volume_24h >= 0", name="ck_market_snapshots_volume_24h_non_negative"),
        sa.CheckConstraint("volume_1w >= 0", name="ck_market_snapshots_volume_1w_non_negative"),
        sa.CheckConstraint("best_ask >= 0", name="ck_market_snapshots_best_ask_non_negative"),
        sa.CheckConstraint(
            "last_trade_price >= 0",
            name="ck_market_snapshots_last_trade_price_non_negative",
        ),
        sa.CheckConstraint(
            "market_kind IS NULL OR market_kind IN ('yesno', 'ou', 'multi')",
            name="ck_market_snapshots_market_kind",
        ),
        sa.CheckConstraint(
            "mapping_confidence IS NULL OR mapping_confidence IN ('verified', 'unknown')",
            name="ck_market_snapshots_mapping_confidence",
        ),
    )
    op.create_index("ix_market_snapshots_bucket", "market_snapshots", ["snapshot_bucket"])
    op.create_index(
        "ix_market_snapshots_market_asof",
        "market_snapshots",
        ["market_id", "asof_ts"],
    )
    op.create_index(
        "ix_market_snapshots_asof_desc",
        "market_snapshots",
        [sa.text("asof_ts DESC")],
    )
    op.create_index("ix_market_snapshots_expires_at", "market_snapshots", ["expires_at"])

    op.create_table(
        "alerts",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("tenant_id", sa.String(length=64), nullable=False),
        sa.Column("alert_type", sa.String(length=32), nullable=False),
        sa.Column("market_id", sa.String(length=128), nullable=False),
        sa.Column("title", sa.String(length=512), nullable=False),
        sa.Column("category", sa.String(length=128), nullable=False, default="unknown"),
        sa.Column("move", sa.Float(), nullable=False, default=0.0),
        sa.Column("market_p_yes", sa.Float(), nullable=False, default=0.0),
        sa.Column("prev_market_p_yes", sa.Float(), nullable=False, default=0.0),
        sa.Column("primary_outcome_label", sa.String(length=64), nullable=True),
        sa.Column("is_yesno", sa.Boolean(), nullable=True),
        sa.Column("mapping_confidence", sa.String(length=16), nullable=True),
        sa.Column("market_kind", sa.String(length=16), nullable=True),
        sa.Column("old_price", sa.Float(), nullable=False, default=0.0),
        sa.Column("new_price", sa.Float(), nullable=False, default=0.0),
        sa.Column("delta_pct", sa.Float(), nullable=False, default=0.0),
        sa.Column("liquidity", sa.Float(), nullable=False, default=0.0),
        sa.Column("volume_24h", sa.Float(), nullable=False, default=0.0),
        sa.Column("best_ask", sa.Float(), nullable=False, default=0.0),
        sa.Column("strength", sa.String(length=16), nullable=False, default="MEDIUM"),
        sa.Column("snapshot_bucket", sa.DateTime(timezone=True), nullable=False),
        sa.Column("source_ts", sa.DateTime(timezone=True), nullable=True),
        sa.Column("message", sa.String(length=1024), nullable=False, default=""),
        sa.Column(
            "triggered_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.UniqueConstraint(
            "alert_type",
            "market_id",
            "snapshot_bucket",
            name="uq_alert_market_bucket",
        ),
        sa.CheckConstraint(
            "market_p_yes >= 0 AND market_p_yes <= 1",
            name="ck_alerts_market_p_yes",
        ),
        sa.CheckConstraint(
            "prev_market_p_yes >= 0 AND prev_market_p_yes <= 1",
            name="ck_alerts_prev_market_p_yes",
        ),
        sa.CheckConstraint(
            "old_price >= 0 AND old_price <= 1",
            name="ck_alerts_old_price",
        ),
        sa.CheckConstraint(
            "new_price >= 0 AND new_price <= 1",
            name="ck_alerts_new_price",
        ),
        sa.CheckConstraint("liquidity >= 0", name="ck_alerts_liquidity_non_negative"),
        sa.CheckConstraint("volume_24h >= 0", name="ck_alerts_volume_24h_non_negative"),
        sa.CheckConstraint("best_ask >= 0", name="ck_alerts_best_ask_non_negative"),
        sa.CheckConstraint(
            "strength IN ('LOW', 'MEDIUM', 'HIGH', 'STRONG')",
            name="ck_alerts_strength",
        ),
        sa.CheckConstraint(
            "market_kind IS NULL OR market_kind IN ('yesno', 'ou', 'multi')",
            name="ck_alerts_market_kind",
        ),
        sa.CheckConstraint(
            "mapping_confidence IS NULL OR mapping_confidence IN ('verified', 'unknown')",
            name="ck_alerts_mapping_confidence",
        ),
    )
    op.create_index("ix_alerts_created_at", "alerts", ["created_at"])
    op.create_index("ix_alerts_expires_at", "alerts", ["expires_at"])
    op.create_index("ix_alerts_tenant_type", "alerts", ["tenant_id", "alert_type"])
    op.create_index(
        "ix_alerts_tenant_created_desc",
        "alerts",
        ["tenant_id", sa.text("created_at DESC"), sa.text("id DESC")],
    )
    op.create_index(
        "ix_alerts_tenant_strength_created_desc",
        "alerts",
        ["tenant_id", "strength", sa.text("created_at DESC"), sa.text("id DESC")],
    )
    op.create_index(
        "ix_alerts_tenant_category_norm_created_desc",
        "alerts",
        ["tenant_id", sa.text("lower(category)"), sa.text("created_at DESC"), sa.text("id DESC")],
    )
    op.create_index(
        "ix_alerts_cooldown",
        "alerts",
        ["tenant_id", "alert_type", "market_id", "triggered_at"],
    )
    op.create_index("ix_alerts_market_triggered", "alerts", ["market_id", "triggered_at"])

    op.create_table(
        "alert_deliveries",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column(
            "alert_id",
            sa.Integer(),
            sa.ForeignKey("alerts.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "user_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("users.user_id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "delivered_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("delivery_status", sa.String(length=16), nullable=False),
        sa.Column(
            "filter_reasons",
            sa.JSON().with_variant(postgresql.JSONB, "postgresql"),
            nullable=True,
        ),
        sa.UniqueConstraint("alert_id", "user_id", name="uq_alert_delivery_alert_user"),
        sa.CheckConstraint(
            "delivery_status IN ('sent', 'skipped', 'filtered')",
            name="ck_alert_deliveries_delivery_status",
        ),
    )
    op.create_index(
        "ix_alert_deliveries_user_status",
        "alert_deliveries",
        ["user_id", "delivery_status"],
    )
    op.create_index(
        "ix_alert_deliveries_delivered_at",
        "alert_deliveries",
        ["delivered_at"],
    )
    op.create_index(
        "ix_alert_deliveries_expires_at",
        "alert_deliveries",
        ["expires_at"],
    )

    op.create_table(
        "ai_recommendations",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column(
            "user_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("users.user_id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "alert_id",
            sa.Integer(),
            sa.ForeignKey("alerts.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("recommendation", sa.String(length=8), nullable=False),
        sa.Column("confidence", sa.String(length=8), nullable=False),
        sa.Column("rationale", sa.Text(), nullable=False),
        sa.Column("risks", sa.Text(), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False, default="PROPOSED"),
        sa.Column("telegram_message_id", sa.String(length=64), nullable=True),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "recommendation IN ('BUY', 'WAIT', 'SKIP')",
            name="ck_ai_recommendations_recommendation",
        ),
        sa.CheckConstraint(
            "confidence IN ('HIGH', 'MEDIUM', 'LOW')",
            name="ck_ai_recommendations_confidence",
        ),
        sa.CheckConstraint(
            "status IN ('PROPOSED', 'CONFIRMED', 'SKIPPED', 'EXPIRED')",
            name="ck_ai_recommendations_status",
        ),
    )
    op.create_index(
        "ix_ai_recommendations_user_status",
        "ai_recommendations",
        ["user_id", "status"],
    )
    op.create_index(
        "ix_ai_recommendations_user_created_desc",
        "ai_recommendations",
        ["user_id", sa.text("created_at DESC"), sa.text("id DESC")],
    )
    op.create_index(
        "ix_ai_recommendations_created_at",
        "ai_recommendations",
        ["created_at"],
    )

    op.create_table(
        "ai_market_mutes",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column(
            "user_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("users.user_id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("market_id", sa.String(length=128), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.UniqueConstraint("user_id", "market_id", name="uq_ai_market_mutes_user_market"),
    )
    op.create_index("ix_ai_market_mutes_expires_at", "ai_market_mutes", ["expires_at"])

    op.create_table(
        "ai_theme_mutes",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column(
            "user_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("users.user_id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("theme_key", sa.String(length=256), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.UniqueConstraint("user_id", "theme_key", name="uq_ai_theme_mutes_user_theme"),
    )
    op.create_index("ix_ai_theme_mutes_expires_at", "ai_theme_mutes", ["expires_at"])

    op.create_table(
        "ai_recommendation_events",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column(
            "recommendation_id",
            sa.Integer(),
            sa.ForeignKey("ai_recommendations.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "user_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("users.user_id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "alert_id",
            sa.Integer(),
            sa.ForeignKey("alerts.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("action", sa.String(length=32), nullable=False),
        sa.Column("details", sa.String(length=1024), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
    )
    op.create_index(
        "ix_ai_recommendation_events_user_created",
        "ai_recommendation_events",
        ["user_id", "created_at"],
    )

    _seed_plans(op.get_bind())


def downgrade() -> None:
    op.drop_table("ai_recommendation_events")
    op.drop_table("ai_theme_mutes")
    op.drop_table("ai_market_mutes")
    op.drop_table("ai_recommendations")
    op.drop_table("alert_deliveries")
    op.drop_table("alerts")
    op.drop_table("market_snapshots")
    op.drop_table("user_alert_preferences")
    op.drop_table("stripe_events")
    op.drop_table("subscriptions")
    op.drop_table("user_sessions")
    op.drop_table("user_auth")
    op.drop_table("pending_telegram_chats")
    op.drop_table("users")
    op.drop_table("plans")
