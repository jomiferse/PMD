"""Type fixes + constraints.

Revision ID: 20260120_constraints_typefixes
Revises: 20260105_baseline
Create Date: 2026-01-20 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "20260120_constraints_typefixes"
down_revision = "20260105_baseline"
branch_labels = None
depends_on = None


_DATETIME_COLUMNS = [
    ("plans", "created_at", False),
    ("users", "created_at", False),
    ("user_auth", "created_at", False),
    ("user_sessions", "created_at", False),
    ("user_sessions", "expires_at", False),
    ("user_sessions", "revoked_at", True),
    ("subscriptions", "current_period_end", True),
    ("subscriptions", "created_at", False),
    ("subscriptions", "updated_at", False),
    ("stripe_events", "created_at", False),
    ("user_alert_preferences", "created_at", False),
    ("market_snapshots", "source_ts", True),
    ("market_snapshots", "snapshot_bucket", False),
    ("market_snapshots", "asof_ts", False),
    ("alerts", "snapshot_bucket", False),
    ("alerts", "source_ts", True),
    ("alerts", "triggered_at", False),
    ("alerts", "created_at", False),
    ("alert_deliveries", "delivered_at", False),
    ("ai_recommendations", "created_at", False),
    ("ai_recommendations", "expires_at", True),
    ("ai_market_mutes", "expires_at", False),
    ("ai_market_mutes", "created_at", False),
    ("ai_theme_mutes", "expires_at", False),
    ("ai_theme_mutes", "created_at", False),
    ("ai_recommendation_events", "created_at", False),
]

_DEFAULT_NOW_COLUMNS = {
    ("plans", "created_at"),
    ("users", "created_at"),
    ("user_auth", "created_at"),
    ("user_sessions", "created_at"),
    ("subscriptions", "created_at"),
    ("subscriptions", "updated_at"),
    ("stripe_events", "created_at"),
    ("user_alert_preferences", "created_at"),
    ("market_snapshots", "asof_ts"),
    ("alerts", "triggered_at"),
    ("alerts", "created_at"),
    ("alert_deliveries", "delivered_at"),
    ("ai_recommendations", "created_at"),
    ("ai_market_mutes", "created_at"),
    ("ai_theme_mutes", "created_at"),
    ("ai_recommendation_events", "created_at"),
}

_ORIGINAL_DEFAULT_NOW_COLUMNS = {
    ("user_auth", "created_at"),
    ("user_sessions", "created_at"),
    ("subscriptions", "created_at"),
    ("subscriptions", "updated_at"),
    ("stripe_events", "created_at"),
}


def upgrade() -> None:
    for table_name, column_name, nullable in _DATETIME_COLUMNS:
        op.alter_column(
            table_name,
            column_name,
            existing_type=sa.DateTime(),
            type_=sa.DateTime(timezone=True),
            existing_nullable=nullable,
            postgresql_using=f"{column_name} AT TIME ZONE 'UTC'",
        )

    for table_name, column_name in _DEFAULT_NOW_COLUMNS:
        op.alter_column(
            table_name,
            column_name,
            existing_type=sa.DateTime(timezone=True),
            existing_nullable=False,
            server_default=sa.text("now()"),
        )

    op.create_check_constraint(
        "ck_market_snapshots_market_p_yes",
        "market_snapshots",
        "market_p_yes >= 0 AND market_p_yes <= 1",
    )
    op.create_check_constraint(
        "ck_market_snapshots_market_p_no",
        "market_snapshots",
        "market_p_no IS NULL OR (market_p_no >= 0 AND market_p_no <= 1)",
    )
    op.create_check_constraint(
        "ck_market_snapshots_model_p_yes",
        "market_snapshots",
        "model_p_yes >= 0 AND model_p_yes <= 1",
    )
    op.create_check_constraint(
        "ck_market_snapshots_liquidity_non_negative",
        "market_snapshots",
        "liquidity >= 0",
    )
    op.create_check_constraint(
        "ck_market_snapshots_volume_24h_non_negative",
        "market_snapshots",
        "volume_24h >= 0",
    )
    op.create_check_constraint(
        "ck_market_snapshots_volume_1w_non_negative",
        "market_snapshots",
        "volume_1w >= 0",
    )
    op.create_check_constraint(
        "ck_market_snapshots_best_ask_non_negative",
        "market_snapshots",
        "best_ask >= 0",
    )
    op.create_check_constraint(
        "ck_market_snapshots_last_trade_price_non_negative",
        "market_snapshots",
        "last_trade_price >= 0",
    )
    op.create_check_constraint(
        "ck_market_snapshots_market_kind",
        "market_snapshots",
        "market_kind IS NULL OR market_kind IN ('yesno', 'ou', 'multi')",
    )
    op.create_check_constraint(
        "ck_market_snapshots_mapping_confidence",
        "market_snapshots",
        "mapping_confidence IS NULL OR mapping_confidence IN ('verified', 'unknown')",
    )

    op.create_check_constraint(
        "ck_alerts_market_p_yes",
        "alerts",
        "market_p_yes >= 0 AND market_p_yes <= 1",
    )
    op.create_check_constraint(
        "ck_alerts_prev_market_p_yes",
        "alerts",
        "prev_market_p_yes >= 0 AND prev_market_p_yes <= 1",
    )
    op.create_check_constraint(
        "ck_alerts_old_price",
        "alerts",
        "old_price >= 0 AND old_price <= 1",
    )
    op.create_check_constraint(
        "ck_alerts_new_price",
        "alerts",
        "new_price >= 0 AND new_price <= 1",
    )
    op.create_check_constraint(
        "ck_alerts_liquidity_non_negative",
        "alerts",
        "liquidity >= 0",
    )
    op.create_check_constraint(
        "ck_alerts_volume_24h_non_negative",
        "alerts",
        "volume_24h >= 0",
    )
    op.create_check_constraint(
        "ck_alerts_best_ask_non_negative",
        "alerts",
        "best_ask >= 0",
    )
    op.create_check_constraint(
        "ck_alerts_strength",
        "alerts",
        "strength IN ('LOW', 'MEDIUM', 'HIGH', 'STRONG')",
    )
    op.create_check_constraint(
        "ck_alerts_market_kind",
        "alerts",
        "market_kind IS NULL OR market_kind IN ('yesno', 'ou', 'multi')",
    )
    op.create_check_constraint(
        "ck_alerts_mapping_confidence",
        "alerts",
        "mapping_confidence IS NULL OR mapping_confidence IN ('verified', 'unknown')",
    )

    op.create_check_constraint(
        "ck_plans_price_monthly_non_negative",
        "plans",
        "price_monthly IS NULL OR price_monthly >= 0",
    )
    op.create_check_constraint(
        "ck_plans_max_copilot_per_day_non_negative",
        "plans",
        "max_copilot_per_day IS NULL OR max_copilot_per_day >= 0",
    )
    op.create_check_constraint(
        "ck_plans_max_fast_copilot_per_day_non_negative",
        "plans",
        "max_fast_copilot_per_day IS NULL OR max_fast_copilot_per_day >= 0",
    )
    op.create_check_constraint(
        "ck_plans_max_copilot_per_hour_non_negative",
        "plans",
        "max_copilot_per_hour IS NULL OR max_copilot_per_hour >= 0",
    )
    op.create_check_constraint(
        "ck_plans_max_copilot_per_digest_non_negative",
        "plans",
        "max_copilot_per_digest IS NULL OR max_copilot_per_digest >= 0",
    )
    op.create_check_constraint(
        "ck_plans_copilot_theme_ttl_minutes_positive",
        "plans",
        "copilot_theme_ttl_minutes IS NULL OR copilot_theme_ttl_minutes > 0",
    )
    op.create_check_constraint(
        "ck_plans_digest_window_minutes_positive",
        "plans",
        "digest_window_minutes IS NULL OR digest_window_minutes > 0",
    )
    op.create_check_constraint(
        "ck_plans_max_themes_per_digest_non_negative",
        "plans",
        "max_themes_per_digest IS NULL OR max_themes_per_digest >= 0",
    )
    op.create_check_constraint(
        "ck_plans_max_alerts_per_digest_non_negative",
        "plans",
        "max_alerts_per_digest IS NULL OR max_alerts_per_digest >= 0",
    )
    op.create_check_constraint(
        "ck_plans_max_markets_per_theme_non_negative",
        "plans",
        "max_markets_per_theme IS NULL OR max_markets_per_theme >= 0",
    )
    op.create_check_constraint(
        "ck_plans_min_liquidity_non_negative",
        "plans",
        "min_liquidity IS NULL OR min_liquidity >= 0",
    )
    op.create_check_constraint(
        "ck_plans_min_volume_24h_non_negative",
        "plans",
        "min_volume_24h IS NULL OR min_volume_24h >= 0",
    )
    op.create_check_constraint(
        "ck_plans_min_abs_move_non_negative",
        "plans",
        "min_abs_move IS NULL OR min_abs_move >= 0",
    )
    op.create_check_constraint(
        "ck_plans_p_min_range",
        "plans",
        "p_min IS NULL OR (p_min >= 0 AND p_min <= 1)",
    )
    op.create_check_constraint(
        "ck_plans_p_max_range",
        "plans",
        "p_max IS NULL OR (p_max >= 0 AND p_max <= 1)",
    )
    op.create_check_constraint(
        "ck_plans_p_range_order",
        "plans",
        "p_min IS NULL OR p_max IS NULL OR p_min < p_max",
    )
    op.create_check_constraint(
        "ck_plans_fast_window_minutes_positive",
        "plans",
        "fast_window_minutes IS NULL OR fast_window_minutes > 0",
    )
    op.create_check_constraint(
        "ck_plans_fast_max_themes_per_digest_non_negative",
        "plans",
        "fast_max_themes_per_digest IS NULL OR fast_max_themes_per_digest >= 0",
    )
    op.create_check_constraint(
        "ck_plans_fast_max_markets_per_theme_non_negative",
        "plans",
        "fast_max_markets_per_theme IS NULL OR fast_max_markets_per_theme >= 0",
    )

    op.create_check_constraint(
        "ck_user_alert_preferences_min_liquidity_non_negative",
        "user_alert_preferences",
        "min_liquidity IS NULL OR min_liquidity >= 0",
    )
    op.create_check_constraint(
        "ck_user_alert_preferences_min_volume_24h_non_negative",
        "user_alert_preferences",
        "min_volume_24h IS NULL OR min_volume_24h >= 0",
    )
    op.create_check_constraint(
        "ck_user_alert_preferences_min_abs_price_move_non_negative",
        "user_alert_preferences",
        "min_abs_price_move IS NULL OR min_abs_price_move >= 0",
    )
    op.create_check_constraint(
        "ck_user_alert_preferences_digest_window_minutes_positive",
        "user_alert_preferences",
        "digest_window_minutes IS NULL OR digest_window_minutes > 0",
    )
    op.create_check_constraint(
        "ck_user_alert_preferences_max_alerts_per_digest_non_negative",
        "user_alert_preferences",
        "max_alerts_per_digest IS NULL OR max_alerts_per_digest >= 0",
    )
    op.create_check_constraint(
        "ck_user_alert_preferences_max_themes_per_digest_non_negative",
        "user_alert_preferences",
        "max_themes_per_digest IS NULL OR max_themes_per_digest >= 0",
    )
    op.create_check_constraint(
        "ck_user_alert_preferences_max_markets_per_theme_non_negative",
        "user_alert_preferences",
        "max_markets_per_theme IS NULL OR max_markets_per_theme >= 0",
    )
    op.create_check_constraint(
        "ck_user_alert_preferences_p_min_range",
        "user_alert_preferences",
        "p_min IS NULL OR (p_min >= 0 AND p_min <= 1)",
    )
    op.create_check_constraint(
        "ck_user_alert_preferences_p_max_range",
        "user_alert_preferences",
        "p_max IS NULL OR (p_max >= 0 AND p_max <= 1)",
    )
    op.create_check_constraint(
        "ck_user_alert_preferences_p_range_order",
        "user_alert_preferences",
        "p_min IS NULL OR p_max IS NULL OR p_min < p_max",
    )
    op.create_check_constraint(
        "ck_user_alert_preferences_fast_window_minutes_positive",
        "user_alert_preferences",
        "fast_window_minutes IS NULL OR fast_window_minutes > 0",
    )
    op.create_check_constraint(
        "ck_user_alert_preferences_fast_max_themes_per_digest_non_negative",
        "user_alert_preferences",
        "fast_max_themes_per_digest IS NULL OR fast_max_themes_per_digest >= 0",
    )
    op.create_check_constraint(
        "ck_user_alert_preferences_fast_max_markets_per_theme_non_negative",
        "user_alert_preferences",
        "fast_max_markets_per_theme IS NULL OR fast_max_markets_per_theme >= 0",
    )

    op.create_check_constraint(
        "ck_alert_deliveries_delivery_status",
        "alert_deliveries",
        "delivery_status IN ('sent', 'skipped', 'filtered')",
    )

    op.create_check_constraint(
        "ck_ai_recommendations_recommendation",
        "ai_recommendations",
        "recommendation IN ('BUY', 'WAIT', 'SKIP')",
    )
    op.create_check_constraint(
        "ck_ai_recommendations_confidence",
        "ai_recommendations",
        "confidence IN ('HIGH', 'MEDIUM', 'LOW')",
    )
    op.create_check_constraint(
        "ck_ai_recommendations_status",
        "ai_recommendations",
        "status IN ('PROPOSED', 'CONFIRMED', 'SKIPPED', 'EXPIRED')",
    )

    op.create_check_constraint(
        "ck_user_sessions_expires_after_created",
        "user_sessions",
        "expires_at > created_at",
    )


def downgrade() -> None:
    op.drop_constraint("ck_user_sessions_expires_after_created", "user_sessions", type_="check")

    op.drop_constraint("ck_ai_recommendations_status", "ai_recommendations", type_="check")
    op.drop_constraint("ck_ai_recommendations_confidence", "ai_recommendations", type_="check")
    op.drop_constraint("ck_ai_recommendations_recommendation", "ai_recommendations", type_="check")

    op.drop_constraint("ck_alert_deliveries_delivery_status", "alert_deliveries", type_="check")

    op.drop_constraint("ck_user_alert_preferences_fast_max_markets_per_theme_non_negative", "user_alert_preferences", type_="check")
    op.drop_constraint("ck_user_alert_preferences_fast_max_themes_per_digest_non_negative", "user_alert_preferences", type_="check")
    op.drop_constraint("ck_user_alert_preferences_fast_window_minutes_positive", "user_alert_preferences", type_="check")
    op.drop_constraint("ck_user_alert_preferences_p_range_order", "user_alert_preferences", type_="check")
    op.drop_constraint("ck_user_alert_preferences_p_max_range", "user_alert_preferences", type_="check")
    op.drop_constraint("ck_user_alert_preferences_p_min_range", "user_alert_preferences", type_="check")
    op.drop_constraint("ck_user_alert_preferences_max_markets_per_theme_non_negative", "user_alert_preferences", type_="check")
    op.drop_constraint("ck_user_alert_preferences_max_themes_per_digest_non_negative", "user_alert_preferences", type_="check")
    op.drop_constraint("ck_user_alert_preferences_max_alerts_per_digest_non_negative", "user_alert_preferences", type_="check")
    op.drop_constraint("ck_user_alert_preferences_digest_window_minutes_positive", "user_alert_preferences", type_="check")
    op.drop_constraint("ck_user_alert_preferences_min_abs_price_move_non_negative", "user_alert_preferences", type_="check")
    op.drop_constraint("ck_user_alert_preferences_min_volume_24h_non_negative", "user_alert_preferences", type_="check")
    op.drop_constraint("ck_user_alert_preferences_min_liquidity_non_negative", "user_alert_preferences", type_="check")

    op.drop_constraint("ck_plans_fast_max_markets_per_theme_non_negative", "plans", type_="check")
    op.drop_constraint("ck_plans_fast_max_themes_per_digest_non_negative", "plans", type_="check")
    op.drop_constraint("ck_plans_fast_window_minutes_positive", "plans", type_="check")
    op.drop_constraint("ck_plans_p_range_order", "plans", type_="check")
    op.drop_constraint("ck_plans_p_max_range", "plans", type_="check")
    op.drop_constraint("ck_plans_p_min_range", "plans", type_="check")
    op.drop_constraint("ck_plans_min_abs_move_non_negative", "plans", type_="check")
    op.drop_constraint("ck_plans_min_volume_24h_non_negative", "plans", type_="check")
    op.drop_constraint("ck_plans_min_liquidity_non_negative", "plans", type_="check")
    op.drop_constraint("ck_plans_max_markets_per_theme_non_negative", "plans", type_="check")
    op.drop_constraint("ck_plans_max_alerts_per_digest_non_negative", "plans", type_="check")
    op.drop_constraint("ck_plans_max_themes_per_digest_non_negative", "plans", type_="check")
    op.drop_constraint("ck_plans_digest_window_minutes_positive", "plans", type_="check")
    op.drop_constraint("ck_plans_copilot_theme_ttl_minutes_positive", "plans", type_="check")
    op.drop_constraint("ck_plans_max_copilot_per_digest_non_negative", "plans", type_="check")
    op.drop_constraint("ck_plans_max_copilot_per_hour_non_negative", "plans", type_="check")
    op.drop_constraint("ck_plans_max_fast_copilot_per_day_non_negative", "plans", type_="check")
    op.drop_constraint("ck_plans_max_copilot_per_day_non_negative", "plans", type_="check")
    op.drop_constraint("ck_plans_price_monthly_non_negative", "plans", type_="check")

    op.drop_constraint("ck_alerts_mapping_confidence", "alerts", type_="check")
    op.drop_constraint("ck_alerts_market_kind", "alerts", type_="check")
    op.drop_constraint("ck_alerts_strength", "alerts", type_="check")
    op.drop_constraint("ck_alerts_best_ask_non_negative", "alerts", type_="check")
    op.drop_constraint("ck_alerts_volume_24h_non_negative", "alerts", type_="check")
    op.drop_constraint("ck_alerts_liquidity_non_negative", "alerts", type_="check")
    op.drop_constraint("ck_alerts_new_price", "alerts", type_="check")
    op.drop_constraint("ck_alerts_old_price", "alerts", type_="check")
    op.drop_constraint("ck_alerts_prev_market_p_yes", "alerts", type_="check")
    op.drop_constraint("ck_alerts_market_p_yes", "alerts", type_="check")

    op.drop_constraint("ck_market_snapshots_mapping_confidence", "market_snapshots", type_="check")
    op.drop_constraint("ck_market_snapshots_market_kind", "market_snapshots", type_="check")
    op.drop_constraint("ck_market_snapshots_last_trade_price_non_negative", "market_snapshots", type_="check")
    op.drop_constraint("ck_market_snapshots_best_ask_non_negative", "market_snapshots", type_="check")
    op.drop_constraint("ck_market_snapshots_volume_1w_non_negative", "market_snapshots", type_="check")
    op.drop_constraint("ck_market_snapshots_volume_24h_non_negative", "market_snapshots", type_="check")
    op.drop_constraint("ck_market_snapshots_liquidity_non_negative", "market_snapshots", type_="check")
    op.drop_constraint("ck_market_snapshots_model_p_yes", "market_snapshots", type_="check")
    op.drop_constraint("ck_market_snapshots_market_p_no", "market_snapshots", type_="check")
    op.drop_constraint("ck_market_snapshots_market_p_yes", "market_snapshots", type_="check")

    for table_name, column_name in _DEFAULT_NOW_COLUMNS:
        server_default = sa.text("now()") if (table_name, column_name) in _ORIGINAL_DEFAULT_NOW_COLUMNS else None
        op.alter_column(
            table_name,
            column_name,
            existing_type=sa.DateTime(timezone=True),
            existing_nullable=False,
            server_default=server_default,
        )

    for table_name, column_name, nullable in _DATETIME_COLUMNS:
        op.alter_column(
            table_name,
            column_name,
            existing_type=sa.DateTime(timezone=True),
            type_=sa.DateTime(),
            existing_nullable=nullable,
            postgresql_using=f"{column_name} AT TIME ZONE 'UTC'",
        )
