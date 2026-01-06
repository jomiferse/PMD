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


def upgrade() -> None:
    op.create_table(
        "plans",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("name", sa.String(length=64), nullable=False),
        sa.Column("price_monthly", sa.Float(), nullable=True),
        sa.Column("is_active", sa.Boolean(), nullable=False, default=True),
        sa.Column("copilot_enabled", sa.Boolean(), nullable=True),
        sa.Column("max_copilot_per_day", sa.Integer(), nullable=True),
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
        sa.Column("fast_window_minutes", sa.Integer(), nullable=True),
        sa.Column("fast_max_themes_per_digest", sa.Integer(), nullable=True),
        sa.Column("fast_max_markets_per_theme", sa.Integer(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.UniqueConstraint("name", name="uq_plans_name"),
    )

    op.create_table(
        "users",
        sa.Column("user_id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("name", sa.String(length=128), nullable=False),
        sa.Column("telegram_chat_id", sa.String(length=64), nullable=True),
        sa.Column("is_active", sa.Boolean(), nullable=False, default=True),
        sa.Column("plan_id", sa.Integer(), sa.ForeignKey("plans.id"), nullable=True),
        sa.Column("copilot_enabled", sa.Boolean(), nullable=False, default=False),
        sa.Column(
            "overrides_json",
            sa.JSON().with_variant(postgresql.JSONB, "postgresql"),
            nullable=True,
        ),
        sa.Column("created_at", sa.DateTime(), nullable=False),
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
        sa.Column("created_at", sa.DateTime(), nullable=False),
    )

    op.create_table(
        "user_preferences",
        sa.Column(
            "user_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("users.user_id", ondelete="CASCADE"),
            primary_key=True,
        ),
        sa.Column("risk_budget_usd_per_day", sa.Numeric(), nullable=False, server_default="0"),
        sa.Column("max_usd_per_trade", sa.Numeric(), nullable=False, server_default="0"),
        sa.Column("max_liquidity_fraction", sa.Numeric(), nullable=False, server_default="0.01"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
            onupdate=sa.func.now(),
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
        sa.Column("source_ts", sa.DateTime(), nullable=True),
        sa.Column("snapshot_bucket", sa.DateTime(), nullable=False),
        sa.Column("asof_ts", sa.DateTime(), nullable=False),
        sa.UniqueConstraint("market_id", "snapshot_bucket", name="uq_market_bucket"),
    )
    op.create_index("ix_market_snapshots_market_id", "market_snapshots", ["market_id"])
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
        sa.Column("strength", sa.String(length=16), nullable=False, default="MEDIUM"),
        sa.Column("snapshot_bucket", sa.DateTime(), nullable=False),
        sa.Column("source_ts", sa.DateTime(), nullable=True),
        sa.Column("message", sa.String(length=1024), nullable=False, default=""),
        sa.Column("triggered_at", sa.DateTime(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.UniqueConstraint(
            "alert_type",
            "market_id",
            "snapshot_bucket",
            name="uq_alert_market_bucket",
        ),
    )
    op.create_index("ix_alerts_created_at", "alerts", ["created_at"])
    op.create_index("ix_alerts_tenant_type", "alerts", ["tenant_id", "alert_type"])
    op.create_index("ix_alerts_tenant_created", "alerts", ["tenant_id", "created_at"])
    op.create_index(
        "ix_alerts_cooldown",
        "alerts",
        ["tenant_id", "alert_type", "market_id", "triggered_at"],
    )
    op.create_index("ix_alerts_market_triggered", "alerts", ["market_id", "triggered_at"])

    op.create_table(
        "api_keys",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("tenant_id", sa.String(length=64), nullable=False),
        sa.Column("name", sa.String(length=128), nullable=False),
        sa.Column("key_hash", sa.String(length=64), nullable=False),
        sa.Column("plan", sa.String(length=64), nullable=False, default="basic"),
        sa.Column("rate_limit_per_min", sa.Integer(), nullable=False, default=60),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("last_used_at", sa.DateTime(), nullable=True),
        sa.Column("revoked_at", sa.DateTime(), nullable=True),
        sa.UniqueConstraint("key_hash", name="uq_api_key_hash"),
    )
    op.create_index("ix_api_keys_tenant_id", "api_keys", ["tenant_id"])

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
        sa.Column("delivered_at", sa.DateTime(), nullable=False),
        sa.Column("delivery_status", sa.String(length=16), nullable=False),
        sa.Column(
            "filter_reasons",
            sa.JSON().with_variant(postgresql.JSONB, "postgresql"),
            nullable=True,
        ),
        sa.UniqueConstraint("alert_id", "user_id", name="uq_alert_delivery_alert_user"),
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
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("recommendation", sa.String(length=8), nullable=False),
        sa.Column("confidence", sa.String(length=8), nullable=False),
        sa.Column("rationale", sa.Text(), nullable=False),
        sa.Column("risks", sa.Text(), nullable=False),
        sa.Column("draft_side", sa.String(length=8), nullable=True),
        sa.Column("draft_price", sa.Float(), nullable=True),
        sa.Column("draft_size", sa.Float(), nullable=True),
        sa.Column("draft_notional_usd", sa.Float(), nullable=True),
        sa.Column("status", sa.String(length=16), nullable=False, default="PROPOSED"),
        sa.Column("telegram_message_id", sa.String(length=64), nullable=True),
        sa.Column("expires_at", sa.DateTime(), nullable=True),
    )
    op.create_index(
        "ix_ai_recommendations_user_status",
        "ai_recommendations",
        ["user_id", "status"],
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
        sa.Column("expires_at", sa.DateTime(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
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
        sa.Column("expires_at", sa.DateTime(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
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
        sa.Column("created_at", sa.DateTime(), nullable=False),
    )
    op.create_index(
        "ix_ai_recommendation_events_user_created",
        "ai_recommendation_events",
        ["user_id", "created_at"],
    )

    op.create_table(
        "user_polymarket_credentials",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column(
            "user_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("users.user_id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("encrypted_payload", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.UniqueConstraint("user_id", name="uq_user_polymarket_credentials_user"),
    )


def downgrade() -> None:
    op.drop_table("user_polymarket_credentials")
    op.drop_table("ai_recommendation_events")
    op.drop_index("ix_ai_theme_mutes_expires_at", table_name="ai_theme_mutes")
    op.drop_table("ai_theme_mutes")
    op.drop_index("ix_ai_market_mutes_expires_at", table_name="ai_market_mutes")
    op.drop_table("ai_market_mutes")
    op.drop_index("ix_ai_recommendations_created_at", table_name="ai_recommendations")
    op.drop_index("ix_ai_recommendations_user_status", table_name="ai_recommendations")
    op.drop_table("ai_recommendations")
    op.drop_index("ix_alert_deliveries_delivered_at", table_name="alert_deliveries")
    op.drop_index("ix_alert_deliveries_user_status", table_name="alert_deliveries")
    op.drop_table("alert_deliveries")
    op.drop_index("ix_api_keys_tenant_id", table_name="api_keys")
    op.drop_table("api_keys")
    op.drop_index("ix_alerts_market_triggered", table_name="alerts")
    op.drop_index("ix_alerts_cooldown", table_name="alerts")
    op.drop_index("ix_alerts_tenant_created", table_name="alerts")
    op.drop_index("ix_alerts_tenant_type", table_name="alerts")
    op.drop_index("ix_alerts_created_at", table_name="alerts")
    op.drop_table("alerts")
    op.drop_index("ix_market_snapshots_asof_desc", table_name="market_snapshots")
    op.drop_index("ix_market_snapshots_market_asof", table_name="market_snapshots")
    op.drop_index("ix_market_snapshots_bucket", table_name="market_snapshots")
    op.drop_index("ix_market_snapshots_market_id", table_name="market_snapshots")
    op.drop_table("market_snapshots")
    op.drop_table("user_preferences")
    op.drop_table("user_alert_preferences")
    op.drop_table("users")
    op.drop_table("plans")
