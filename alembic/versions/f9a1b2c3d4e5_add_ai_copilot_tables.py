"""add ai copilot preferences and recommendation tables

Revision ID: f9a1b2c3d4e5
Revises: f7a8b9c0d1e2
Create Date: 2026-01-03 02:10:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision: str = "f9a1b2c3d4e5"
down_revision: Union[str, None] = "f7a8b9c0d1e2"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "user_alert_preferences",
        sa.Column("ai_copilot_enabled", sa.Boolean(), server_default=sa.text("false"), nullable=False),
    )
    op.add_column(
        "user_alert_preferences",
        sa.Column("risk_budget_usd_per_day", sa.Float(), server_default=sa.text("0"), nullable=False),
    )
    op.add_column(
        "user_alert_preferences",
        sa.Column("max_usd_per_trade", sa.Float(), server_default=sa.text("0"), nullable=False),
    )
    op.add_column(
        "user_alert_preferences",
        sa.Column("max_liquidity_fraction", sa.Float(), server_default=sa.text("0.01"), nullable=False),
    )

    op.create_table(
        "ai_recommendations",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("user_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("alert_id", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(), server_default=sa.text("now()"), nullable=False),
        sa.Column("recommendation", sa.String(length=8), nullable=False),
        sa.Column("confidence", sa.String(length=8), nullable=False),
        sa.Column("rationale", sa.Text(), nullable=False),
        sa.Column("risks", sa.Text(), nullable=False),
        sa.Column("draft_side", sa.String(length=8), nullable=True),
        sa.Column("draft_price", sa.Float(), nullable=True),
        sa.Column("draft_size", sa.Float(), nullable=True),
        sa.Column("draft_notional_usd", sa.Float(), nullable=True),
        sa.Column("status", sa.String(length=16), server_default=sa.text("'PROPOSED'"), nullable=False),
        sa.Column("telegram_message_id", sa.String(length=64), nullable=True),
        sa.Column("expires_at", sa.DateTime(), nullable=True),
        sa.ForeignKeyConstraint(["alert_id"], ["alerts.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["user_id"], ["users.user_id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_ai_recommendations_user_status",
        "ai_recommendations",
        ["user_id", "status"],
        unique=False,
    )
    op.create_index(
        "ix_ai_recommendations_created_at",
        "ai_recommendations",
        ["created_at"],
        unique=False,
    )

    op.create_table(
        "ai_market_mutes",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("user_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("market_id", sa.String(length=128), nullable=False),
        sa.Column("expires_at", sa.DateTime(), nullable=False),
        sa.Column("created_at", sa.DateTime(), server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(["user_id"], ["users.user_id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("user_id", "market_id", name="uq_ai_market_mutes_user_market"),
    )
    op.create_index(
        "ix_ai_market_mutes_expires_at",
        "ai_market_mutes",
        ["expires_at"],
        unique=False,
    )

    op.create_table(
        "ai_recommendation_events",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("recommendation_id", sa.Integer(), nullable=False),
        sa.Column("user_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("alert_id", sa.Integer(), nullable=False),
        sa.Column("action", sa.String(length=32), nullable=False),
        sa.Column("details", sa.String(length=1024), nullable=True),
        sa.Column("created_at", sa.DateTime(), server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(["alert_id"], ["alerts.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["recommendation_id"], ["ai_recommendations.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["user_id"], ["users.user_id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_ai_recommendation_events_user_created",
        "ai_recommendation_events",
        ["user_id", "created_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_ai_recommendation_events_user_created", table_name="ai_recommendation_events")
    op.drop_table("ai_recommendation_events")
    op.drop_index("ix_ai_market_mutes_expires_at", table_name="ai_market_mutes")
    op.drop_table("ai_market_mutes")
    op.drop_index("ix_ai_recommendations_created_at", table_name="ai_recommendations")
    op.drop_index("ix_ai_recommendations_user_status", table_name="ai_recommendations")
    op.drop_table("ai_recommendations")
    op.drop_column("user_alert_preferences", "max_liquidity_fraction")
    op.drop_column("user_alert_preferences", "max_usd_per_trade")
    op.drop_column("user_alert_preferences", "risk_budget_usd_per_day")
    op.drop_column("user_alert_preferences", "ai_copilot_enabled")
