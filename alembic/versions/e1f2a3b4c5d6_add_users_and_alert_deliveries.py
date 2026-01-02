"""add users, preferences, and alert deliveries

Revision ID: e1f2a3b4c5d6
Revises: d4e5f6a7b8c9
Create Date: 2026-01-02 23:15:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision: str = "e1f2a3b4c5d6"
down_revision: Union[str, None] = "d4e5f6a7b8c9"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "users",
        sa.Column("user_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("name", sa.String(length=128), nullable=False),
        sa.Column("telegram_chat_id", sa.String(length=64), nullable=True),
        sa.Column("is_active", sa.Boolean(), server_default=sa.text("true"), nullable=False),
        sa.Column("created_at", sa.DateTime(), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("user_id"),
    )

    op.create_table(
        "user_alert_preferences",
        sa.Column("user_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("min_liquidity", sa.Float(), nullable=True),
        sa.Column("min_volume_24h", sa.Float(), nullable=True),
        sa.Column("min_abs_price_move", sa.Float(), nullable=True),
        sa.Column("alert_strengths", sa.String(length=32), nullable=True),
        sa.Column("digest_window_minutes", sa.Integer(), nullable=True),
        sa.Column("max_alerts_per_digest", sa.Integer(), nullable=True),
        sa.Column("created_at", sa.DateTime(), server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(["user_id"], ["users.user_id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("user_id"),
    )

    op.create_table(
        "alert_deliveries",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("alert_id", sa.Integer(), nullable=False),
        sa.Column("user_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("delivered_at", sa.DateTime(), server_default=sa.text("now()"), nullable=False),
        sa.Column("delivery_status", sa.String(length=16), nullable=False),
        sa.ForeignKeyConstraint(["alert_id"], ["alerts.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["user_id"], ["users.user_id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("alert_id", "user_id", name="uq_alert_delivery_alert_user"),
    )
    op.create_index(
        "ix_alert_deliveries_user_status",
        "alert_deliveries",
        ["user_id", "delivery_status"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_alert_deliveries_user_status", table_name="alert_deliveries")
    op.drop_table("alert_deliveries")
    op.drop_table("user_alert_preferences")
    op.drop_table("users")
