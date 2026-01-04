"""Add fast signals preference.

Revision ID: aa12bb34cc56
Revises: f9a1b2c3d4e5
Create Date: 2026-01-04 00:30:00.000000
"""

from alembic import op
import sqlalchemy as sa


revision = "aa12bb34cc56"
down_revision = "f9a1b2c3d4e5"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "user_alert_preferences",
        sa.Column("fast_signals_enabled", sa.Boolean(), server_default=sa.text("false"), nullable=False),
    )
    op.alter_column("user_alert_preferences", "fast_signals_enabled", server_default=None)


def downgrade() -> None:
    op.drop_column("user_alert_preferences", "fast_signals_enabled")
