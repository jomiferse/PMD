"""Add max_fast_copilot_per_day to plans.

Revision ID: 20260107_add_max_fast_copilot
Revises: 20260106_add_alert_best_ask
Create Date: 2026-01-07 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "20260107_add_max_fast_copilot"
down_revision = "20260106_add_alert_best_ask"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "plans",
        sa.Column("max_fast_copilot_per_day", sa.Integer(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("plans", "max_fast_copilot_per_day")
