"""Add best_ask to alerts.

Revision ID: 20260106_add_alert_best_ask
Revises: 20260105_baseline
Create Date: 2026-01-06 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "20260106_add_alert_best_ask"
down_revision = "20260105_baseline"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "alerts",
        sa.Column("best_ask", sa.Float(), nullable=False, server_default=sa.text("0")),
    )


def downgrade() -> None:
    op.drop_column("alerts", "best_ask")
