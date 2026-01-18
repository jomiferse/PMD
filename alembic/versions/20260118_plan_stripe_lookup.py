"""Add Stripe price lookup key to plans.

Revision ID: 20260118_plan_stripe_lookup
Revises: 20260113_perf_indexes
Create Date: 2026-01-18 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "20260118_plan_stripe_lookup"
down_revision = "20260113_perf_indexes"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "plans",
        sa.Column("stripe_price_lookup_key", sa.String(length=64), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("plans", "stripe_price_lookup_key")
