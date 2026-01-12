"""Add market_p_no to market snapshots.

Revision ID: 20260112_market_p_no
Revises: 20260109_auth_billing
Create Date: 2026-01-12 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect

from app.core.snapshots import backfill_market_p_no


# revision identifiers, used by Alembic.
revision = "20260112_market_p_no"
down_revision = "20260109_auth_billing"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = inspect(bind)
    columns = {col["name"] for col in inspector.get_columns("market_snapshots")}

    if "market_p_no" not in columns:
        op.add_column("market_snapshots", sa.Column("market_p_no", sa.Float(), nullable=True))
    if "market_p_no_derived" not in columns:
        op.add_column(
            "market_snapshots",
            sa.Column(
                "market_p_no_derived",
                sa.Boolean(),
                nullable=True,
                server_default=sa.text("true"),
            ),
        )

    backfill_market_p_no(bind)


def downgrade() -> None:
    bind = op.get_bind()
    inspector = inspect(bind)
    columns = {col["name"] for col in inspector.get_columns("market_snapshots")}

    if "market_p_no_derived" in columns:
        op.drop_column("market_snapshots", "market_p_no_derived")
    if "market_p_no" in columns:
        op.drop_column("market_snapshots", "market_p_no")
