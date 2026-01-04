"""Add primary outcome label metadata.

Revision ID: b7c8d9e0f1a2
Revises: aa12bb34cc56
Create Date: 2026-01-04 01:10:00.000000
"""
from alembic import op
import sqlalchemy as sa


revision = "b7c8d9e0f1a2"
down_revision = "aa12bb34cc56"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("market_snapshots", sa.Column("primary_outcome_label", sa.String(length=64), nullable=True))
    op.add_column("market_snapshots", sa.Column("is_yesno", sa.Boolean(), nullable=True))
    op.add_column("alerts", sa.Column("primary_outcome_label", sa.String(length=64), nullable=True))
    op.add_column("alerts", sa.Column("is_yesno", sa.Boolean(), nullable=True))


def downgrade() -> None:
    op.drop_column("alerts", "is_yesno")
    op.drop_column("alerts", "primary_outcome_label")
    op.drop_column("market_snapshots", "is_yesno")
    op.drop_column("market_snapshots", "primary_outcome_label")
