"""Retention expires_at support.

Revision ID: 20260120_retention_expires
Revises: 20260120_hot_query_indexes
Create Date: 2026-01-20 00:20:00.000000
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "20260120_retention_expires"
down_revision = "20260120_hot_query_indexes"
branch_labels = None
depends_on = None


SNAPSHOT_RETENTION_DAYS = 7
ALERT_RETENTION_DAYS = 30
DELIVERY_RETENTION_DAYS = 30


def upgrade() -> None:
    op.add_column(
        "market_snapshots",
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "alerts",
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "alert_deliveries",
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
    )

    op.execute(
        sa.text(
            "UPDATE market_snapshots "
            "SET expires_at = asof_ts + (:days * INTERVAL '1 day') "
            "WHERE expires_at IS NULL"
        ),
        {"days": SNAPSHOT_RETENTION_DAYS},
    )
    op.execute(
        sa.text(
            "UPDATE alerts "
            "SET expires_at = COALESCE(triggered_at, created_at) + (:days * INTERVAL '1 day') "
            "WHERE expires_at IS NULL"
        ),
        {"days": ALERT_RETENTION_DAYS},
    )
    op.execute(
        sa.text(
            "UPDATE alert_deliveries "
            "SET expires_at = delivered_at + (:days * INTERVAL '1 day') "
            "WHERE expires_at IS NULL"
        ),
        {"days": DELIVERY_RETENTION_DAYS},
    )

    op.create_index("ix_market_snapshots_expires_at", "market_snapshots", ["expires_at"])
    op.create_index("ix_alerts_expires_at", "alerts", ["expires_at"])
    op.create_index("ix_alert_deliveries_expires_at", "alert_deliveries", ["expires_at"])


def downgrade() -> None:
    op.drop_index("ix_alert_deliveries_expires_at", table_name="alert_deliveries")
    op.drop_index("ix_alerts_expires_at", table_name="alerts")
    op.drop_index("ix_market_snapshots_expires_at", table_name="market_snapshots")

    op.drop_column("alert_deliveries", "expires_at")
    op.drop_column("alerts", "expires_at")
    op.drop_column("market_snapshots", "expires_at")
