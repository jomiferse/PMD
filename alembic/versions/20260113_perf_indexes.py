"""Add performance indexes for alerts and snapshots.

Revision ID: 20260113_perf_indexes
Revises: 20260112_market_p_no
Create Date: 2026-01-13 00:00:00.000000
"""

from alembic import op
from sqlalchemy import inspect

# revision identifiers, used by Alembic.
revision = "20260113_perf_indexes"
down_revision = "20260112_market_p_no"
branch_labels = None
depends_on = None


def _index_names(inspector, table_name: str) -> set[str]:
    return {idx["name"] for idx in inspector.get_indexes(table_name)}


def upgrade() -> None:
    bind = op.get_bind()
    inspector = inspect(bind)

    snapshot_indexes = _index_names(inspector, "market_snapshots")
    if "ix_market_snapshots_market_bucket" not in snapshot_indexes:
        op.create_index(
            "ix_market_snapshots_market_bucket",
            "market_snapshots",
            ["market_id", "snapshot_bucket"],
        )

    alert_indexes = _index_names(inspector, "alerts")
    if "ix_alerts_tenant_strength" not in alert_indexes:
        op.create_index(
            "ix_alerts_tenant_strength",
            "alerts",
            ["tenant_id", "strength"],
        )
    if "ix_alerts_tenant_category" not in alert_indexes:
        op.create_index(
            "ix_alerts_tenant_category",
            "alerts",
            ["tenant_id", "category"],
        )

    rec_indexes = _index_names(inspector, "ai_recommendations")
    if "ix_ai_recommendations_user_created" not in rec_indexes:
        op.create_index(
            "ix_ai_recommendations_user_created",
            "ai_recommendations",
            ["user_id", "created_at"],
        )


def downgrade() -> None:
    bind = op.get_bind()
    inspector = inspect(bind)

    snapshot_indexes = _index_names(inspector, "market_snapshots")
    if "ix_market_snapshots_market_bucket" in snapshot_indexes:
        op.drop_index("ix_market_snapshots_market_bucket", table_name="market_snapshots")

    alert_indexes = _index_names(inspector, "alerts")
    if "ix_alerts_tenant_strength" in alert_indexes:
        op.drop_index("ix_alerts_tenant_strength", table_name="alerts")
    if "ix_alerts_tenant_category" in alert_indexes:
        op.drop_index("ix_alerts_tenant_category", table_name="alerts")

    rec_indexes = _index_names(inspector, "ai_recommendations")
    if "ix_ai_recommendations_user_created" in rec_indexes:
        op.drop_index("ix_ai_recommendations_user_created", table_name="ai_recommendations")
