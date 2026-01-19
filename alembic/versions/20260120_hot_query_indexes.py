"""Hot query indexes.

Revision ID: 20260120_hot_query_indexes
Revises: 20260120_constraints_typefixes
Create Date: 2026-01-20 00:10:00.000000
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "20260120_hot_query_indexes"
down_revision = "20260120_constraints_typefixes"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_index("ix_market_snapshots_market_id", table_name="market_snapshots")
    op.drop_index("ix_market_snapshots_market_bucket", table_name="market_snapshots")

    op.drop_index("ix_alerts_tenant_created", table_name="alerts")
    op.drop_index("ix_alerts_tenant_strength", table_name="alerts")
    op.drop_index("ix_alerts_tenant_category", table_name="alerts")

    op.drop_index("ix_ai_recommendations_user_created", table_name="ai_recommendations")
    op.drop_index("ix_subscriptions_user_id", table_name="subscriptions")

    op.create_index(
        "ix_alerts_tenant_created_desc",
        "alerts",
        ["tenant_id", sa.text("created_at DESC"), sa.text("id DESC")],
    )
    op.create_index(
        "ix_alerts_tenant_strength_created_desc",
        "alerts",
        ["tenant_id", "strength", sa.text("created_at DESC"), sa.text("id DESC")],
    )
    op.create_index(
        "ix_alerts_tenant_category_norm_created_desc",
        "alerts",
        [
            "tenant_id",
            sa.text("lower(category)"),
            sa.text("created_at DESC"),
            sa.text("id DESC"),
        ],
    )

    op.create_index(
        "ix_ai_recommendations_user_created_desc",
        "ai_recommendations",
        ["user_id", sa.text("created_at DESC"), sa.text("id DESC")],
    )

    op.create_index(
        "ix_subscriptions_user_created_desc",
        "subscriptions",
        ["user_id", sa.text("created_at DESC")],
    )


def downgrade() -> None:
    op.drop_index("ix_subscriptions_user_created_desc", table_name="subscriptions")
    op.drop_index("ix_ai_recommendations_user_created_desc", table_name="ai_recommendations")
    op.drop_index("ix_alerts_tenant_category_norm_created_desc", table_name="alerts")
    op.drop_index("ix_alerts_tenant_strength_created_desc", table_name="alerts")
    op.drop_index("ix_alerts_tenant_created_desc", table_name="alerts")

    op.create_index("ix_subscriptions_user_id", "subscriptions", ["user_id"])
    op.create_index("ix_ai_recommendations_user_created", "ai_recommendations", ["user_id", "created_at"])

    op.create_index("ix_alerts_tenant_category", "alerts", ["tenant_id", "category"])
    op.create_index("ix_alerts_tenant_strength", "alerts", ["tenant_id", "strength"])
    op.create_index("ix_alerts_tenant_created", "alerts", ["tenant_id", "created_at"])

    op.create_index("ix_market_snapshots_market_bucket", "market_snapshots", ["market_id", "snapshot_bucket"])
    op.create_index("ix_market_snapshots_market_id", "market_snapshots", ["market_id"])
