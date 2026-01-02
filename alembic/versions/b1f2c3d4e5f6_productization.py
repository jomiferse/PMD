"""productization tables and snapshot fields

Revision ID: b1f2c3d4e5f6
Revises: a955e60915f1
Create Date: 2026-01-02 02:10:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "b1f2c3d4e5f6"
down_revision: Union[str, None] = "a955e60915f1"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("market_snapshots", sa.Column("source_ts", sa.DateTime(), nullable=True))
    op.add_column("market_snapshots", sa.Column("snapshot_bucket", sa.DateTime(), nullable=True))
    op.add_column("market_snapshots", sa.Column("volume_24h", sa.Float(), server_default="0", nullable=False))
    op.add_column("market_snapshots", sa.Column("volume_1w", sa.Float(), server_default="0", nullable=False))
    op.add_column("market_snapshots", sa.Column("best_ask", sa.Float(), server_default="0", nullable=False))
    op.add_column("market_snapshots", sa.Column("last_trade_price", sa.Float(), server_default="0", nullable=False))

    op.execute(
        """
        UPDATE market_snapshots
        SET snapshot_bucket = date_trunc('hour', asof_ts)
            + (floor(date_part('minute', asof_ts) / 5) * interval '5 minutes')
        """
    )
    op.alter_column("market_snapshots", "snapshot_bucket", nullable=False)

    op.execute(
        """
        DELETE FROM market_snapshots a
        USING market_snapshots b
        WHERE a.market_id = b.market_id
          AND a.snapshot_bucket = b.snapshot_bucket
          AND a.id > b.id
        """
    )

    op.drop_constraint("uq_market_asof", "market_snapshots", type_="unique")
    op.create_unique_constraint("uq_market_bucket", "market_snapshots", ["market_id", "snapshot_bucket"])
    op.create_index("ix_market_snapshots_bucket", "market_snapshots", ["snapshot_bucket"], unique=False)

    op.create_table(
        "api_keys",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("tenant_id", sa.String(length=64), nullable=False),
        sa.Column("name", sa.String(length=128), nullable=False),
        sa.Column("key_hash", sa.String(length=64), nullable=False),
        sa.Column("plan", sa.String(length=64), nullable=False),
        sa.Column("rate_limit_per_min", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("last_used_at", sa.DateTime(), nullable=True),
        sa.Column("revoked_at", sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("key_hash", name="uq_api_key_hash"),
    )
    op.create_index("ix_api_keys_tenant_id", "api_keys", ["tenant_id"], unique=False)

    op.create_table(
        "alerts",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("tenant_id", sa.String(length=64), nullable=False),
        sa.Column("alert_type", sa.String(length=32), nullable=False),
        sa.Column("market_id", sa.String(length=128), nullable=False),
        sa.Column("title", sa.String(length=512), nullable=False),
        sa.Column("category", sa.String(length=128), nullable=False),
        sa.Column("move", sa.Float(), nullable=False),
        sa.Column("market_p_yes", sa.Float(), nullable=False),
        sa.Column("prev_market_p_yes", sa.Float(), nullable=False),
        sa.Column("liquidity", sa.Float(), nullable=False),
        sa.Column("volume_24h", sa.Float(), nullable=False),
        sa.Column("snapshot_bucket", sa.DateTime(), nullable=False),
        sa.Column("source_ts", sa.DateTime(), nullable=True),
        sa.Column("message", sa.String(length=1024), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("alert_type", "market_id", "snapshot_bucket", name="uq_alert_market_bucket"),
    )
    op.create_index("ix_alerts_created_at", "alerts", ["created_at"], unique=False)
    op.create_index("ix_alerts_tenant_type", "alerts", ["tenant_id", "alert_type"], unique=False)


def downgrade() -> None:
    op.drop_index("ix_alerts_tenant_type", table_name="alerts")
    op.drop_index("ix_alerts_created_at", table_name="alerts")
    op.drop_table("alerts")
    op.drop_index("ix_api_keys_tenant_id", table_name="api_keys")
    op.drop_table("api_keys")

    op.drop_index("ix_market_snapshots_bucket", table_name="market_snapshots")
    op.drop_constraint("uq_market_bucket", "market_snapshots", type_="unique")
    op.create_unique_constraint("uq_market_asof", "market_snapshots", ["market_id", "asof_ts"])

    op.drop_column("market_snapshots", "last_trade_price")
    op.drop_column("market_snapshots", "best_ask")
    op.drop_column("market_snapshots", "volume_1w")
    op.drop_column("market_snapshots", "volume_24h")
    op.drop_column("market_snapshots", "snapshot_bucket")
    op.drop_column("market_snapshots", "source_ts")
