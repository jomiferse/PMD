"""add alert price fields and snapshot indexes

Revision ID: c3d4e5f6a7b8
Revises: b1f2c3d4e5f6
Create Date: 2026-01-02 21:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "c3d4e5f6a7b8"
down_revision: Union[str, None] = "b1f2c3d4e5f6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("alerts", sa.Column("old_price", sa.Float(), server_default="0", nullable=False))
    op.add_column("alerts", sa.Column("new_price", sa.Float(), server_default="0", nullable=False))
    op.add_column("alerts", sa.Column("delta_pct", sa.Float(), server_default="0", nullable=False))
    op.add_column("alerts", sa.Column("triggered_at", sa.DateTime(), nullable=True))

    op.execute(
        """
        UPDATE alerts
        SET old_price = prev_market_p_yes,
            new_price = market_p_yes,
            delta_pct = CASE
                WHEN prev_market_p_yes > 0 THEN ABS(market_p_yes - prev_market_p_yes) / prev_market_p_yes
                ELSE 0
            END,
            triggered_at = created_at
        """
    )
    op.execute("UPDATE alerts SET alert_type = 'DISLOCATION' WHERE alert_type = 'dislocation'")
    op.alter_column("alerts", "triggered_at", nullable=False)

    op.create_index("ix_alerts_market_triggered", "alerts", ["market_id", "triggered_at"], unique=False)

    op.create_index("ix_market_snapshots_market_asof", "market_snapshots", ["market_id", "asof_ts"], unique=False)
    op.execute("CREATE INDEX ix_market_snapshots_asof_desc ON market_snapshots (asof_ts DESC)")


def downgrade() -> None:
    op.execute("DROP INDEX ix_market_snapshots_asof_desc")
    op.drop_index("ix_market_snapshots_market_asof", table_name="market_snapshots")

    op.drop_index("ix_alerts_market_triggered", table_name="alerts")
    op.drop_column("alerts", "triggered_at")
    op.drop_column("alerts", "delta_pct")
    op.drop_column("alerts", "new_price")
    op.drop_column("alerts", "old_price")
