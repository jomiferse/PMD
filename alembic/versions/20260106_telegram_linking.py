"""Add pending telegram chats and bigint chat IDs.

Revision ID: 20260106_telegram_linking
Revises: 20260105_baseline
Create Date: 2026-01-06 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "20260106_telegram_linking"
down_revision = "20260105_baseline"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "pending_telegram_chats",
        sa.Column("telegram_chat_id", sa.BigInteger(), primary_key=True),
        sa.Column("first_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False, server_default=sa.text("'pending'")),
    )

    bind = op.get_bind()
    dialect = bind.dialect.name if bind is not None else None
    if dialect == "postgresql":
        op.alter_column(
            "users",
            "telegram_chat_id",
            existing_type=sa.String(length=64),
            type_=sa.BigInteger(),
            postgresql_using="telegram_chat_id::bigint",
            existing_nullable=True,
        )
    else:
        op.alter_column(
            "users",
            "telegram_chat_id",
            existing_type=sa.String(length=64),
            type_=sa.BigInteger(),
            existing_nullable=True,
        )
    op.create_unique_constraint("uq_users_telegram_chat_id", "users", ["telegram_chat_id"])


def downgrade() -> None:
    op.drop_constraint("uq_users_telegram_chat_id", "users", type_="unique")
    op.alter_column(
        "users",
        "telegram_chat_id",
        existing_type=sa.BigInteger(),
        type_=sa.String(length=64),
        existing_nullable=True,
    )
    op.drop_table("pending_telegram_chats")
