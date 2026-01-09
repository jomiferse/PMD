"""Add auth sessions and subscription tables.

Revision ID: 20260109_auth_billing
Revises: 20260106_telegram_linking
Create Date: 2026-01-09 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "20260109_auth_billing"
down_revision = "20260106_telegram_linking"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "user_auth",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("user_id", sa.dialects.postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("email", sa.String(length=320), nullable=False),
        sa.Column("password_hash", sa.String(length=256), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")),
    )
    op.create_unique_constraint("uq_user_auth_email", "user_auth", ["email"])
    op.create_index("ix_user_auth_email", "user_auth", ["email"])
    op.create_foreign_key(
        "fk_user_auth_user_id",
        "user_auth",
        "users",
        ["user_id"],
        ["user_id"],
        ondelete="CASCADE",
    )

    op.create_table(
        "user_sessions",
        sa.Column("token", sa.String(length=64), primary_key=True),
        sa.Column("user_id", sa.dialects.postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")),
        sa.Column("expires_at", sa.DateTime(), nullable=False),
        sa.Column("revoked_at", sa.DateTime(), nullable=True),
    )
    op.create_index("ix_user_sessions_user_id", "user_sessions", ["user_id"])
    op.create_index("ix_user_sessions_expires_at", "user_sessions", ["expires_at"])
    op.create_foreign_key(
        "fk_user_sessions_user_id",
        "user_sessions",
        "users",
        ["user_id"],
        ["user_id"],
        ondelete="CASCADE",
    )

    op.create_table(
        "subscriptions",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("user_id", sa.dialects.postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("plan_id", sa.Integer(), nullable=True),
        sa.Column("status", sa.String(length=32), nullable=False, server_default="incomplete"),
        sa.Column("current_period_end", sa.DateTime(), nullable=True),
        sa.Column("stripe_customer_id", sa.String(length=128), nullable=True),
        sa.Column("stripe_subscription_id", sa.String(length=128), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")),
    )
    op.create_unique_constraint(
        "uq_subscriptions_stripe_subscription_id",
        "subscriptions",
        ["stripe_subscription_id"],
    )
    op.create_index("ix_subscriptions_user_id", "subscriptions", ["user_id"])
    op.create_index("ix_subscriptions_customer_id", "subscriptions", ["stripe_customer_id"])
    op.create_foreign_key(
        "fk_subscriptions_user_id",
        "subscriptions",
        "users",
        ["user_id"],
        ["user_id"],
        ondelete="CASCADE",
    )
    op.create_foreign_key(
        "fk_subscriptions_plan_id",
        "subscriptions",
        "plans",
        ["plan_id"],
        ["id"],
    )

    op.create_table(
        "stripe_events",
        sa.Column("event_id", sa.String(length=128), primary_key=True),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")),
    )


def downgrade() -> None:
    op.drop_table("stripe_events")
    op.drop_constraint("fk_subscriptions_plan_id", "subscriptions", type_="foreignkey")
    op.drop_constraint("fk_subscriptions_user_id", "subscriptions", type_="foreignkey")
    op.drop_index("ix_subscriptions_customer_id", table_name="subscriptions")
    op.drop_index("ix_subscriptions_user_id", table_name="subscriptions")
    op.drop_constraint("uq_subscriptions_stripe_subscription_id", "subscriptions", type_="unique")
    op.drop_table("subscriptions")

    op.drop_constraint("fk_user_sessions_user_id", "user_sessions", type_="foreignkey")
    op.drop_index("ix_user_sessions_expires_at", table_name="user_sessions")
    op.drop_index("ix_user_sessions_user_id", table_name="user_sessions")
    op.drop_table("user_sessions")

    op.drop_constraint("fk_user_auth_user_id", "user_auth", type_="foreignkey")
    op.drop_index("ix_user_auth_email", table_name="user_auth")
    op.drop_constraint("uq_user_auth_email", "user_auth", type_="unique")
    op.drop_table("user_auth")
