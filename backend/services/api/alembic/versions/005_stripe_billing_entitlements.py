"""Stripe customers, subscriptions, entitlements, tenant trial columns

Revision ID: 005
Revises: 004
Create Date: 2026-04-12

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "005"
down_revision: Union[str, None] = "004"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "tenants",
        sa.Column("trial_started_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "tenants",
        sa.Column("trial_ends_at", sa.DateTime(timezone=True), nullable=True),
    )

    op.add_column(
        "subscription_plans",
        sa.Column(
            "plan_kind",
            sa.String(length=32),
            nullable=False,
            server_default="all_strategies",
        ),
    )

    op.create_table(
        "stripe_customers",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("stripe_customer_id", sa.String(length=255), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("tenant_id", name="uq_stripe_customers_tenant_id"),
        sa.UniqueConstraint("stripe_customer_id", name="uq_stripe_customers_stripe_id"),
    )
    op.create_index("ix_stripe_customers_stripe_id", "stripe_customers", ["stripe_customer_id"])

    op.create_table(
        "stripe_subscriptions",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("stripe_subscription_id", sa.String(length=255), nullable=False),
        sa.Column("stripe_customer_id", sa.String(length=255), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("subscription_plan_id", sa.Uuid(), nullable=True),
        sa.Column("primary_stripe_price_id", sa.String(length=255), nullable=True),
        sa.Column("current_period_start", sa.DateTime(timezone=True), nullable=True),
        sa.Column("current_period_end", sa.DateTime(timezone=True), nullable=True),
        sa.Column("cancel_at_period_end", sa.Boolean(), nullable=False, server_default="false"),
        sa.Column("metadata_json", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["subscription_plan_id"], ["subscription_plans.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("stripe_subscription_id", name="uq_stripe_subscriptions_stripe_id"),
    )
    op.create_index("ix_stripe_subscriptions_tenant", "stripe_subscriptions", ["tenant_id"])
    op.create_index("ix_stripe_subscriptions_status", "stripe_subscriptions", ["status"])

    op.create_table(
        "stripe_subscription_items",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("stripe_subscription_row_id", sa.Uuid(), nullable=False),
        sa.Column("stripe_subscription_item_id", sa.String(length=255), nullable=False),
        sa.Column("stripe_price_id", sa.String(length=255), nullable=False),
        sa.Column("platform_strategy_id", sa.Uuid(), nullable=True),
        sa.Column("quantity", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["platform_strategy_id"],
            ["platform_strategies.id"],
            ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(
            ["stripe_subscription_row_id"],
            ["stripe_subscriptions.id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "stripe_subscription_item_id",
            name="uq_stripe_subscription_items_stripe_item_id",
        ),
    )
    op.create_index(
        "ix_stripe_subscription_items_sub_row",
        "stripe_subscription_items",
        ["stripe_subscription_row_id"],
    )

    op.create_table(
        "tenant_entitlements",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("platform_strategy_id", sa.Uuid(), nullable=True),
        sa.Column("source", sa.String(length=32), nullable=False),
        sa.Column("valid_from", sa.DateTime(timezone=True), nullable=False),
        sa.Column("valid_until", sa.DateTime(timezone=True), nullable=True),
        sa.Column("stripe_subscription_id", sa.Uuid(), nullable=True),
        sa.Column("stripe_subscription_item_row_id", sa.Uuid(), nullable=True),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default="true"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["platform_strategy_id"],
            ["platform_strategies.id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["stripe_subscription_id"],
            ["stripe_subscriptions.id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["stripe_subscription_item_row_id"],
            ["stripe_subscription_items.id"],
            ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_tenant_entitlements_tenant", "tenant_entitlements", ["tenant_id"])
    op.create_index(
        "ix_tenant_entitlements_tenant_active",
        "tenant_entitlements",
        ["tenant_id", "is_active"],
    )

    op.create_table(
        "billing_checkout_sessions",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("subscription_plan_id", sa.Uuid(), nullable=False),
        sa.Column("stripe_checkout_session_id", sa.String(length=255), nullable=False),
        sa.Column("strategy_slugs", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["subscription_plan_id"],
            ["subscription_plans.id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "stripe_checkout_session_id",
            name="uq_billing_checkout_sessions_stripe_session",
        ),
    )

    op.alter_column(
        "platform_trial_policy",
        "trial_duration_days",
        server_default="30",
    )


def downgrade() -> None:
    op.drop_table("billing_checkout_sessions")
    op.drop_table("tenant_entitlements")
    op.drop_table("stripe_subscription_items")
    op.drop_table("stripe_subscriptions")
    op.drop_table("stripe_customers")

    op.drop_column("subscription_plans", "plan_kind")
    op.drop_column("tenants", "trial_ends_at")
    op.drop_column("tenants", "trial_started_at")

    op.alter_column(
        "platform_trial_policy",
        "trial_duration_days",
        server_default="14",
    )
