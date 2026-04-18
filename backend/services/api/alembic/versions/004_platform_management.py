"""platform settings, token allowlist, strategy catalog, plans, trial policy, audit logs

Revision ID: 004
Revises: 003
Create Date: 2026-04-12

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "004"
down_revision: Union[str, None] = "003"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "tenant_integrations",
        sa.Column("coinbase_last_check_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "tenant_integrations",
        sa.Column("coinbase_health_status", sa.String(length=32), nullable=True),
    )
    op.add_column(
        "tenant_integrations",
        sa.Column("coinbase_last_error", sa.Text(), nullable=True),
    )

    op.create_table(
        "platform_settings",
        sa.Column("key", sa.String(length=128), nullable=False),
        sa.Column("value", sa.JSON(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_by_user_id", sa.Uuid(), nullable=True),
        sa.ForeignKeyConstraint(["updated_by_user_id"], ["users.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("key"),
    )

    op.create_table(
        "platform_token_allowlist",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("symbol", sa.String(length=64), nullable=False),
        sa.Column("quote_currency", sa.String(length=16), nullable=False, server_default="USD"),
        sa.Column("network", sa.String(length=64), nullable=False, server_default="mainnet"),
        sa.Column("contract_address", sa.String(length=128), nullable=True),
        sa.Column("display_name", sa.String(length=256), nullable=True),
        sa.Column("is_enabled", sa.Boolean(), nullable=False, server_default="true"),
        sa.Column("sort_order", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("extra", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_platform_token_allowlist_symbol", "platform_token_allowlist", ["symbol"])
    op.create_index(
        "ix_platform_token_allowlist_enabled",
        "platform_token_allowlist",
        ["is_enabled"],
    )

    op.create_table(
        "platform_strategies",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("slug", sa.String(length=64), nullable=False),
        sa.Column("display_name", sa.String(length=256), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("is_enabled", sa.Boolean(), nullable=False, server_default="true"),
        sa.Column("entry_point", sa.String(length=256), nullable=True),
        sa.Column("config_schema", sa.JSON(), nullable=True),
        sa.Column("sort_order", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("slug", name="uq_platform_strategies_slug"),
    )

    op.create_table(
        "subscription_plans",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("slug", sa.String(length=64), nullable=False),
        sa.Column("display_name", sa.String(length=256), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("stripe_price_id", sa.String(length=255), nullable=False),
        sa.Column("stripe_product_id", sa.String(length=255), nullable=True),
        sa.Column("billing_interval", sa.String(length=16), nullable=False),
        sa.Column("amount_cents", sa.Integer(), nullable=False),
        sa.Column("currency", sa.String(length=8), nullable=False, server_default="usd"),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default="true"),
        sa.Column("features", sa.JSON(), nullable=True),
        sa.Column("trial_days_override", sa.Integer(), nullable=True),
        sa.Column("sort_order", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("slug", name="uq_subscription_plans_slug"),
    )
    op.create_index("ix_subscription_plans_stripe_price", "subscription_plans", ["stripe_price_id"])

    op.create_table(
        "platform_trial_policy",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("is_enabled", sa.Boolean(), nullable=False, server_default="true"),
        sa.Column("trial_duration_days", sa.Integer(), nullable=False, server_default="14"),
        sa.Column("max_trials_per_tenant", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("grace_period_days", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("metadata", sa.JSON(), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_by_user_id", sa.Uuid(), nullable=True),
        sa.ForeignKeyConstraint(["updated_by_user_id"], ["users.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
    )

    op.create_table(
        "admin_audit_logs",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("actor_user_id", sa.Uuid(), nullable=False),
        sa.Column("action", sa.String(length=128), nullable=False),
        sa.Column("resource_type", sa.String(length=64), nullable=False),
        sa.Column("resource_id", sa.String(length=128), nullable=True),
        sa.Column("details", sa.JSON(), nullable=True),
        sa.Column("ip_address", sa.String(length=64), nullable=True),
        sa.Column("user_agent", sa.String(length=512), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["actor_user_id"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_admin_audit_logs_created_at", "admin_audit_logs", ["created_at"])
    op.create_index("ix_admin_audit_logs_actor", "admin_audit_logs", ["actor_user_id"])


def downgrade() -> None:
    op.drop_table("admin_audit_logs")
    op.drop_table("platform_trial_policy")
    op.drop_table("subscription_plans")
    op.drop_table("platform_strategies")
    op.drop_table("platform_token_allowlist")
    op.drop_table("platform_settings")
    op.drop_column("tenant_integrations", "coinbase_last_error")
    op.drop_column("tenant_integrations", "coinbase_health_status")
    op.drop_column("tenant_integrations", "coinbase_last_check_at")
