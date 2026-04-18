"""Add strategy capital allocation and bucket accounting

Revision ID: 010
Revises: 009
Create Date: 2026-04-12

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "010"
down_revision: Union[str, None] = "009"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "strategy_allocation_plans",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("trading_mode", sa.String(length=16), nullable=False),
        sa.Column("allocation_mode", sa.String(length=16), nullable=False),
        sa.Column("preset_name", sa.String(length=32), nullable=True),
        sa.Column("total_capital_cents", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "user_id", "trading_mode", name="uq_strategy_allocation_plan_user_mode"
        ),
    )
    op.create_index(
        "ix_strategy_allocation_plans_user_id", "strategy_allocation_plans", ["user_id"]
    )
    op.create_index(
        "ix_strategy_allocation_plans_trading_mode", "strategy_allocation_plans", ["trading_mode"]
    )

    op.create_table(
        "strategy_allocation_items",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("plan_id", sa.Uuid(), nullable=False),
        sa.Column("strategy_id", sa.String(length=64), nullable=False),
        sa.Column("allocation_bps", sa.Integer(), nullable=False),
        sa.Column("assigned_capital_cents", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["plan_id"], ["strategy_allocation_plans.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "plan_id", "strategy_id", name="uq_strategy_allocation_item_plan_strategy"
        ),
    )
    op.create_index(
        "ix_strategy_allocation_items_plan_id", "strategy_allocation_items", ["plan_id"]
    )
    op.create_index(
        "ix_strategy_allocation_items_strategy_id", "strategy_allocation_items", ["strategy_id"]
    )

    op.create_table(
        "strategy_capital_buckets",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("strategy_id", sa.String(length=64), nullable=False),
        sa.Column("trading_mode", sa.String(length=16), nullable=False),
        sa.Column("assigned_capital_cents", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("available_cash_cents", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("reserved_cash_cents", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("locked_capital_cents", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("realized_pnl_cents", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("unrealized_pnl_cents", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("available_buying_power_cents", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("version", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "user_id", "strategy_id", "trading_mode", name="uq_strategy_bucket_user_strategy_mode"
        ),
    )
    op.create_index("ix_strategy_capital_buckets_user_id", "strategy_capital_buckets", ["user_id"])
    op.create_index(
        "ix_strategy_capital_buckets_strategy_id", "strategy_capital_buckets", ["strategy_id"]
    )
    op.create_index(
        "ix_strategy_capital_buckets_trading_mode", "strategy_capital_buckets", ["trading_mode"]
    )

    op.create_table(
        "strategy_capital_ledger",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("strategy_id", sa.String(length=64), nullable=False),
        sa.Column("trading_mode", sa.String(length=16), nullable=False),
        sa.Column("event_type", sa.String(length=32), nullable=False),
        sa.Column("amount_cents", sa.Integer(), nullable=False),
        sa.Column("before_available_cash_cents", sa.Integer(), nullable=False),
        sa.Column("after_available_cash_cents", sa.Integer(), nullable=False),
        sa.Column("before_reserved_cash_cents", sa.Integer(), nullable=False),
        sa.Column("after_reserved_cash_cents", sa.Integer(), nullable=False),
        sa.Column("before_locked_capital_cents", sa.Integer(), nullable=False),
        sa.Column("after_locked_capital_cents", sa.Integer(), nullable=False),
        sa.Column("before_realized_pnl_cents", sa.Integer(), nullable=False),
        sa.Column("after_realized_pnl_cents", sa.Integer(), nullable=False),
        sa.Column("before_unrealized_pnl_cents", sa.Integer(), nullable=False),
        sa.Column("after_unrealized_pnl_cents", sa.Integer(), nullable=False),
        sa.Column("reference_id", sa.String(length=128), nullable=True),
        sa.Column("metadata", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_strategy_capital_ledger_user_id", "strategy_capital_ledger", ["user_id"])
    op.create_index(
        "ix_strategy_capital_ledger_strategy_id", "strategy_capital_ledger", ["strategy_id"]
    )
    op.create_index(
        "ix_strategy_capital_ledger_trading_mode", "strategy_capital_ledger", ["trading_mode"]
    )
    op.create_index(
        "ix_strategy_capital_ledger_event_type", "strategy_capital_ledger", ["event_type"]
    )
    op.create_index(
        "ix_strategy_capital_ledger_reference_id", "strategy_capital_ledger", ["reference_id"]
    )
    op.create_index(
        "ix_strategy_capital_ledger_created_at", "strategy_capital_ledger", ["created_at"]
    )


def downgrade() -> None:
    op.drop_table("strategy_capital_ledger")
    op.drop_table("strategy_capital_buckets")
    op.drop_table("strategy_allocation_items")
    op.drop_table("strategy_allocation_plans")
