"""add strategic aggressive allocation strategy tables

Revision ID: 034_strategic_allocation
Revises: 033_ai_diagnostic_reviews
Create Date: 2026-05-15 00:00:00.000000
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Union

from alembic import op
import sqlalchemy as sa


revision: str = "034_strategic_allocation"
down_revision: Union[str, None] = "033_ai_diagnostic_reviews"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "strategic_aggressive_allocation_configs",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("strategy_id", sa.String(length=64), nullable=False),
        sa.Column("trading_mode", sa.String(length=16), nullable=False),
        sa.Column("is_enabled", sa.Boolean(), nullable=False, server_default="false"),
        sa.Column("total_allocated_amount_usd", sa.JSON(), nullable=False, server_default="{}"),
        sa.Column("bucket_allocations", sa.JSON(), nullable=False, server_default="[]"),
        sa.Column("selected_tokens", sa.JSON(), nullable=False, server_default="{}"),
        sa.Column("max_allocation_per_token", sa.JSON(), nullable=False, server_default="{}"),
        sa.Column("profit_taking_rules", sa.JSON(), nullable=False, server_default="{}"),
        sa.Column("stop_loss_rules", sa.JSON(), nullable=False, server_default="{}"),
        sa.Column("trailing_stop_rules", sa.JSON(), nullable=False, server_default="{}"),
        sa.Column("rebalance_settings", sa.JSON(), nullable=False, server_default="{}"),
        sa.Column("mode_settings", sa.JSON(), nullable=False, server_default="{}"),
        sa.Column("metadata", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "user_id",
            "strategy_id",
            "trading_mode",
            name="uq_strategic_aggressive_allocation_config",
        ),
    )
    op.create_index(
        "ix_strategic_aggressive_allocation_configs_user_id",
        "strategic_aggressive_allocation_configs",
        ["user_id"],
    )
    op.create_index(
        "ix_strategic_aggressive_allocation_configs_trading_mode",
        "strategic_aggressive_allocation_configs",
        ["trading_mode"],
    )

    op.create_table(
        "strategic_aggressive_allocation_profit_events",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("strategy_id", sa.String(length=64), nullable=False),
        sa.Column("trading_mode", sa.String(length=16), nullable=False),
        sa.Column("symbol", sa.String(length=32), nullable=False),
        sa.Column("bucket_id", sa.String(length=64), nullable=False),
        sa.Column("event_type", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("quantity", sa.String(length=64), nullable=False, server_default="0"),
        sa.Column("trigger_price", sa.String(length=64), nullable=True),
        sa.Column("realized_pnl_cents", sa.Integer(), nullable=True),
        sa.Column("signal_id", sa.Uuid(), nullable=True),
        sa.Column("correlation_id", sa.Uuid(), nullable=True),
        sa.Column("metadata", sa.JSON(), nullable=True),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_saa_profit_events_user_id",
        "strategic_aggressive_allocation_profit_events",
        ["user_id"],
    )
    op.create_index(
        "ix_saa_profit_events_trading_mode",
        "strategic_aggressive_allocation_profit_events",
        ["trading_mode"],
    )
    op.create_index(
        "ix_saa_profit_events_symbol",
        "strategic_aggressive_allocation_profit_events",
        ["symbol"],
    )
    op.create_index(
        "ix_saa_profit_events_bucket_id",
        "strategic_aggressive_allocation_profit_events",
        ["bucket_id"],
    )
    op.create_index(
        "ix_saa_profit_events_event_type",
        "strategic_aggressive_allocation_profit_events",
        ["event_type"],
    )
    op.create_index(
        "ix_saa_profit_events_signal_id",
        "strategic_aggressive_allocation_profit_events",
        ["signal_id"],
    )
    op.create_index(
        "ix_saa_profit_events_correlation_id",
        "strategic_aggressive_allocation_profit_events",
        ["correlation_id"],
    )
    op.create_index(
        "ix_saa_profit_events_occurred_at",
        "strategic_aggressive_allocation_profit_events",
        ["occurred_at"],
    )


def downgrade() -> None:
    op.drop_table("strategic_aggressive_allocation_profit_events")
    op.drop_table("strategic_aggressive_allocation_configs")
