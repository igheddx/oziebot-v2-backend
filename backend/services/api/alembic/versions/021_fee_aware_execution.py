"""Add fee-aware execution columns.

Revision ID: 021_fee_aware_execution
Revises: 020_strategy_baseline_config
Create Date: 2026-04-18
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "021_fee_aware_execution"
down_revision = "020_strategy_baseline_config"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "execution_orders",
        sa.Column("expected_gross_edge_bps", sa.Integer(), nullable=False, server_default="0"),
    )
    op.add_column(
        "execution_orders",
        sa.Column("estimated_fee_bps", sa.Integer(), nullable=False, server_default="0"),
    )
    op.add_column(
        "execution_orders",
        sa.Column("estimated_slippage_bps", sa.Integer(), nullable=False, server_default="0"),
    )
    op.add_column(
        "execution_orders",
        sa.Column("estimated_total_cost_bps", sa.Integer(), nullable=False, server_default="0"),
    )
    op.add_column(
        "execution_orders",
        sa.Column("expected_net_edge_bps", sa.Integer(), nullable=False, server_default="0"),
    )
    op.add_column(
        "execution_orders",
        sa.Column(
            "execution_preference",
            sa.String(length=32),
            nullable=False,
            server_default="maker_preferred",
        ),
    )
    op.add_column(
        "execution_orders",
        sa.Column(
            "fallback_behavior",
            sa.String(length=32),
            nullable=False,
            server_default="convert_to_taker",
        ),
    )
    op.add_column(
        "execution_orders",
        sa.Column("maker_timeout_seconds", sa.Integer(), nullable=False, server_default="0"),
    )
    op.add_column(
        "execution_orders",
        sa.Column("limit_price_offset_bps", sa.Integer(), nullable=False, server_default="0"),
    )
    op.add_column(
        "execution_orders",
        sa.Column("actual_fill_type", sa.String(length=32), nullable=True),
    )
    op.add_column(
        "execution_orders",
        sa.Column(
            "fallback_triggered",
            sa.Boolean(),
            nullable=False,
            server_default=sa.false(),
        ),
    )


def downgrade() -> None:
    op.drop_column("execution_orders", "fallback_triggered")
    op.drop_column("execution_orders", "actual_fill_type")
    op.drop_column("execution_orders", "limit_price_offset_bps")
    op.drop_column("execution_orders", "maker_timeout_seconds")
    op.drop_column("execution_orders", "fallback_behavior")
    op.drop_column("execution_orders", "execution_preference")
    op.drop_column("execution_orders", "expected_net_edge_bps")
    op.drop_column("execution_orders", "estimated_total_cost_bps")
    op.drop_column("execution_orders", "estimated_slippage_bps")
    op.drop_column("execution_orders", "estimated_fee_bps")
    op.drop_column("execution_orders", "expected_gross_edge_bps")
