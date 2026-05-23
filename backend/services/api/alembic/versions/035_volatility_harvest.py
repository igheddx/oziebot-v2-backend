"""add volatility harvest strategy tables

Revision ID: 035_volatility_harvest
Revises: 034_strategic_allocation
Create Date: 2026-05-22 00:00:00.000000
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Union

from alembic import op
import sqlalchemy as sa


revision: str = "035_volatility_harvest"
down_revision: Union[str, None] = "034_strategic_allocation"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "volatility_harvest_config",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("strategy_id", sa.String(length=64), nullable=False),
        sa.Column("trading_mode", sa.String(length=16), nullable=False),
        sa.Column("is_enabled", sa.Boolean(), nullable=False, server_default="false"),
        sa.Column("total_allocated_amount_usd", sa.JSON(), nullable=False, server_default="{}"),
        sa.Column("selected_tokens", sa.JSON(), nullable=False, server_default="[]"),
        sa.Column(
            "core_position_percentage", sa.String(length=32), nullable=False, server_default="70"
        ),
        sa.Column(
            "trading_position_percentage", sa.String(length=32), nullable=False, server_default="30"
        ),
        sa.Column("entry_layers", sa.JSON(), nullable=False, server_default="[]"),
        sa.Column("harvest_bands", sa.JSON(), nullable=False, server_default="[]"),
        sa.Column("rebuy_bands", sa.JSON(), nullable=False, server_default="[]"),
        sa.Column("volatility_settings", sa.JSON(), nullable=False, server_default="{}"),
        sa.Column("risk_controls", sa.JSON(), nullable=False, server_default="{}"),
        sa.Column("fee_settings", sa.JSON(), nullable=False, server_default="{}"),
        sa.Column("mode_settings", sa.JSON(), nullable=False, server_default="{}"),
        sa.Column("admin_overrides", sa.JSON(), nullable=False, server_default="{}"),
        sa.Column("metadata", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "user_id",
            "strategy_id",
            "trading_mode",
            name="uq_volatility_harvest_config_scope",
        ),
    )
    op.create_index(
        "ix_volatility_harvest_config_user_id",
        "volatility_harvest_config",
        ["user_id"],
    )
    op.create_index(
        "ix_volatility_harvest_config_trading_mode",
        "volatility_harvest_config",
        ["trading_mode"],
    )

    op.create_table(
        "volatility_harvest_positions",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("strategy_id", sa.String(length=64), nullable=False),
        sa.Column("trading_mode", sa.String(length=16), nullable=False),
        sa.Column("symbol", sa.String(length=32), nullable=False),
        sa.Column("core_quantity", sa.String(length=64), nullable=False, server_default="0"),
        sa.Column("trading_quantity", sa.String(length=64), nullable=False, server_default="0"),
        sa.Column("avg_core_entry_price", sa.String(length=64), nullable=False, server_default="0"),
        sa.Column(
            "avg_trading_entry_price", sa.String(length=64), nullable=False, server_default="0"
        ),
        sa.Column("harvested_cash_cents", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("realized_gains_cents", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("unrealized_gains_cents", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("total_harvested_gains_cents", sa.Integer(), nullable=False, server_default="0"),
        sa.Column(
            "token_accumulation_quantity", sa.String(length=64), nullable=False, server_default="0"
        ),
        sa.Column(
            "token_accumulation_pct", sa.String(length=32), nullable=False, server_default="0"
        ),
        sa.Column("total_harvest_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("total_rebuy_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("last_local_high", sa.String(length=64), nullable=True),
        sa.Column("last_harvest_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_rebuy_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_action_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("metadata", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "user_id",
            "strategy_id",
            "trading_mode",
            "symbol",
            name="uq_volatility_harvest_position_scope",
        ),
    )
    op.create_index(
        "ix_volatility_harvest_positions_user_id",
        "volatility_harvest_positions",
        ["user_id"],
    )
    op.create_index(
        "ix_volatility_harvest_positions_trading_mode",
        "volatility_harvest_positions",
        ["trading_mode"],
    )
    op.create_index(
        "ix_volatility_harvest_positions_symbol",
        "volatility_harvest_positions",
        ["symbol"],
    )

    op.create_table(
        "volatility_harvest_transactions",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("strategy_id", sa.String(length=64), nullable=False),
        sa.Column("trading_mode", sa.String(length=16), nullable=False),
        sa.Column("symbol", sa.String(length=32), nullable=False),
        sa.Column("order_id", sa.Uuid(), nullable=True),
        sa.Column("transaction_type", sa.String(length=64), nullable=False),
        sa.Column("bucket_type", sa.String(length=32), nullable=True),
        sa.Column("band_code", sa.String(length=64), nullable=True),
        sa.Column("quantity", sa.String(length=64), nullable=False, server_default="0"),
        sa.Column("price", sa.String(length=64), nullable=False, server_default="0"),
        sa.Column("gross_notional_cents", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("fee_cents", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("slippage_cents", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("net_profit_cents", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("harvested_cash_balance_cents", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("token_quantity_after", sa.String(length=64), nullable=False, server_default="0"),
        sa.Column("signal_id", sa.Uuid(), nullable=True),
        sa.Column("correlation_id", sa.Uuid(), nullable=True),
        sa.Column("metadata", sa.JSON(), nullable=True),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("order_id", name="uq_volatility_harvest_transaction_order"),
    )
    op.create_index(
        "ix_volatility_harvest_transactions_user_id",
        "volatility_harvest_transactions",
        ["user_id"],
    )
    op.create_index(
        "ix_volatility_harvest_transactions_trading_mode",
        "volatility_harvest_transactions",
        ["trading_mode"],
    )
    op.create_index(
        "ix_volatility_harvest_transactions_symbol",
        "volatility_harvest_transactions",
        ["symbol"],
    )
    op.create_index(
        "ix_volatility_harvest_transactions_order_id",
        "volatility_harvest_transactions",
        ["order_id"],
    )
    op.create_index(
        "ix_volatility_harvest_transactions_transaction_type",
        "volatility_harvest_transactions",
        ["transaction_type"],
    )
    op.create_index(
        "ix_volatility_harvest_transactions_signal_id",
        "volatility_harvest_transactions",
        ["signal_id"],
    )
    op.create_index(
        "ix_volatility_harvest_transactions_correlation_id",
        "volatility_harvest_transactions",
        ["correlation_id"],
    )
    op.create_index(
        "ix_volatility_harvest_transactions_occurred_at",
        "volatility_harvest_transactions",
        ["occurred_at"],
    )

    op.create_table(
        "volatility_harvest_metrics",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("strategy_id", sa.String(length=64), nullable=False),
        sa.Column("trading_mode", sa.String(length=16), nullable=False),
        sa.Column("harvested_cash_cents", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("total_harvested_gains_cents", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("realized_gains_cents", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("unrealized_gains_cents", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("total_core_quantity", sa.String(length=64), nullable=False, server_default="0"),
        sa.Column(
            "total_trading_quantity", sa.String(length=64), nullable=False, server_default="0"
        ),
        sa.Column(
            "total_token_accumulation_quantity",
            sa.String(length=64),
            nullable=False,
            server_default="0",
        ),
        sa.Column(
            "total_token_accumulation_pct", sa.String(length=32), nullable=False, server_default="0"
        ),
        sa.Column("lifetime_harvest_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("lifetime_rebuy_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column(
            "avg_rebuy_efficiency_pct", sa.String(length=32), nullable=False, server_default="0"
        ),
        sa.Column("accumulation_history", sa.JSON(), nullable=False, server_default="[]"),
        sa.Column("metadata", sa.JSON(), nullable=True),
        sa.Column("last_synced_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "user_id",
            "strategy_id",
            "trading_mode",
            name="uq_volatility_harvest_metrics_scope",
        ),
    )
    op.create_index(
        "ix_volatility_harvest_metrics_user_id",
        "volatility_harvest_metrics",
        ["user_id"],
    )
    op.create_index(
        "ix_volatility_harvest_metrics_trading_mode",
        "volatility_harvest_metrics",
        ["trading_mode"],
    )


def downgrade() -> None:
    op.drop_table("volatility_harvest_metrics")
    op.drop_table("volatility_harvest_transactions")
    op.drop_table("volatility_harvest_positions")
    op.drop_table("volatility_harvest_config")
