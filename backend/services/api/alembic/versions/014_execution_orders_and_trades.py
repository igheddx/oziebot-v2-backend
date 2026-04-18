"""Add execution orders, fills, trades, and positions

Revision ID: 014
Revises: 013
Create Date: 2026-04-12

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "014"
down_revision: Union[str, None] = "013"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "execution_orders",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("intent_id", sa.Uuid(), nullable=False),
        sa.Column("correlation_id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("strategy_id", sa.String(length=128), nullable=False),
        sa.Column("symbol", sa.String(length=32), nullable=False),
        sa.Column("side", sa.String(length=16), nullable=False),
        sa.Column("order_type", sa.String(length=16), nullable=False),
        sa.Column("trading_mode", sa.String(length=16), nullable=False),
        sa.Column("venue", sa.String(length=32), nullable=False),
        sa.Column("state", sa.String(length=32), nullable=False),
        sa.Column("quantity", sa.String(length=64), nullable=False),
        sa.Column("requested_notional_cents", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("reserved_cash_cents", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("locked_cash_cents", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("filled_quantity", sa.String(length=64), nullable=False, server_default="0"),
        sa.Column("avg_fill_price", sa.String(length=64), nullable=True),
        sa.Column("fees_cents", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("idempotency_key", sa.String(length=128), nullable=False),
        sa.Column("client_order_id", sa.String(length=128), nullable=False),
        sa.Column("venue_order_id", sa.String(length=128), nullable=True),
        sa.Column("failure_code", sa.String(length=64), nullable=True),
        sa.Column("failure_detail", sa.String(length=512), nullable=True),
        sa.Column("trace_id", sa.String(length=64), nullable=False),
        sa.Column("intent_payload", sa.JSON(), nullable=False),
        sa.Column("risk_payload", sa.JSON(), nullable=False),
        sa.Column("adapter_payload", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("submitted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("cancelled_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("failed_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("client_order_id", name="uq_execution_order_client_order_id"),
        sa.UniqueConstraint("idempotency_key", name="uq_execution_order_idempotency_key"),
        sa.UniqueConstraint("intent_id", "trading_mode", name="uq_execution_order_intent_mode"),
    )
    for name, cols in (
        ("ix_execution_orders_intent_id", ["intent_id"]),
        ("ix_execution_orders_tenant_id", ["tenant_id"]),
        ("ix_execution_orders_user_id", ["user_id"]),
        ("ix_execution_orders_strategy_id", ["strategy_id"]),
        ("ix_execution_orders_symbol", ["symbol"]),
        ("ix_execution_orders_trading_mode", ["trading_mode"]),
        ("ix_execution_orders_venue", ["venue"]),
        ("ix_execution_orders_state", ["state"]),
        ("ix_execution_orders_trace_id", ["trace_id"]),
        ("ix_execution_orders_venue_order_id", ["venue_order_id"]),
        ("ix_execution_orders_created_at", ["created_at"]),
    ):
        op.create_index(name, "execution_orders", cols)

    op.create_table(
        "execution_fills",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("order_id", sa.Uuid(), nullable=False),
        sa.Column("venue_fill_id", sa.String(length=128), nullable=False),
        sa.Column("fill_sequence", sa.Integer(), nullable=False),
        sa.Column("quantity", sa.String(length=64), nullable=False),
        sa.Column("price", sa.String(length=64), nullable=False),
        sa.Column("gross_notional_cents", sa.Integer(), nullable=False),
        sa.Column("fee_cents", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("liquidity", sa.String(length=32), nullable=True),
        sa.Column("raw_payload", sa.JSON(), nullable=True),
        sa.Column("filled_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["order_id"], ["execution_orders.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("order_id", "venue_fill_id", name="uq_execution_fill_order_venue_fill"),
    )
    op.create_index("ix_execution_fills_order_id", "execution_fills", ["order_id"])
    op.create_index("ix_execution_fills_filled_at", "execution_fills", ["filled_at"])

    op.create_table(
        "execution_trades",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("order_id", sa.Uuid(), nullable=False),
        sa.Column("fill_id", sa.Uuid(), nullable=True),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("strategy_id", sa.String(length=128), nullable=False),
        sa.Column("symbol", sa.String(length=32), nullable=False),
        sa.Column("trading_mode", sa.String(length=16), nullable=False),
        sa.Column("side", sa.String(length=16), nullable=False),
        sa.Column("quantity", sa.String(length=64), nullable=False),
        sa.Column("price", sa.String(length=64), nullable=False),
        sa.Column("gross_notional_cents", sa.Integer(), nullable=False),
        sa.Column("fee_cents", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("realized_pnl_cents", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("position_quantity_after", sa.String(length=64), nullable=False),
        sa.Column("avg_entry_price_after", sa.String(length=64), nullable=False),
        sa.Column("executed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("raw_payload", sa.JSON(), nullable=True),
        sa.ForeignKeyConstraint(["fill_id"], ["execution_fills.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["order_id"], ["execution_orders.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    for name, cols in (
        ("ix_execution_trades_order_id", ["order_id"]),
        ("ix_execution_trades_fill_id", ["fill_id"]),
        ("ix_execution_trades_tenant_id", ["tenant_id"]),
        ("ix_execution_trades_user_id", ["user_id"]),
        ("ix_execution_trades_strategy_id", ["strategy_id"]),
        ("ix_execution_trades_symbol", ["symbol"]),
        ("ix_execution_trades_trading_mode", ["trading_mode"]),
        ("ix_execution_trades_executed_at", ["executed_at"]),
    ):
        op.create_index(name, "execution_trades", cols)

    op.create_table(
        "execution_positions",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("strategy_id", sa.String(length=128), nullable=False),
        sa.Column("symbol", sa.String(length=32), nullable=False),
        sa.Column("trading_mode", sa.String(length=16), nullable=False),
        sa.Column("quantity", sa.String(length=64), nullable=False, server_default="0"),
        sa.Column("avg_entry_price", sa.String(length=64), nullable=False, server_default="0"),
        sa.Column("realized_pnl_cents", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_trade_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "tenant_id",
            "user_id",
            "strategy_id",
            "symbol",
            "trading_mode",
            name="uq_execution_position_scope",
        ),
    )
    for name, cols in (
        ("ix_execution_positions_tenant_id", ["tenant_id"]),
        ("ix_execution_positions_user_id", ["user_id"]),
        ("ix_execution_positions_strategy_id", ["strategy_id"]),
        ("ix_execution_positions_symbol", ["symbol"]),
        ("ix_execution_positions_trading_mode", ["trading_mode"]),
    ):
        op.create_index(name, "execution_positions", cols)


def downgrade() -> None:
    op.drop_table("execution_positions")
    op.drop_table("execution_trades")
    op.drop_table("execution_fills")
    op.drop_table("execution_orders")
