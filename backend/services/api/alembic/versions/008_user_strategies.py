"""Add user strategy configurations and signal logging tables

Revision ID: 008
Revises: 007
Create Date: 2026-04-12

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "008"
down_revision: Union[str, None] = "007"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Create user_strategies table
    op.create_table(
        "user_strategies",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("strategy_id", sa.String(length=64), nullable=False),
        sa.Column("is_enabled", sa.Boolean(), nullable=False, server_default="true"),
        sa.Column("config", sa.JSON(), nullable=False, server_default="{}"),
        sa.Column("metadata", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("user_id", "strategy_id", name="uq_user_strategy"),
    )
    op.create_index("ix_user_strategies_user_id", "user_strategies", ["user_id"])
    op.create_index("ix_user_strategies_strategy_id", "user_strategies", ["strategy_id"])

    # Create strategy_signal_logs table
    op.create_table(
        "strategy_signal_logs",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("strategy_id", sa.String(length=64), nullable=False),
        sa.Column("signal_id", sa.Uuid(), nullable=False),
        sa.Column("correlation_id", sa.Uuid(), nullable=False),
        sa.Column("trading_mode", sa.String(length=16), nullable=False),
        sa.Column("signal_type", sa.String(length=16), nullable=False),
        sa.Column("symbol", sa.String(length=64), nullable=True),
        sa.Column("confidence", sa.Float(), nullable=False, server_default="0.5"),
        sa.Column("reason", sa.String(length=512), nullable=False, server_default=""),
        sa.Column("signal_data", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("signal_id", name="uq_signal_id"),
    )
    op.create_index("ix_strategy_signal_logs_user_id", "strategy_signal_logs", ["user_id"])
    op.create_index("ix_strategy_signal_logs_strategy_id", "strategy_signal_logs", ["strategy_id"])
    op.create_index("ix_strategy_signal_logs_created_at", "strategy_signal_logs", ["created_at"])

    # Create strategy_performance table
    op.create_table(
        "strategy_performance",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("strategy_id", sa.String(length=64), nullable=False),
        sa.Column("trading_mode", sa.String(length=16), nullable=False),
        sa.Column("total_signals", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("buy_signals", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("sell_signals", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("hold_signals", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("close_signals", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("avg_confidence", sa.Float(), nullable=False, server_default="0.0"),
        sa.Column("last_signal_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("user_id", "strategy_id", "trading_mode", name="uq_user_strategy_mode"),
    )
    op.create_index("ix_strategy_performance_user_id", "strategy_performance", ["user_id"])


def downgrade() -> None:
    op.drop_table("strategy_performance")
    op.drop_table("strategy_signal_logs")
    op.drop_table("user_strategies")
