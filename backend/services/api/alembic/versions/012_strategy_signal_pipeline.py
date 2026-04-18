"""Add strategy signal pipeline audit tables

Revision ID: 012
Revises: 011
Create Date: 2026-04-12

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "012"
down_revision: Union[str, None] = "011"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "strategy_runs",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("run_id", sa.Uuid(), nullable=False),
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("strategy_name", sa.String(length=128), nullable=False),
        sa.Column("symbol", sa.String(length=32), nullable=False),
        sa.Column("trading_mode", sa.String(length=16), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("trace_id", sa.String(length=64), nullable=False),
        sa.Column("metadata", sa.JSON(), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("run_id"),
    )
    op.create_index("ix_strategy_runs_run_id", "strategy_runs", ["run_id"])
    op.create_index("ix_strategy_runs_user_id", "strategy_runs", ["user_id"])
    op.create_index("ix_strategy_runs_strategy_name", "strategy_runs", ["strategy_name"])
    op.create_index("ix_strategy_runs_symbol", "strategy_runs", ["symbol"])
    op.create_index("ix_strategy_runs_trading_mode", "strategy_runs", ["trading_mode"])
    op.create_index("ix_strategy_runs_trace_id", "strategy_runs", ["trace_id"])

    op.create_table(
        "strategy_signals",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("signal_id", sa.Uuid(), nullable=False),
        sa.Column("run_id", sa.Uuid(), nullable=False),
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("strategy_name", sa.String(length=128), nullable=False),
        sa.Column("symbol", sa.String(length=32), nullable=False),
        sa.Column("action", sa.String(length=16), nullable=False),
        sa.Column("confidence", sa.Float(), nullable=False),
        sa.Column("suggested_size", sa.String(length=64), nullable=False),
        sa.Column("reasoning_metadata", sa.JSON(), nullable=False),
        sa.Column("trading_mode", sa.String(length=16), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("signal_id"),
    )
    op.create_index("ix_strategy_signals_signal_id", "strategy_signals", ["signal_id"])
    op.create_index("ix_strategy_signals_run_id", "strategy_signals", ["run_id"])
    op.create_index("ix_strategy_signals_user_id", "strategy_signals", ["user_id"])
    op.create_index("ix_strategy_signals_strategy_name", "strategy_signals", ["strategy_name"])
    op.create_index("ix_strategy_signals_symbol", "strategy_signals", ["symbol"])
    op.create_index("ix_strategy_signals_action", "strategy_signals", ["action"])
    op.create_index("ix_strategy_signals_trading_mode", "strategy_signals", ["trading_mode"])
    op.create_index("ix_strategy_signals_timestamp", "strategy_signals", ["timestamp"])


def downgrade() -> None:
    op.drop_table("strategy_signals")
    op.drop_table("strategy_runs")
