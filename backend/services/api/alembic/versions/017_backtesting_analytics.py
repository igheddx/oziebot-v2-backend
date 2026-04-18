"""Add backtesting run/trade/snapshot/analytics tables

Revision ID: 017
Revises: 016
Create Date: 2026-04-12

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "017"
down_revision: Union[str, None] = "016"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "backtest_runs",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("strategy_id", sa.String(length=128), nullable=False),
        sa.Column("trading_mode", sa.String(length=16), nullable=False),
        sa.Column("benchmark_mode", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("deterministic_fingerprint", sa.String(length=128), nullable=True),
        sa.Column("dataset_name", sa.String(length=128), nullable=False),
        sa.Column("timeframe", sa.String(length=32), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("params_json", sa.JSON(), nullable=False),
        sa.Column("summary_json", sa.JSON(), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "user_id", "deterministic_fingerprint", name="uq_backtest_run_user_fingerprint"
        ),
    )
    op.create_index("ix_backtest_runs_user_id", "backtest_runs", ["user_id"])
    op.create_index("ix_backtest_runs_tenant_id", "backtest_runs", ["tenant_id"])
    op.create_index("ix_backtest_runs_strategy_id", "backtest_runs", ["strategy_id"])
    op.create_index("ix_backtest_runs_trading_mode", "backtest_runs", ["trading_mode"])
    op.create_index(
        "ix_backtest_runs_deterministic_fingerprint", "backtest_runs", ["deterministic_fingerprint"]
    )
    op.create_index("ix_backtest_runs_started_at", "backtest_runs", ["started_at"])

    op.create_table(
        "backtest_trade_results",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("run_id", sa.Uuid(), nullable=False),
        sa.Column("symbol", sa.String(length=32), nullable=False),
        sa.Column("side", sa.String(length=16), nullable=False),
        sa.Column("entry_ts", sa.DateTime(timezone=True), nullable=False),
        sa.Column("exit_ts", sa.DateTime(timezone=True), nullable=False),
        sa.Column("quantity", sa.String(length=64), nullable=False),
        sa.Column("entry_price", sa.String(length=64), nullable=False),
        sa.Column("exit_price", sa.String(length=64), nullable=False),
        sa.Column("gross_return_bps", sa.Float(), nullable=False),
        sa.Column("net_return_bps", sa.Float(), nullable=False),
        sa.Column("fee_bps_total", sa.Float(), nullable=False),
        sa.Column("slippage_bps_total", sa.Float(), nullable=False),
        sa.Column("fee_impact_cents", sa.Integer(), nullable=False),
        sa.Column("slippage_impact_cents", sa.Integer(), nullable=False),
        sa.Column("pnl_cents", sa.Integer(), nullable=False),
        sa.Column("holding_seconds", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["run_id"], ["backtest_runs.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_backtest_trade_results_run_id", "backtest_trade_results", ["run_id"])
    op.create_index("ix_backtest_trade_results_symbol", "backtest_trade_results", ["symbol"])

    op.create_table(
        "backtest_performance_snapshots",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("run_id", sa.Uuid(), nullable=False),
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("strategy_id", sa.String(length=128), nullable=False),
        sa.Column("trading_mode", sa.String(length=16), nullable=False),
        sa.Column("token_symbol", sa.String(length=32), nullable=True),
        sa.Column("scope", sa.String(length=16), nullable=False),
        sa.Column("scope_key", sa.String(length=128), nullable=False),
        sa.Column("total_trades", sa.Integer(), nullable=False),
        sa.Column("win_rate", sa.Float(), nullable=False),
        sa.Column("avg_return_bps", sa.Float(), nullable=False),
        sa.Column("max_drawdown", sa.Float(), nullable=False),
        sa.Column("sharpe_like", sa.Float(), nullable=False),
        sa.Column("avg_slippage_bps", sa.Float(), nullable=False),
        sa.Column("fee_impact_cents", sa.Integer(), nullable=False),
        sa.Column("avg_holding_seconds", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["run_id"], ["backtest_runs.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_backtest_performance_snapshots_run_id", "backtest_performance_snapshots", ["run_id"]
    )
    op.create_index(
        "ix_backtest_performance_snapshots_user_id", "backtest_performance_snapshots", ["user_id"]
    )
    op.create_index(
        "ix_backtest_performance_snapshots_strategy_id",
        "backtest_performance_snapshots",
        ["strategy_id"],
    )
    op.create_index(
        "ix_backtest_performance_snapshots_trading_mode",
        "backtest_performance_snapshots",
        ["trading_mode"],
    )
    op.create_index(
        "ix_backtest_performance_snapshots_token_symbol",
        "backtest_performance_snapshots",
        ["token_symbol"],
    )
    op.create_index(
        "ix_backtest_performance_snapshots_scope", "backtest_performance_snapshots", ["scope"]
    )
    op.create_index(
        "ix_backtest_performance_snapshots_scope_key",
        "backtest_performance_snapshots",
        ["scope_key"],
    )
    op.create_index(
        "ix_backtest_performance_snapshots_created_at",
        "backtest_performance_snapshots",
        ["created_at"],
    )

    op.create_table(
        "strategy_analytics_artifacts",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("run_id", sa.Uuid(), nullable=False),
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("strategy_id", sa.String(length=128), nullable=False),
        sa.Column("trading_mode", sa.String(length=16), nullable=False),
        sa.Column("token_symbol", sa.String(length=32), nullable=True),
        sa.Column("feature_vector", sa.JSON(), nullable=False),
        sa.Column("labels", sa.JSON(), nullable=False),
        sa.Column("metadata", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["run_id"], ["backtest_runs.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_strategy_analytics_artifacts_run_id", "strategy_analytics_artifacts", ["run_id"]
    )
    op.create_index(
        "ix_strategy_analytics_artifacts_user_id", "strategy_analytics_artifacts", ["user_id"]
    )
    op.create_index(
        "ix_strategy_analytics_artifacts_strategy_id",
        "strategy_analytics_artifacts",
        ["strategy_id"],
    )
    op.create_index(
        "ix_strategy_analytics_artifacts_trading_mode",
        "strategy_analytics_artifacts",
        ["trading_mode"],
    )
    op.create_index(
        "ix_strategy_analytics_artifacts_token_symbol",
        "strategy_analytics_artifacts",
        ["token_symbol"],
    )
    op.create_index(
        "ix_strategy_analytics_artifacts_created_at", "strategy_analytics_artifacts", ["created_at"]
    )


def downgrade() -> None:
    op.drop_table("strategy_analytics_artifacts")
    op.drop_table("backtest_performance_snapshots")
    op.drop_table("backtest_trade_results")
    op.drop_table("backtest_runs")
