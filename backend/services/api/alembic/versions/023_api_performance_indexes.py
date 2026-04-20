"""Add composite indexes for dashboard and analytics hot paths.

Revision ID: 023_api_performance_indexes
Revises: 022_trade_intelligence_ai_ready
Create Date: 2026-04-20
"""

from __future__ import annotations

from alembic import op


revision = "023_api_performance_indexes"
down_revision = "022_trade_intelligence_ai_ready"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_index(
        "ix_strategy_runs_user_mode_completed_at",
        "strategy_runs",
        ["user_id", "trading_mode", "completed_at"],
    )
    op.create_index(
        "ix_strategy_runs_user_mode_started_at",
        "strategy_runs",
        ["user_id", "trading_mode", "started_at"],
    )
    op.create_index(
        "ix_strategy_signals_user_mode_timestamp",
        "strategy_signals",
        ["user_id", "trading_mode", "timestamp"],
    )
    op.create_index(
        "ix_risk_events_user_mode_created_at",
        "risk_events",
        ["user_id", "trading_mode", "created_at"],
    )
    op.create_index(
        "ix_execution_orders_user_mode_created_at",
        "execution_orders",
        ["user_id", "trading_mode", "created_at"],
    )
    op.create_index(
        "ix_execution_orders_user_mode_state_created_at",
        "execution_orders",
        ["user_id", "trading_mode", "state", "created_at"],
    )
    op.create_index(
        "ix_execution_trades_user_mode_executed_at",
        "execution_trades",
        ["user_id", "trading_mode", "executed_at"],
    )
    op.create_index(
        "ix_execution_positions_user_mode_updated_at",
        "execution_positions",
        ["user_id", "trading_mode", "updated_at"],
    )
    op.create_index(
        "ix_strategy_signal_snapshots_user_mode_timestamp",
        "strategy_signal_snapshots",
        ["user_id", "trading_mode", "timestamp"],
    )
    op.create_index(
        "ix_strategy_decision_audits_decision_stage_created_at",
        "strategy_decision_audits",
        ["decision", "stage", "created_at"],
    )
    op.create_index(
        "ix_trade_outcome_features_mode_created_at",
        "trade_outcome_features",
        ["trading_mode", "created_at"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_trade_outcome_features_mode_created_at",
        table_name="trade_outcome_features",
    )
    op.drop_index(
        "ix_strategy_decision_audits_decision_stage_created_at",
        table_name="strategy_decision_audits",
    )
    op.drop_index(
        "ix_strategy_signal_snapshots_user_mode_timestamp",
        table_name="strategy_signal_snapshots",
    )
    op.drop_index(
        "ix_execution_positions_user_mode_updated_at",
        table_name="execution_positions",
    )
    op.drop_index(
        "ix_execution_trades_user_mode_executed_at",
        table_name="execution_trades",
    )
    op.drop_index(
        "ix_execution_orders_user_mode_state_created_at",
        table_name="execution_orders",
    )
    op.drop_index(
        "ix_execution_orders_user_mode_created_at",
        table_name="execution_orders",
    )
    op.drop_index(
        "ix_risk_events_user_mode_created_at",
        table_name="risk_events",
    )
    op.drop_index(
        "ix_strategy_signals_user_mode_timestamp",
        table_name="strategy_signals",
    )
    op.drop_index(
        "ix_strategy_runs_user_mode_started_at",
        table_name="strategy_runs",
    )
    op.drop_index(
        "ix_strategy_runs_user_mode_completed_at",
        table_name="strategy_runs",
    )
