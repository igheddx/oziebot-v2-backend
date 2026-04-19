"""Add trade intelligence and AI readiness tables.

Revision ID: 022_trade_intelligence_ai_ready
Revises: 021_fee_aware_execution
Create Date: 2026-04-19
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "022_trade_intelligence_ai_ready"
down_revision = "021_fee_aware_execution"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "strategy_signal_snapshots",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("trading_mode", sa.String(length=16), nullable=False),
        sa.Column("strategy_name", sa.String(length=128), nullable=False),
        sa.Column("token_symbol", sa.String(length=32), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("current_price", sa.Numeric(28, 10), nullable=False),
        sa.Column("best_bid", sa.Numeric(28, 10), nullable=False),
        sa.Column("best_ask", sa.Numeric(28, 10), nullable=False),
        sa.Column("spread_pct", sa.Numeric(18, 10), nullable=False),
        sa.Column("estimated_slippage_pct", sa.Numeric(18, 10), nullable=False),
        sa.Column("volume", sa.Numeric(28, 10), nullable=False),
        sa.Column("volatility", sa.Numeric(18, 10), nullable=True),
        sa.Column("confidence_score", sa.Float(), nullable=True),
        sa.Column("raw_feature_json", sa.JSON(), nullable=False),
        sa.Column("token_policy_status", sa.String(length=32), nullable=True),
        sa.Column("token_policy_multiplier", sa.Numeric(18, 10), nullable=True),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_strategy_signal_snapshots_user_id",
        "strategy_signal_snapshots",
        ["user_id"],
    )
    op.create_index(
        "ix_strategy_signal_snapshots_tenant_id",
        "strategy_signal_snapshots",
        ["tenant_id"],
    )
    op.create_index(
        "ix_strategy_signal_snapshots_trading_mode",
        "strategy_signal_snapshots",
        ["trading_mode"],
    )
    op.create_index(
        "ix_strategy_signal_snapshots_strategy_name",
        "strategy_signal_snapshots",
        ["strategy_name"],
    )
    op.create_index(
        "ix_strategy_signal_snapshots_token_symbol",
        "strategy_signal_snapshots",
        ["token_symbol"],
    )
    op.create_index(
        "ix_strategy_signal_snapshots_timestamp",
        "strategy_signal_snapshots",
        ["timestamp"],
    )
    op.create_index(
        "ix_strategy_signal_snapshots_token_policy_status",
        "strategy_signal_snapshots",
        ["token_policy_status"],
    )

    op.create_table(
        "strategy_decision_audits",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("signal_snapshot_id", sa.Uuid(), nullable=True),
        sa.Column("stage", sa.String(length=32), nullable=False),
        sa.Column("decision", sa.String(length=32), nullable=False),
        sa.Column("reason_code", sa.String(length=128), nullable=True),
        sa.Column("reason_detail", sa.String(length=512), nullable=True),
        sa.Column("size_before", sa.Numeric(28, 10), nullable=True),
        sa.Column("size_after", sa.Numeric(28, 10), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["signal_snapshot_id"],
            ["strategy_signal_snapshots.id"],
            ondelete="SET NULL",
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_strategy_decision_audits_signal_snapshot_id",
        "strategy_decision_audits",
        ["signal_snapshot_id"],
    )
    op.create_index(
        "ix_strategy_decision_audits_stage",
        "strategy_decision_audits",
        ["stage"],
    )
    op.create_index(
        "ix_strategy_decision_audits_decision",
        "strategy_decision_audits",
        ["decision"],
    )
    op.create_index(
        "ix_strategy_decision_audits_reason_code",
        "strategy_decision_audits",
        ["reason_code"],
    )
    op.create_index(
        "ix_strategy_decision_audits_created_at",
        "strategy_decision_audits",
        ["created_at"],
    )

    op.create_table(
        "trade_outcome_features",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("trade_id", sa.Uuid(), nullable=False),
        sa.Column("signal_snapshot_id", sa.Uuid(), nullable=True),
        sa.Column("trading_mode", sa.String(length=16), nullable=False),
        sa.Column("strategy_name", sa.String(length=128), nullable=False),
        sa.Column("token_symbol", sa.String(length=32), nullable=False),
        sa.Column("entry_price", sa.Numeric(28, 10), nullable=False),
        sa.Column("exit_price", sa.Numeric(28, 10), nullable=False),
        sa.Column("filled_size", sa.Numeric(28, 10), nullable=False),
        sa.Column("fee_paid", sa.Numeric(28, 10), nullable=False),
        sa.Column("slippage_realized", sa.Numeric(18, 10), nullable=True),
        sa.Column("hold_seconds", sa.Integer(), nullable=True),
        sa.Column("realized_pnl", sa.Numeric(28, 10), nullable=False),
        sa.Column("realized_return_pct", sa.Numeric(18, 10), nullable=True),
        sa.Column("max_favorable_excursion_pct", sa.Numeric(18, 10), nullable=True),
        sa.Column("max_adverse_excursion_pct", sa.Numeric(18, 10), nullable=True),
        sa.Column("exit_reason", sa.String(length=128), nullable=True),
        sa.Column("win_loss_label", sa.String(length=16), nullable=False),
        sa.Column("profitable_after_fees_label", sa.String(length=16), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["trade_id"], ["execution_trades.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(
            ["signal_snapshot_id"],
            ["strategy_signal_snapshots.id"],
            ondelete="SET NULL",
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_trade_outcome_features_trade_id",
        "trade_outcome_features",
        ["trade_id"],
    )
    op.create_index(
        "ix_trade_outcome_features_signal_snapshot_id",
        "trade_outcome_features",
        ["signal_snapshot_id"],
    )
    op.create_index(
        "ix_trade_outcome_features_trading_mode",
        "trade_outcome_features",
        ["trading_mode"],
    )
    op.create_index(
        "ix_trade_outcome_features_strategy_name",
        "trade_outcome_features",
        ["strategy_name"],
    )
    op.create_index(
        "ix_trade_outcome_features_token_symbol",
        "trade_outcome_features",
        ["token_symbol"],
    )
    op.create_index(
        "ix_trade_outcome_features_win_loss_label",
        "trade_outcome_features",
        ["win_loss_label"],
    )
    op.create_index(
        "ix_trade_outcome_features_profitable_after_fees_label",
        "trade_outcome_features",
        ["profitable_after_fees_label"],
    )
    op.create_index(
        "ix_trade_outcome_features_created_at",
        "trade_outcome_features",
        ["created_at"],
    )

    op.create_table(
        "ai_inference_records",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("signal_snapshot_id", sa.Uuid(), nullable=False),
        sa.Column("model_name", sa.String(length=128), nullable=False),
        sa.Column("model_version", sa.String(length=64), nullable=False),
        sa.Column("recommendation", sa.String(length=32), nullable=False),
        sa.Column("confidence_score", sa.Float(), nullable=True),
        sa.Column("explanation_json", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["signal_snapshot_id"],
            ["strategy_signal_snapshots.id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_ai_inference_records_signal_snapshot_id",
        "ai_inference_records",
        ["signal_snapshot_id"],
    )
    op.create_index(
        "ix_ai_inference_records_model_name",
        "ai_inference_records",
        ["model_name"],
    )
    op.create_index(
        "ix_ai_inference_records_recommendation",
        "ai_inference_records",
        ["recommendation"],
    )
    op.create_index(
        "ix_ai_inference_records_created_at",
        "ai_inference_records",
        ["created_at"],
    )


def downgrade() -> None:
    op.drop_index("ix_ai_inference_records_created_at", table_name="ai_inference_records")
    op.drop_index("ix_ai_inference_records_recommendation", table_name="ai_inference_records")
    op.drop_index("ix_ai_inference_records_model_name", table_name="ai_inference_records")
    op.drop_index("ix_ai_inference_records_signal_snapshot_id", table_name="ai_inference_records")
    op.drop_table("ai_inference_records")

    op.drop_index("ix_trade_outcome_features_created_at", table_name="trade_outcome_features")
    op.drop_index(
        "ix_trade_outcome_features_profitable_after_fees_label",
        table_name="trade_outcome_features",
    )
    op.drop_index("ix_trade_outcome_features_win_loss_label", table_name="trade_outcome_features")
    op.drop_index("ix_trade_outcome_features_token_symbol", table_name="trade_outcome_features")
    op.drop_index("ix_trade_outcome_features_strategy_name", table_name="trade_outcome_features")
    op.drop_index("ix_trade_outcome_features_trading_mode", table_name="trade_outcome_features")
    op.drop_index(
        "ix_trade_outcome_features_signal_snapshot_id",
        table_name="trade_outcome_features",
    )
    op.drop_index("ix_trade_outcome_features_trade_id", table_name="trade_outcome_features")
    op.drop_table("trade_outcome_features")

    op.drop_index("ix_strategy_decision_audits_created_at", table_name="strategy_decision_audits")
    op.drop_index("ix_strategy_decision_audits_reason_code", table_name="strategy_decision_audits")
    op.drop_index("ix_strategy_decision_audits_decision", table_name="strategy_decision_audits")
    op.drop_index("ix_strategy_decision_audits_stage", table_name="strategy_decision_audits")
    op.drop_index(
        "ix_strategy_decision_audits_signal_snapshot_id",
        table_name="strategy_decision_audits",
    )
    op.drop_table("strategy_decision_audits")

    op.drop_index(
        "ix_strategy_signal_snapshots_token_policy_status",
        table_name="strategy_signal_snapshots",
    )
    op.drop_index("ix_strategy_signal_snapshots_timestamp", table_name="strategy_signal_snapshots")
    op.drop_index("ix_strategy_signal_snapshots_token_symbol", table_name="strategy_signal_snapshots")
    op.drop_index("ix_strategy_signal_snapshots_strategy_name", table_name="strategy_signal_snapshots")
    op.drop_index("ix_strategy_signal_snapshots_trading_mode", table_name="strategy_signal_snapshots")
    op.drop_index("ix_strategy_signal_snapshots_tenant_id", table_name="strategy_signal_snapshots")
    op.drop_index("ix_strategy_signal_snapshots_user_id", table_name="strategy_signal_snapshots")
    op.drop_table("strategy_signal_snapshots")

