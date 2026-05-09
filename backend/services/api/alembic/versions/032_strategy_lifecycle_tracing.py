"""add strategy lifecycle tracing table

Revision ID: 032_strategy_lifecycle_trace
Revises: 031_token_strategy_policy_elig
Create Date: 2026-05-09 08:40:00.000000
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Union

from alembic import op
import sqlalchemy as sa


revision: str = "032_strategy_lifecycle_trace"
down_revision: Union[str, None] = "031_token_strategy_policy_elig"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "strategy_lifecycle_events",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("correlation_id", sa.String(length=64), nullable=False),
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=True),
        sa.Column("strategy_name", sa.String(length=128), nullable=False),
        sa.Column("symbol", sa.String(length=32), nullable=False),
        sa.Column("trading_mode", sa.String(length=16), nullable=False),
        sa.Column("side", sa.String(length=16), nullable=True),
        sa.Column("stage", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("signal_snapshot_id", sa.Uuid(), nullable=True),
        sa.Column("run_id", sa.Uuid(), nullable=True),
        sa.Column("signal_id", sa.Uuid(), nullable=True),
        sa.Column("intent_id", sa.Uuid(), nullable=True),
        sa.Column("order_id", sa.Uuid(), nullable=True),
        sa.Column("trade_id", sa.Uuid(), nullable=True),
        sa.Column("reason_code", sa.String(length=128), nullable=True),
        sa.Column("reason_detail", sa.String(length=512), nullable=True),
        sa.Column("metadata", sa.JSON(), nullable=False),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_strategy_lifecycle_events_correlation_id",
        "strategy_lifecycle_events",
        ["correlation_id"],
    )
    op.create_index(
        "ix_strategy_lifecycle_events_user_id",
        "strategy_lifecycle_events",
        ["user_id"],
    )
    op.create_index(
        "ix_strategy_lifecycle_events_tenant_id",
        "strategy_lifecycle_events",
        ["tenant_id"],
    )
    op.create_index(
        "ix_strategy_lifecycle_events_strategy_name",
        "strategy_lifecycle_events",
        ["strategy_name"],
    )
    op.create_index(
        "ix_strategy_lifecycle_events_symbol",
        "strategy_lifecycle_events",
        ["symbol"],
    )
    op.create_index(
        "ix_strategy_lifecycle_events_trading_mode",
        "strategy_lifecycle_events",
        ["trading_mode"],
    )
    op.create_index(
        "ix_strategy_lifecycle_events_side",
        "strategy_lifecycle_events",
        ["side"],
    )
    op.create_index(
        "ix_strategy_lifecycle_events_stage",
        "strategy_lifecycle_events",
        ["stage"],
    )
    op.create_index(
        "ix_strategy_lifecycle_events_status",
        "strategy_lifecycle_events",
        ["status"],
    )
    op.create_index(
        "ix_strategy_lifecycle_events_signal_snapshot_id",
        "strategy_lifecycle_events",
        ["signal_snapshot_id"],
    )
    op.create_index(
        "ix_strategy_lifecycle_events_run_id",
        "strategy_lifecycle_events",
        ["run_id"],
    )
    op.create_index(
        "ix_strategy_lifecycle_events_signal_id",
        "strategy_lifecycle_events",
        ["signal_id"],
    )
    op.create_index(
        "ix_strategy_lifecycle_events_intent_id",
        "strategy_lifecycle_events",
        ["intent_id"],
    )
    op.create_index(
        "ix_strategy_lifecycle_events_order_id",
        "strategy_lifecycle_events",
        ["order_id"],
    )
    op.create_index(
        "ix_strategy_lifecycle_events_trade_id",
        "strategy_lifecycle_events",
        ["trade_id"],
    )
    op.create_index(
        "ix_strategy_lifecycle_events_reason_code",
        "strategy_lifecycle_events",
        ["reason_code"],
    )
    op.create_index(
        "ix_strategy_lifecycle_events_occurred_at",
        "strategy_lifecycle_events",
        ["occurred_at"],
    )


def downgrade() -> None:
    op.drop_table("strategy_lifecycle_events")
