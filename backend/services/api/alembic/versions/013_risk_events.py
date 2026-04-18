"""Add risk decision audit table

Revision ID: 013
Revises: 012
Create Date: 2026-04-12

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "013"
down_revision: Union[str, None] = "012"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "risk_events",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("signal_id", sa.Uuid(), nullable=False),
        sa.Column("run_id", sa.Uuid(), nullable=False),
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("strategy_name", sa.String(length=128), nullable=False),
        sa.Column("symbol", sa.String(length=32), nullable=False),
        sa.Column("trading_mode", sa.String(length=16), nullable=False),
        sa.Column("outcome", sa.String(length=32), nullable=False),
        sa.Column("reason", sa.String(length=64), nullable=True),
        sa.Column("detail", sa.String(length=512), nullable=True),
        sa.Column("original_size", sa.String(length=64), nullable=False),
        sa.Column("final_size", sa.String(length=64), nullable=False),
        sa.Column("trace_id", sa.String(length=64), nullable=False),
        sa.Column("rules_evaluated", sa.JSON(), nullable=False),
        sa.Column("signal_payload", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_risk_events_signal_id", "risk_events", ["signal_id"])
    op.create_index("ix_risk_events_run_id", "risk_events", ["run_id"])
    op.create_index("ix_risk_events_user_id", "risk_events", ["user_id"])
    op.create_index("ix_risk_events_strategy_name", "risk_events", ["strategy_name"])
    op.create_index("ix_risk_events_symbol", "risk_events", ["symbol"])
    op.create_index("ix_risk_events_trading_mode", "risk_events", ["trading_mode"])
    op.create_index("ix_risk_events_outcome", "risk_events", ["outcome"])
    op.create_index("ix_risk_events_trace_id", "risk_events", ["trace_id"])
    op.create_index("ix_risk_events_created_at", "risk_events", ["created_at"])


def downgrade() -> None:
    op.drop_table("risk_events")
