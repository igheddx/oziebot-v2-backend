"""Add execution reconciliation audit events

Revision ID: 015
Revises: 014
Create Date: 2026-04-12

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "015"
down_revision: Union[str, None] = "014"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "execution_reconciliation_events",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("order_id", sa.Uuid(), nullable=True),
        sa.Column("trading_mode", sa.String(length=16), nullable=False),
        sa.Column("scope", sa.String(length=32), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("detail", sa.String(length=512), nullable=True),
        sa.Column("internal_snapshot", sa.JSON(), nullable=False),
        sa.Column("external_snapshot", sa.JSON(), nullable=False),
        sa.Column("repair_applied", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("metadata", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["order_id"], ["execution_orders.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_execution_reconciliation_events_tenant_id", "execution_reconciliation_events", ["tenant_id"])
    op.create_index("ix_execution_reconciliation_events_order_id", "execution_reconciliation_events", ["order_id"])
    op.create_index("ix_execution_reconciliation_events_trading_mode", "execution_reconciliation_events", ["trading_mode"])
    op.create_index("ix_execution_reconciliation_events_scope", "execution_reconciliation_events", ["scope"])
    op.create_index("ix_execution_reconciliation_events_status", "execution_reconciliation_events", ["status"])
    op.create_index("ix_execution_reconciliation_events_created_at", "execution_reconciliation_events", ["created_at"])


def downgrade() -> None:
    op.drop_table("execution_reconciliation_events")