"""Add user strategy runtime states

Revision ID: 009
Revises: 008
Create Date: 2026-04-12

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "009"
down_revision: Union[str, None] = "008"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "user_strategy_states",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("strategy_id", sa.String(length=64), nullable=False),
        sa.Column("trading_mode", sa.String(length=16), nullable=False),
        sa.Column("state", sa.JSON(), nullable=False, server_default="{}"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "user_id",
            "strategy_id",
            "trading_mode",
            name="uq_user_strategy_state_mode",
        ),
    )
    op.create_index("ix_user_strategy_states_user_id", "user_strategy_states", ["user_id"])
    op.create_index(
        "ix_user_strategy_states_strategy_id", "user_strategy_states", ["strategy_id"]
    )


def downgrade() -> None:
    op.drop_table("user_strategy_states")
