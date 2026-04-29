"""add trade outcome retention analytics fields

Revision ID: 028_trade_outcome_retention_analytics
Revises: 027_strategy_tuning_defaults
Create Date: 2026-04-29 02:30:00.000000
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "028_trade_outcome_retention_analytics"
down_revision: Union[str, None] = "027_strategy_tuning_defaults"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "trade_outcome_features",
        sa.Column("profit_giveback_pct", sa.Numeric(18, 10), nullable=True),
    )
    op.add_column(
        "trade_outcome_features",
        sa.Column(
            "partial_profit_taken",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        ),
    )
    op.add_column(
        "trade_outcome_features",
        sa.Column("remaining_position_outcome", sa.String(length=32), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("trade_outcome_features", "remaining_position_outcome")
    op.drop_column("trade_outcome_features", "partial_profit_taken")
    op.drop_column("trade_outcome_features", "profit_giveback_pct")
