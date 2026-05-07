"""token strategy policy eligibility fields

Revision ID: 028_token_strategy_policy_eligibility
Revises: 027_strategy_tuning_defaults
Create Date: 2026-05-06 21:30:00.000000
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Union

from alembic import op
import sqlalchemy as sa


revision: str = "028_token_strategy_policy_eligibility"
down_revision: Union[str, None] = "027_strategy_tuning_defaults"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "token_strategy_policy",
        sa.Column(
            "size_multiplier",
            sa.Numeric(precision=12, scale=6),
            nullable=True,
        ),
    )
    op.add_column(
        "token_strategy_policy",
        sa.Column(
            "max_position_usd_override",
            sa.Numeric(precision=18, scale=2),
            nullable=True,
        ),
    )
    op.add_column(
        "token_strategy_policy",
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
    )

    op.execute(
        sa.text(
            """
            UPDATE token_strategy_policy
            SET
              size_multiplier = CASE
                WHEN COALESCE(recommendation_status_override, recommendation_status) = 'blocked' THEN 0
                WHEN COALESCE(recommendation_status_override, recommendation_status) = 'discouraged' THEN 0.5
                ELSE 1
              END,
              created_at = COALESCE(computed_at, updated_at)
            """
        )
    )

    op.alter_column("token_strategy_policy", "created_at", nullable=False)


def downgrade() -> None:
    op.drop_column("token_strategy_policy", "created_at")
    op.drop_column("token_strategy_policy", "max_position_usd_override")
    op.drop_column("token_strategy_policy", "size_multiplier")
