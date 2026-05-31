"""mark template school years used to anchor district pacing guides

Revision ID: 068_teacher_assist_school_year_template_flag
Revises: 067_teacher_assist_pilot_readiness_foundation
Create Date: 2026-05-31 20:00:00.000000
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Union

from alembic import op
import sqlalchemy as sa


revision: str = "068_teacher_assist_school_year_template_flag"
down_revision: Union[str, None] = "067_teacher_assist_pilot_readiness_foundation"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "school_years",
        sa.Column("is_template", sa.Boolean(), nullable=False, server_default="false"),
    )
    op.execute(
        """
        UPDATE school_years AS sy
        SET is_template = true,
            is_active = false
        FROM pacing_guides AS pg
        WHERE pg.school_year_id = sy.id
          AND pg.guide_type = 'DISTRICT'
        """
    )


def downgrade() -> None:
    op.drop_column("school_years", "is_template")
