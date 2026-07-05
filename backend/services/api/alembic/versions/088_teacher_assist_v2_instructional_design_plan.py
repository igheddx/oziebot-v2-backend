"""Add TeacherAssist v2 instructional design plan."""

from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB
from alembic import op

revision = "088_teacher_assist_v2_instructional_design_plan"
down_revision = "087_teacher_assist_v2_slide_visual_assets"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "teacher_assist_v2_instructional_packages",
        sa.Column("instructional_design_plan_json", JSONB(), nullable=True),
    )
    op.add_column(
        "teacher_assist_v2_instructional_packages",
        sa.Column(
            "instructional_design_plan_locked_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
    )


def downgrade() -> None:
    op.drop_column(
        "teacher_assist_v2_instructional_packages",
        "instructional_design_plan_locked_at",
    )
    op.drop_column(
        "teacher_assist_v2_instructional_packages",
        "instructional_design_plan_json",
    )
