"""Add TeacherAssist v2 instructional validation report."""

from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB
from alembic import op

revision = "089_teacher_assist_v2_validation_report"
down_revision = "088_teacher_assist_v2_instructional_design_plan"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "teacher_assist_v2_instructional_packages",
        sa.Column("instructional_validation_report_json", JSONB(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column(
        "teacher_assist_v2_instructional_packages",
        "instructional_validation_report_json",
    )
