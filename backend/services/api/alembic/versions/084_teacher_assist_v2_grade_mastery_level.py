"""Add mastery_level to v2 assignment grades and gradebook records."""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "084_teacher_assist_v2_grade_mastery_level"
down_revision = "083_pacing_guide_period_days"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "teacher_assist_v2_assignment_grades",
        sa.Column("mastery_level", sa.String(length=32), nullable=True),
    )
    op.add_column(
        "teacher_assist_v2_gradebook_records",
        sa.Column("mastery_level", sa.String(length=32), nullable=True),
    )
    op.execute(
        sa.text(
            """
            UPDATE teacher_assist_v2_assignment_grades
            SET mastery_level = CASE
                WHEN percentage >= 80 THEN 'mastery'
                WHEN percentage >= 60 THEN 'developing'
                ELSE 'beginning'
            END
            WHERE mastery_level IS NULL
            """
        )
    )
    op.execute(
        sa.text(
            """
            UPDATE teacher_assist_v2_gradebook_records
            SET mastery_level = CASE
                WHEN percentage >= 80 THEN 'mastery'
                WHEN percentage >= 60 THEN 'developing'
                ELSE 'beginning'
            END
            WHERE mastery_level IS NULL
            """
        )
    )
    op.alter_column("teacher_assist_v2_assignment_grades", "mastery_level", nullable=False)
    op.alter_column("teacher_assist_v2_gradebook_records", "mastery_level", nullable=False)


def downgrade() -> None:
    op.drop_column("teacher_assist_v2_gradebook_records", "mastery_level")
    op.drop_column("teacher_assist_v2_assignment_grades", "mastery_level")
