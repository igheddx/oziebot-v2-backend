"""Add TeacherAssist v2 Today's Teaching Brief (teacher coaching summary)."""

from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB
from alembic import op

revision = "091_teacher_assist_v2_coaching_summary"
down_revision = "090_teacher_assist_v2_alignment_report"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "teacher_assist_v2_instructional_packages",
        sa.Column("teacher_coaching_summary_json", JSONB(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column(
        "teacher_assist_v2_instructional_packages",
        "teacher_coaching_summary_json",
    )
