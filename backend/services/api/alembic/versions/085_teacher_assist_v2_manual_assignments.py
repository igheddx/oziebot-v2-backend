"""Add creation_origin to v2 assignments and packet_kind to print packets."""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "085_teacher_assist_v2_manual_assignments"
down_revision = "084_teacher_assist_v2_grade_mastery_level"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "teacher_assist_v2_assignments",
        sa.Column("creation_origin", sa.String(length=32), nullable=False, server_default="PACKAGE"),
    )
    op.add_column(
        "teacher_assist_v2_assignment_print_packets",
        sa.Column("packet_kind", sa.String(length=32), nullable=False, server_default="STUDENT_PACKET"),
    )


def downgrade() -> None:
    op.drop_column("teacher_assist_v2_assignment_print_packets", "packet_kind")
    op.drop_column("teacher_assist_v2_assignments", "creation_origin")
