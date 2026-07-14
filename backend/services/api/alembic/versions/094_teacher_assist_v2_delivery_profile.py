"""TeacherAssist v2 Instructional Delivery Profile — classroom delivery constraint."""

from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB
from alembic import op

revision = "094_teacher_assist_v2_delivery_profile"
down_revision = "093_teacher_assist_v2_recovery_artifacts"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "teacher_assist_v2_instructional_packages",
        sa.Column("instructional_delivery_profile", JSONB(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("teacher_assist_v2_instructional_packages", "instructional_delivery_profile")
