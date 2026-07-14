"""TeacherAssist v2 — Persist instructional resource bank on package."""

from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB
from alembic import op

revision = "095_teacher_assist_v2_resource_bank"
down_revision = "094_teacher_assist_v2_delivery_profile"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "teacher_assist_v2_instructional_packages",
        sa.Column("instructional_resource_bank_json", JSONB(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("teacher_assist_v2_instructional_packages", "instructional_resource_bank_json")
