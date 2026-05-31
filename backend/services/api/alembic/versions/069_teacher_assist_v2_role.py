"""Add teacher_assist_role to users for v2 role-based routing."""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "069_teacher_assist_v2_role"
down_revision = "068_teacher_assist_school_year_template_flag"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "users",
        sa.Column("teacher_assist_role", sa.String(length=32), nullable=True),
    )
    op.execute(
        """
        UPDATE users
        SET teacher_assist_role = 'root_admin'
        WHERE is_root_admin = true AND teacher_assist_role IS NULL
        """
    )


def downgrade() -> None:
    op.drop_column("users", "teacher_assist_role")
