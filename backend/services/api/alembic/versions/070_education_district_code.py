"""Add district_code to education_districts."""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "070_education_district_code"
down_revision = "069_teacher_assist_v2_role"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "education_districts",
        sa.Column("district_code", sa.String(length=32), nullable=True),
    )
    op.create_index(
        "ix_education_districts_district_code", "education_districts", ["district_code"]
    )
    op.execute(
        """
        UPDATE education_districts
        SET district_code = 'LISD'
        WHERE name = 'Leander Independent School District'
          AND district_code IS NULL
        """
    )


def downgrade() -> None:
    op.drop_index("ix_education_districts_district_code", table_name="education_districts")
    op.drop_column("education_districts", "district_code")
