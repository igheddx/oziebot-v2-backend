"""Platform school years and objective hierarchy anchors for TeacherAssist v2."""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "071_v2_instructional_foundation"
down_revision = "070_education_district_code"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "education_school_years",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("state_id", sa.Uuid(), nullable=False),
        sa.Column("district_id", sa.Uuid(), nullable=True),
        sa.Column("school_id", sa.Uuid(), nullable=True),
        sa.Column("title", sa.String(length=64), nullable=False),
        sa.Column("start_date", sa.Date(), nullable=False),
        sa.Column("end_date", sa.Date(), nullable=False),
        sa.Column("active", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["district_id"], ["education_districts.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["school_id"], ["education_schools.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["state_id"], ["education_states.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_education_school_years_state_id", "education_school_years", ["state_id"])
    op.create_index("ix_education_school_years_district_id", "education_school_years", ["district_id"])
    op.create_index("ix_education_school_years_school_id", "education_school_years", ["school_id"])

    op.add_column("education_objectives", sa.Column("district_id", sa.Uuid(), nullable=True))
    op.add_column("education_objectives", sa.Column("school_id", sa.Uuid(), nullable=True))
    op.add_column("education_objectives", sa.Column("grade_id", sa.Uuid(), nullable=True))
    op.add_column("education_objectives", sa.Column("subject_id", sa.Uuid(), nullable=True))
    op.add_column("education_objectives", sa.Column("school_year_id", sa.Uuid(), nullable=True))
    op.create_foreign_key(
        "fk_education_objectives_district_id",
        "education_objectives",
        "education_districts",
        ["district_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_foreign_key(
        "fk_education_objectives_school_id",
        "education_objectives",
        "education_schools",
        ["school_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_foreign_key(
        "fk_education_objectives_grade_id",
        "education_objectives",
        "education_grades",
        ["grade_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_foreign_key(
        "fk_education_objectives_subject_id",
        "education_objectives",
        "education_subjects",
        ["subject_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_foreign_key(
        "fk_education_objectives_school_year_id",
        "education_objectives",
        "education_school_years",
        ["school_year_id"],
        ["id"],
        ondelete="SET NULL",
    )


def downgrade() -> None:
    op.drop_constraint("fk_education_objectives_school_year_id", "education_objectives", type_="foreignkey")
    op.drop_constraint("fk_education_objectives_subject_id", "education_objectives", type_="foreignkey")
    op.drop_constraint("fk_education_objectives_grade_id", "education_objectives", type_="foreignkey")
    op.drop_constraint("fk_education_objectives_school_id", "education_objectives", type_="foreignkey")
    op.drop_constraint("fk_education_objectives_district_id", "education_objectives", type_="foreignkey")
    op.drop_column("education_objectives", "school_year_id")
    op.drop_column("education_objectives", "subject_id")
    op.drop_column("education_objectives", "grade_id")
    op.drop_column("education_objectives", "school_id")
    op.drop_column("education_objectives", "district_id")
    op.drop_table("education_school_years")
