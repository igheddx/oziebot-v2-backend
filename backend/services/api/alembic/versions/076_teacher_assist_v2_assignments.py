"""TeacherAssist v2 assignment framework linked to instructional packages."""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "076_teacher_assist_v2_assignments"
down_revision = "075_instructional_package_lifecycle"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "teacher_assist_v2_assignments",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("teacher_user_id", sa.Uuid(), nullable=False),
        sa.Column("platform_school_year_id", sa.Uuid(), nullable=False),
        sa.Column("catalog_district_id", sa.Uuid(), nullable=False),
        sa.Column("catalog_school_id", sa.Uuid(), nullable=True),
        sa.Column("catalog_grade_id", sa.Uuid(), nullable=False),
        sa.Column("catalog_subject_id", sa.Uuid(), nullable=False),
        sa.Column("instructional_package_id", sa.Uuid(), nullable=False),
        sa.Column("pacing_guide_id", sa.Uuid(), nullable=False),
        sa.Column("week_number", sa.Integer(), nullable=False),
        sa.Column("assignment_type", sa.String(length=32), nullable=False),
        sa.Column("title", sa.String(length=256), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("status", sa.String(length=32), nullable=False, server_default="DRAFT"),
        sa.Column("education_objective_ids_json", sa.JSON(), nullable=False),
        sa.Column("created_by_user_id", sa.Uuid(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["catalog_district_id"], ["education_districts.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["catalog_grade_id"], ["education_grades.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["catalog_school_id"], ["education_schools.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["catalog_subject_id"], ["education_subjects.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["created_by_user_id"], ["users.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["instructional_package_id"], ["teacher_assist_v2_instructional_packages.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["pacing_guide_id"], ["pacing_guides.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["platform_school_year_id"], ["education_school_years.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["teacher_user_id"], ["users.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_ta_v2_assignments_teacher",
        "teacher_assist_v2_assignments",
        ["teacher_user_id"],
    )
    op.create_index(
        "ix_ta_v2_assignments_package",
        "teacher_assist_v2_assignments",
        ["instructional_package_id"],
    )

    op.add_column(
        "teacher_assist_v2_instructional_package_artifacts",
        sa.Column("assignment_id", sa.Uuid(), nullable=True),
    )
    op.create_foreign_key(
        "fk_ta_v2_artifacts_assignment",
        "teacher_assist_v2_instructional_package_artifacts",
        "teacher_assist_v2_assignments",
        ["assignment_id"],
        ["id"],
        ondelete="SET NULL",
    )


def downgrade() -> None:
    op.drop_constraint("fk_ta_v2_artifacts_assignment", "teacher_assist_v2_instructional_package_artifacts", type_="foreignkey")
    op.drop_column("teacher_assist_v2_instructional_package_artifacts", "assignment_id")
    op.drop_index("ix_ta_v2_assignments_package", table_name="teacher_assist_v2_assignments")
    op.drop_index("ix_ta_v2_assignments_teacher", table_name="teacher_assist_v2_assignments")
    op.drop_table("teacher_assist_v2_assignments")
