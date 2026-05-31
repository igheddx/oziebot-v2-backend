"""TeacherAssist v2 gradebook and mastery evidence from confirmed grades."""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "080_teacher_assist_v2_gradebook_mastery"
down_revision = "079_teacher_assist_v2_grade_review"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "teacher_assist_v2_gradebook_records",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("teacher_user_id", sa.Uuid(), nullable=False),
        sa.Column("platform_school_year_id", sa.Uuid(), nullable=False),
        sa.Column("catalog_district_id", sa.Uuid(), nullable=False),
        sa.Column("catalog_school_id", sa.Uuid(), nullable=True),
        sa.Column("catalog_grade_id", sa.Uuid(), nullable=False),
        sa.Column("catalog_subject_id", sa.Uuid(), nullable=False),
        sa.Column("pacing_guide_id", sa.Uuid(), nullable=False),
        sa.Column("instructional_package_id", sa.Uuid(), nullable=False),
        sa.Column("assignment_id", sa.Uuid(), nullable=False),
        sa.Column("student_number", sa.Integer(), nullable=False),
        sa.Column("education_objective_ids_json", sa.JSON(), nullable=False),
        sa.Column("assignment_grade_id", sa.Uuid(), nullable=False),
        sa.Column("score", sa.Float(), nullable=False),
        sa.Column("max_score", sa.Float(), nullable=False),
        sa.Column("percentage", sa.Float(), nullable=False),
        sa.Column("teacher_comment", sa.Text(), nullable=False),
        sa.Column("confirmed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("sync_status", sa.String(length=32), nullable=False, server_default="SYNCED"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["assignment_grade_id"], ["teacher_assist_v2_assignment_grades.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["assignment_id"], ["teacher_assist_v2_assignments.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["catalog_district_id"], ["education_districts.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["catalog_grade_id"], ["education_grades.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["catalog_school_id"], ["education_schools.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["catalog_subject_id"], ["education_subjects.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["instructional_package_id"], ["teacher_assist_v2_instructional_packages.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["pacing_guide_id"], ["pacing_guides.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["platform_school_year_id"], ["education_school_years.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["teacher_user_id"], ["users.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("assignment_id", "student_number", name="uq_ta_v2_gradebook_assignment_student"),
    )
    op.create_index(
        "ix_ta_v2_gradebook_teacher_school_year",
        "teacher_assist_v2_gradebook_records",
        ["teacher_user_id", "platform_school_year_id"],
    )
    op.create_index(
        "ix_ta_v2_gradebook_assignment",
        "teacher_assist_v2_gradebook_records",
        ["assignment_id"],
    )

    op.create_table(
        "teacher_assist_v2_gradebook_record_revisions",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("gradebook_record_id", sa.Uuid(), nullable=False),
        sa.Column("assignment_grade_id", sa.Uuid(), nullable=False),
        sa.Column("previous_score", sa.Float(), nullable=False),
        sa.Column("previous_max_score", sa.Float(), nullable=False),
        sa.Column("previous_percentage", sa.Float(), nullable=False),
        sa.Column("previous_teacher_comment", sa.Text(), nullable=False),
        sa.Column("new_score", sa.Float(), nullable=False),
        sa.Column("new_max_score", sa.Float(), nullable=False),
        sa.Column("new_percentage", sa.Float(), nullable=False),
        sa.Column("new_teacher_comment", sa.Text(), nullable=False),
        sa.Column("revised_by_user_id", sa.Uuid(), nullable=False),
        sa.Column("revised_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["assignment_grade_id"], ["teacher_assist_v2_assignment_grades.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["gradebook_record_id"], ["teacher_assist_v2_gradebook_records.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["revised_by_user_id"], ["users.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_ta_v2_gradebook_revision_record",
        "teacher_assist_v2_gradebook_record_revisions",
        ["gradebook_record_id"],
    )

    op.create_table(
        "teacher_assist_v2_mastery_evidence",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("teacher_user_id", sa.Uuid(), nullable=False),
        sa.Column("student_number", sa.Integer(), nullable=False),
        sa.Column("education_objective_id", sa.Uuid(), nullable=False),
        sa.Column("assignment_id", sa.Uuid(), nullable=False),
        sa.Column("gradebook_record_id", sa.Uuid(), nullable=False),
        sa.Column("assignment_grade_id", sa.Uuid(), nullable=False),
        sa.Column("evidence_type", sa.String(length=32), nullable=False),
        sa.Column("score", sa.Float(), nullable=False),
        sa.Column("percentage", sa.Float(), nullable=False),
        sa.Column("mastery_level", sa.String(length=32), nullable=False),
        sa.Column("teacher_confirmed", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("is_current", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["assignment_grade_id"], ["teacher_assist_v2_assignment_grades.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["assignment_id"], ["teacher_assist_v2_assignments.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["education_objective_id"], ["education_objectives.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["gradebook_record_id"], ["teacher_assist_v2_gradebook_records.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["teacher_user_id"], ["users.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_ta_v2_mastery_objective_current",
        "teacher_assist_v2_mastery_evidence",
        ["education_objective_id", "is_current"],
    )
    op.create_index(
        "ix_ta_v2_mastery_assignment_student",
        "teacher_assist_v2_mastery_evidence",
        ["assignment_id", "student_number", "is_current"],
    )


def downgrade() -> None:
    op.drop_index("ix_ta_v2_mastery_assignment_student", table_name="teacher_assist_v2_mastery_evidence")
    op.drop_index("ix_ta_v2_mastery_objective_current", table_name="teacher_assist_v2_mastery_evidence")
    op.drop_table("teacher_assist_v2_mastery_evidence")
    op.drop_index("ix_ta_v2_gradebook_revision_record", table_name="teacher_assist_v2_gradebook_record_revisions")
    op.drop_table("teacher_assist_v2_gradebook_record_revisions")
    op.drop_index("ix_ta_v2_gradebook_assignment", table_name="teacher_assist_v2_gradebook_records")
    op.drop_index("ix_ta_v2_gradebook_teacher_school_year", table_name="teacher_assist_v2_gradebook_records")
    op.drop_table("teacher_assist_v2_gradebook_records")
