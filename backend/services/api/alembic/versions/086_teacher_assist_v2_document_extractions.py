"""Add TeacherAssist v2 document extractions."""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "086_teacher_assist_v2_document_extractions"
down_revision = "085_teacher_assist_v2_manual_assignments"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "teacher_assist_v2_document_extractions",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("teacher_user_id", sa.Uuid(), nullable=False),
        sa.Column("source_type", sa.String(length=32), nullable=False),
        sa.Column("supporting_material_id", sa.Uuid(), nullable=True),
        sa.Column("planning_supplemental_material_id", sa.Uuid(), nullable=True),
        sa.Column("student_submission_id", sa.Uuid(), nullable=True),
        sa.Column("pacing_guide_id", sa.Uuid(), nullable=True),
        sa.Column("package_id", sa.Uuid(), nullable=True),
        sa.Column("assignment_id", sa.Uuid(), nullable=True),
        sa.Column("platform_school_year_id", sa.Uuid(), nullable=True),
        sa.Column("catalog_state_id", sa.Uuid(), nullable=True),
        sa.Column("catalog_district_id", sa.Uuid(), nullable=True),
        sa.Column("catalog_school_id", sa.Uuid(), nullable=True),
        sa.Column("catalog_grade_id", sa.Uuid(), nullable=True),
        sa.Column("catalog_subject_id", sa.Uuid(), nullable=True),
        sa.Column("week_start", sa.Integer(), nullable=True),
        sa.Column("week_end", sa.Integer(), nullable=True),
        sa.Column("student_number", sa.Integer(), nullable=True),
        sa.Column("storage_key", sa.String(length=512), nullable=False),
        sa.Column("original_filename", sa.String(length=255), nullable=False),
        sa.Column("mime_type", sa.String(length=255), nullable=False),
        sa.Column("file_size", sa.Integer(), nullable=False),
        sa.Column("extraction_status", sa.String(length=32), nullable=False),
        sa.Column("extracted_text", sa.Text(), nullable=True),
        sa.Column("extracted_text_preview", sa.Text(), nullable=True),
        sa.Column("character_count", sa.Integer(), nullable=True),
        sa.Column("token_estimate", sa.Integer(), nullable=True),
        sa.Column("provider", sa.String(length=128), nullable=True),
        sa.Column("teacher_edited_text", sa.Text(), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["assignment_id"], ["teacher_assist_v2_assignments.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["catalog_district_id"], ["education_districts.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["catalog_grade_id"], ["education_grades.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["catalog_school_id"], ["education_schools.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["catalog_state_id"], ["education_states.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["catalog_subject_id"], ["education_subjects.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["package_id"], ["teacher_assist_v2_instructional_packages.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["pacing_guide_id"], ["pacing_guides.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(
            ["planning_supplemental_material_id"],
            ["teacher_assist_v2_planning_supplemental_materials.id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["platform_school_year_id"], ["education_school_years.id"], ondelete="SET NULL"
        ),
        sa.ForeignKeyConstraint(
            ["student_submission_id"], ["teacher_assist_v2_student_submissions.id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(
            ["supporting_material_id"], ["pacing_guide_supporting_materials.id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(["teacher_user_id"], ["users.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("supporting_material_id"),
        sa.UniqueConstraint("planning_supplemental_material_id"),
        sa.UniqueConstraint("student_submission_id"),
    )
    op.create_index(
        "ix_ta_v2_doc_extract_tenant_teacher",
        "teacher_assist_v2_document_extractions",
        ["tenant_id", "teacher_user_id"],
    )
    op.create_index(
        "ix_ta_v2_doc_extract_status",
        "teacher_assist_v2_document_extractions",
        ["extraction_status"],
    )


def downgrade() -> None:
    op.drop_index("ix_ta_v2_doc_extract_status", table_name="teacher_assist_v2_document_extractions")
    op.drop_index("ix_ta_v2_doc_extract_tenant_teacher", table_name="teacher_assist_v2_document_extractions")
    op.drop_table("teacher_assist_v2_document_extractions")
