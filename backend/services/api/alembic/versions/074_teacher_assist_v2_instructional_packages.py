"""TeacherAssist v2 instructional packages and planning supplemental materials."""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "074_teacher_assist_v2_instructional_packages"
down_revision = "073_pacing_guide_supporting_materials"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "teacher_assist_v2_instructional_packages",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("teacher_user_id", sa.Uuid(), nullable=False),
        sa.Column("platform_school_year_id", sa.Uuid(), nullable=False),
        sa.Column("catalog_state_id", sa.Uuid(), nullable=False),
        sa.Column("catalog_district_id", sa.Uuid(), nullable=False),
        sa.Column("catalog_school_id", sa.Uuid(), nullable=True),
        sa.Column("catalog_grade_id", sa.Uuid(), nullable=False),
        sa.Column("subject_ids_json", sa.JSON(), nullable=False),
        sa.Column("pacing_guide_ids_json", sa.JSON(), nullable=False),
        sa.Column("week_start", sa.Integer(), nullable=False),
        sa.Column("week_end", sa.Integer(), nullable=False),
        sa.Column("teaching_order_json", sa.JSON(), nullable=False),
        sa.Column("selected_outputs_json", sa.JSON(), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False, server_default="ready"),
        sa.Column("provider_name", sa.String(length=64), nullable=True),
        sa.Column("metadata_json", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["catalog_district_id"], ["education_districts.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["catalog_grade_id"], ["education_grades.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["catalog_school_id"], ["education_schools.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["catalog_state_id"], ["education_states.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["platform_school_year_id"], ["education_school_years.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["teacher_user_id"], ["users.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_ta_v2_instructional_packages_teacher",
        "teacher_assist_v2_instructional_packages",
        ["teacher_user_id"],
    )

    op.create_table(
        "teacher_assist_v2_instructional_package_artifacts",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("package_id", sa.Uuid(), nullable=False),
        sa.Column("artifact_type", sa.String(length=64), nullable=False),
        sa.Column("subject_id", sa.Uuid(), nullable=True),
        sa.Column("period_id", sa.Uuid(), nullable=True),
        sa.Column("day_label", sa.String(length=32), nullable=True),
        sa.Column("sequence_number", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("title", sa.String(length=256), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False, server_default="ready"),
        sa.Column("content_json", sa.JSON(), nullable=False),
        sa.Column("preview_html", sa.Text(), nullable=True),
        sa.Column("storage_key", sa.String(length=512), nullable=True),
        sa.Column("export_format", sa.String(length=32), nullable=True),
        sa.Column("metadata_json", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["package_id"], ["teacher_assist_v2_instructional_packages.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["period_id"], ["pacing_guide_periods.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["subject_id"], ["education_subjects.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_ta_v2_package_artifacts_package",
        "teacher_assist_v2_instructional_package_artifacts",
        ["package_id"],
    )

    op.create_table(
        "teacher_assist_v2_planning_supplemental_materials",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("teacher_user_id", sa.Uuid(), nullable=False),
        sa.Column("platform_school_year_id", sa.Uuid(), nullable=False),
        sa.Column("catalog_state_id", sa.Uuid(), nullable=False),
        sa.Column("catalog_district_id", sa.Uuid(), nullable=False),
        sa.Column("catalog_school_id", sa.Uuid(), nullable=True),
        sa.Column("catalog_grade_id", sa.Uuid(), nullable=False),
        sa.Column("week_start", sa.Integer(), nullable=False),
        sa.Column("week_end", sa.Integer(), nullable=False),
        sa.Column("package_id", sa.Uuid(), nullable=True),
        sa.Column("material_kind", sa.String(length=16), nullable=False),
        sa.Column("resource_type", sa.String(length=64), nullable=False),
        sa.Column("title", sa.String(length=256), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("note_body", sa.Text(), nullable=True),
        sa.Column("external_url", sa.String(length=2048), nullable=True),
        sa.Column("storage_key", sa.String(length=512), nullable=True),
        sa.Column("original_filename", sa.String(length=512), nullable=True),
        sa.Column("mime_type", sa.String(length=128), nullable=True),
        sa.Column("file_size", sa.Integer(), nullable=True),
        sa.Column("uploaded_by_user_id", sa.Uuid(), nullable=False),
        sa.Column("active", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["catalog_district_id"], ["education_districts.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["catalog_grade_id"], ["education_grades.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["catalog_school_id"], ["education_schools.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["catalog_state_id"], ["education_states.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["package_id"], ["teacher_assist_v2_instructional_packages.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["platform_school_year_id"], ["education_school_years.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["teacher_user_id"], ["users.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["uploaded_by_user_id"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_ta_v2_planning_supplemental_teacher_week",
        "teacher_assist_v2_planning_supplemental_materials",
        ["teacher_user_id", "week_start", "week_end"],
    )


def downgrade() -> None:
    op.drop_index("ix_ta_v2_planning_supplemental_teacher_week", table_name="teacher_assist_v2_planning_supplemental_materials")
    op.drop_table("teacher_assist_v2_planning_supplemental_materials")
    op.drop_index("ix_ta_v2_package_artifacts_package", table_name="teacher_assist_v2_instructional_package_artifacts")
    op.drop_table("teacher_assist_v2_instructional_package_artifacts")
    op.drop_index("ix_ta_v2_instructional_packages_teacher", table_name="teacher_assist_v2_instructional_packages")
    op.drop_table("teacher_assist_v2_instructional_packages")
