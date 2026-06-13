"""TeacherAssist v2 Google OAuth connections and assignment-linked Google Forms."""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "081_teacher_assist_v2_google_forms"
down_revision = "080_teacher_assist_v2_gradebook_mastery"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "teacher_assist_v2_teacher_google_connections",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("teacher_user_id", sa.Uuid(), nullable=False),
        sa.Column("google_email", sa.String(length=320), nullable=True),
        sa.Column("encrypted_access_token", sa.LargeBinary(), nullable=False),
        sa.Column("encrypted_refresh_token", sa.LargeBinary(), nullable=True),
        sa.Column("token_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("scopes_json", sa.JSON(), nullable=False),
        sa.Column("connected_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["teacher_user_id"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("teacher_user_id", name="uq_ta_v2_teacher_google_connections_teacher"),
    )
    op.create_index(
        "ix_ta_v2_teacher_google_connections_teacher",
        "teacher_assist_v2_teacher_google_connections",
        ["teacher_user_id"],
    )

    op.create_table(
        "teacher_assist_v2_assignment_google_forms",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("teacher_user_id", sa.Uuid(), nullable=False),
        sa.Column("assignment_id", sa.Uuid(), nullable=False),
        sa.Column("artifact_id", sa.Uuid(), nullable=False),
        sa.Column("instructional_package_id", sa.Uuid(), nullable=False),
        sa.Column("platform_school_year_id", sa.Uuid(), nullable=False),
        sa.Column("catalog_district_id", sa.Uuid(), nullable=False),
        sa.Column("catalog_school_id", sa.Uuid(), nullable=True),
        sa.Column("catalog_grade_id", sa.Uuid(), nullable=False),
        sa.Column("catalog_subject_id", sa.Uuid(), nullable=False),
        sa.Column("education_objective_ids_json", sa.JSON(), nullable=False),
        sa.Column("google_form_id", sa.String(length=128), nullable=False),
        sa.Column("google_form_url", sa.String(length=1024), nullable=False),
        sa.Column("google_edit_url", sa.String(length=1024), nullable=False),
        sa.Column("google_response_url", sa.String(length=1024), nullable=True),
        sa.Column("google_created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("google_created_by_user_id", sa.Uuid(), nullable=False),
        sa.Column("google_sync_status", sa.String(length=32), nullable=False, server_default="CREATED"),
        sa.Column("question_mapping_json", sa.JSON(), nullable=False),
        sa.Column("last_import_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_import_count", sa.Integer(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["artifact_id"], ["teacher_assist_v2_instructional_package_artifacts.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["assignment_id"], ["teacher_assist_v2_assignments.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["catalog_district_id"], ["education_districts.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["catalog_grade_id"], ["education_grades.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["catalog_school_id"], ["education_schools.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["catalog_subject_id"], ["education_subjects.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["google_created_by_user_id"], ["users.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["instructional_package_id"], ["teacher_assist_v2_instructional_packages.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["platform_school_year_id"], ["education_school_years.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["teacher_user_id"], ["users.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("assignment_id", name="uq_ta_v2_assignment_google_forms_assignment"),
        sa.UniqueConstraint("google_form_id", name="uq_ta_v2_assignment_google_forms_form_id"),
    )
    op.create_index(
        "ix_ta_v2_assignment_google_forms_assignment",
        "teacher_assist_v2_assignment_google_forms",
        ["assignment_id"],
    )
    op.create_index(
        "ix_ta_v2_assignment_google_forms_teacher",
        "teacher_assist_v2_assignment_google_forms",
        ["teacher_user_id"],
    )


def downgrade() -> None:
    op.drop_index("ix_ta_v2_assignment_google_forms_teacher", table_name="teacher_assist_v2_assignment_google_forms")
    op.drop_index("ix_ta_v2_assignment_google_forms_assignment", table_name="teacher_assist_v2_assignment_google_forms")
    op.drop_table("teacher_assist_v2_assignment_google_forms")
    op.drop_index("ix_ta_v2_teacher_google_connections_teacher", table_name="teacher_assist_v2_teacher_google_connections")
    op.drop_table("teacher_assist_v2_teacher_google_connections")
