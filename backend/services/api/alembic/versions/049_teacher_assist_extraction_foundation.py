"""add teacher assist extraction foundation

Revision ID: 049_teacher_assist_extraction_foundation
Revises: 048_teacher_assist_activity_events
Create Date: 2026-05-28 12:30:00.000000
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Union

from alembic import op
import sqlalchemy as sa


revision: str = "049_teacher_assist_extraction_foundation"
down_revision: Union[str, None] = "048_teacher_assist_activity_events"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "teacher_assist_extraction_jobs",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("teacher_user_id", sa.Uuid(), nullable=False),
        sa.Column("artifact_type", sa.String(length=32), nullable=False),
        sa.Column("resource_library_item_id", sa.Uuid(), nullable=True),
        sa.Column("student_work_submission_id", sa.Uuid(), nullable=True),
        sa.Column("assignment_id", sa.Uuid(), nullable=True),
        sa.Column("school_year_id", sa.Uuid(), nullable=True),
        sa.Column("grading_period_id", sa.Uuid(), nullable=True),
        sa.Column("class_id", sa.Uuid(), nullable=True),
        sa.Column("subject_id", sa.Uuid(), nullable=True),
        sa.Column("student_number", sa.Integer(), nullable=True),
        sa.Column("storage_key", sa.String(length=512), nullable=False),
        sa.Column("original_filename", sa.String(length=255), nullable=False),
        sa.Column("mime_type", sa.String(length=255), nullable=False),
        sa.Column("file_size", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("progress_percent", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("provider_name", sa.String(length=64), nullable=True),
        sa.Column("error_code", sa.String(length=64), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("error_metadata_json", sa.JSON(), nullable=True),
        sa.Column("execution_log_json", sa.JSON(), nullable=True),
        sa.Column("leased_by_worker", sa.String(length=128), nullable=True),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("heartbeat_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("retry_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("max_retries", sa.Integer(), nullable=False, server_default="3"),
        sa.Column("timeout_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "(resource_library_item_id IS NOT NULL AND student_work_submission_id IS NULL) "
            "OR (resource_library_item_id IS NULL AND student_work_submission_id IS NOT NULL)",
            name="ck_teacher_assist_extraction_jobs_artifact_ref",
        ),
        sa.ForeignKeyConstraint(["assignment_id"], ["assignments.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["class_id"], ["classes.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["grading_period_id"], ["grading_periods.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["resource_library_item_id"], ["resource_library_items.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["school_year_id"], ["school_years.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["student_work_submission_id"], ["assignment_student_work_submissions.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["subject_id"], ["subjects.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["teacher_user_id"], ["users.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    for column_name in (
        "tenant_id",
        "teacher_user_id",
        "artifact_type",
        "resource_library_item_id",
        "student_work_submission_id",
        "assignment_id",
        "school_year_id",
        "grading_period_id",
        "class_id",
        "subject_id",
        "status",
    ):
        op.create_index(
            op.f(f"ix_teacher_assist_extraction_jobs_{column_name}"),
            "teacher_assist_extraction_jobs",
            [column_name],
            unique=False,
        )

    op.create_table(
        "teacher_assist_extracted_text_records",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("teacher_user_id", sa.Uuid(), nullable=False),
        sa.Column("extraction_job_id", sa.Uuid(), nullable=False),
        sa.Column("artifact_type", sa.Text(), nullable=False),
        sa.Column("resource_library_item_id", sa.Uuid(), nullable=True),
        sa.Column("student_work_submission_id", sa.Uuid(), nullable=True),
        sa.Column("assignment_id", sa.Uuid(), nullable=True),
        sa.Column("school_year_id", sa.Uuid(), nullable=True),
        sa.Column("grading_period_id", sa.Uuid(), nullable=True),
        sa.Column("class_id", sa.Uuid(), nullable=True),
        sa.Column("subject_id", sa.Uuid(), nullable=True),
        sa.Column("student_number", sa.Integer(), nullable=True),
        sa.Column("extracted_text", sa.Text(), nullable=False),
        sa.Column("preview_text", sa.Text(), nullable=False),
        sa.Column("text_char_count", sa.Integer(), nullable=False),
        sa.Column("pii_flagged", sa.Boolean(), nullable=False, server_default="0"),
        sa.Column("redaction_applied", sa.Boolean(), nullable=False, server_default="0"),
        sa.Column("metadata_json", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "(resource_library_item_id IS NOT NULL AND student_work_submission_id IS NULL) "
            "OR (resource_library_item_id IS NULL AND student_work_submission_id IS NOT NULL)",
            name="ck_teacher_assist_extracted_text_records_artifact_ref",
        ),
        sa.ForeignKeyConstraint(["assignment_id"], ["assignments.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["class_id"], ["classes.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["extraction_job_id"], ["teacher_assist_extraction_jobs.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["grading_period_id"], ["grading_periods.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["resource_library_item_id"], ["resource_library_items.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["school_year_id"], ["school_years.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["student_work_submission_id"], ["assignment_student_work_submissions.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["subject_id"], ["subjects.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["teacher_user_id"], ["users.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    for column_name in (
        "tenant_id",
        "teacher_user_id",
        "extraction_job_id",
        "resource_library_item_id",
        "student_work_submission_id",
        "assignment_id",
        "school_year_id",
        "grading_period_id",
        "class_id",
        "subject_id",
    ):
        op.create_index(
            op.f(f"ix_teacher_assist_extracted_text_records_{column_name}"),
            "teacher_assist_extracted_text_records",
            [column_name],
            unique=False,
        )


def downgrade() -> None:
    for column_name in (
        "subject_id",
        "class_id",
        "grading_period_id",
        "school_year_id",
        "assignment_id",
        "student_work_submission_id",
        "resource_library_item_id",
        "extraction_job_id",
        "teacher_user_id",
        "tenant_id",
    ):
        op.drop_index(
            op.f(f"ix_teacher_assist_extracted_text_records_{column_name}"),
            table_name="teacher_assist_extracted_text_records",
        )
    op.drop_table("teacher_assist_extracted_text_records")

    for column_name in (
        "status",
        "subject_id",
        "class_id",
        "grading_period_id",
        "school_year_id",
        "assignment_id",
        "student_work_submission_id",
        "resource_library_item_id",
        "artifact_type",
        "teacher_user_id",
        "tenant_id",
    ):
        op.drop_index(
            op.f(f"ix_teacher_assist_extraction_jobs_{column_name}"),
            table_name="teacher_assist_extraction_jobs",
        )
    op.drop_table("teacher_assist_extraction_jobs")
