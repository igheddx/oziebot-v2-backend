"""add teacher assist gradebook commit foundation

Revision ID: 053_teacher_assist_gradebook_commit_foundation
Revises: 052_teacher_assist_export_artifacts
Create Date: 2026-05-28 20:00:00.000000
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Union

from alembic import op
import sqlalchemy as sa


revision: str = "053_teacher_assist_gradebook_commit_foundation"
down_revision: Union[str, None] = "052_teacher_assist_export_artifacts"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "teacher_assist_assignment_grade_records",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("teacher_user_id", sa.Uuid(), nullable=False),
        sa.Column("assignment_id", sa.Uuid(), nullable=False),
        sa.Column("student_work_submission_id", sa.Uuid(), nullable=False),
        sa.Column("grading_review_id", sa.Uuid(), nullable=False),
        sa.Column("student_number", sa.Integer(), nullable=False),
        sa.Column("school_year_id", sa.Uuid(), nullable=False),
        sa.Column("grading_period_id", sa.Uuid(), nullable=True),
        sa.Column("class_id", sa.Uuid(), nullable=False),
        sa.Column("subject_id", sa.Uuid(), nullable=False),
        sa.Column("record_status", sa.String(length=32), nullable=False),
        sa.Column("current_commit_id", sa.Uuid(), nullable=True),
        sa.Column("committed_score", sa.Float(), nullable=True),
        sa.Column("max_score", sa.Float(), nullable=True),
        sa.Column("committed_feedback", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["assignment_id"], ["assignments.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["class_id"], ["classes.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["grading_period_id"], ["grading_periods.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["grading_review_id"], ["assignment_grading_reviews.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["school_year_id"], ["school_years.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(
            ["student_work_submission_id"],
            ["assignment_student_work_submissions.id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(["subject_id"], ["subjects.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["teacher_user_id"], ["users.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("grading_review_id"),
        sa.UniqueConstraint("student_work_submission_id"),
    )
    for column_name in (
        "tenant_id",
        "teacher_user_id",
        "assignment_id",
        "student_work_submission_id",
        "grading_review_id",
        "school_year_id",
        "grading_period_id",
        "class_id",
        "subject_id",
        "record_status",
        "current_commit_id",
    ):
        op.create_index(
            op.f(f"ix_teacher_assist_assignment_grade_records_{column_name}"),
            "teacher_assist_assignment_grade_records",
            [column_name],
            unique=False,
        )

    op.create_table(
        "teacher_assist_assignment_gradebook_commits",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("teacher_user_id", sa.Uuid(), nullable=False),
        sa.Column("grade_record_id", sa.Uuid(), nullable=False),
        sa.Column("assignment_id", sa.Uuid(), nullable=False),
        sa.Column("student_work_submission_id", sa.Uuid(), nullable=False),
        sa.Column("grading_review_id", sa.Uuid(), nullable=False),
        sa.Column("student_number", sa.Integer(), nullable=False),
        sa.Column("school_year_id", sa.Uuid(), nullable=False),
        sa.Column("grading_period_id", sa.Uuid(), nullable=True),
        sa.Column("class_id", sa.Uuid(), nullable=False),
        sa.Column("subject_id", sa.Uuid(), nullable=False),
        sa.Column("commit_type", sa.String(length=32), nullable=False),
        sa.Column("commit_status", sa.String(length=32), nullable=False),
        sa.Column("committed_score", sa.Float(), nullable=True),
        sa.Column("max_score", sa.Float(), nullable=True),
        sa.Column("committed_feedback", sa.Text(), nullable=True),
        sa.Column("teacher_confirmation_checkpoint_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("reason", sa.Text(), nullable=True),
        sa.Column("supersedes_commit_id", sa.Uuid(), nullable=True),
        sa.Column("reversed_by_commit_id", sa.Uuid(), nullable=True),
        sa.Column("audit_metadata_json", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["assignment_id"], ["assignments.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["class_id"], ["classes.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["grade_record_id"], ["teacher_assist_assignment_grade_records.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["grading_period_id"], ["grading_periods.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["grading_review_id"], ["assignment_grading_reviews.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["reversed_by_commit_id"], ["teacher_assist_assignment_gradebook_commits.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["school_year_id"], ["school_years.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(
            ["student_work_submission_id"],
            ["assignment_student_work_submissions.id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(["subject_id"], ["subjects.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["supersedes_commit_id"], ["teacher_assist_assignment_gradebook_commits.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["teacher_user_id"], ["users.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    for column_name in (
        "tenant_id",
        "teacher_user_id",
        "grade_record_id",
        "assignment_id",
        "student_work_submission_id",
        "grading_review_id",
        "school_year_id",
        "grading_period_id",
        "class_id",
        "subject_id",
        "commit_type",
        "commit_status",
        "supersedes_commit_id",
        "reversed_by_commit_id",
    ):
        op.create_index(
            op.f(f"ix_teacher_assist_assignment_gradebook_commits_{column_name}"),
            "teacher_assist_assignment_gradebook_commits",
            [column_name],
            unique=False,
        )

    op.create_foreign_key(
        "fk_teacher_assist_assignment_grade_records_current_commit_id",
        "teacher_assist_assignment_grade_records",
        "teacher_assist_assignment_gradebook_commits",
        ["current_commit_id"],
        ["id"],
        ondelete="SET NULL",
    )

    op.create_table(
        "teacher_assist_assignment_gradebook_audit_events",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("teacher_user_id", sa.Uuid(), nullable=False),
        sa.Column("grade_record_id", sa.Uuid(), nullable=True),
        sa.Column("gradebook_commit_id", sa.Uuid(), nullable=True),
        sa.Column("assignment_id", sa.Uuid(), nullable=False),
        sa.Column("student_number", sa.Integer(), nullable=False),
        sa.Column("event_type", sa.String(length=64), nullable=False),
        sa.Column("summary_text", sa.Text(), nullable=False),
        sa.Column("details_json", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["assignment_id"], ["assignments.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["grade_record_id"], ["teacher_assist_assignment_grade_records.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(
            ["gradebook_commit_id"],
            ["teacher_assist_assignment_gradebook_commits.id"],
            ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(["teacher_user_id"], ["users.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    for column_name in (
        "tenant_id",
        "teacher_user_id",
        "grade_record_id",
        "gradebook_commit_id",
        "assignment_id",
        "event_type",
    ):
        op.create_index(
            op.f(f"ix_teacher_assist_assignment_gradebook_audit_events_{column_name}"),
            "teacher_assist_assignment_gradebook_audit_events",
            [column_name],
            unique=False,
        )


def downgrade() -> None:
    for column_name in (
        "event_type",
        "assignment_id",
        "gradebook_commit_id",
        "grade_record_id",
        "teacher_user_id",
        "tenant_id",
    ):
        op.drop_index(
            op.f(f"ix_teacher_assist_assignment_gradebook_audit_events_{column_name}"),
            table_name="teacher_assist_assignment_gradebook_audit_events",
        )
    op.drop_table("teacher_assist_assignment_gradebook_audit_events")

    op.drop_constraint(
        "fk_teacher_assist_assignment_grade_records_current_commit_id",
        "teacher_assist_assignment_grade_records",
        type_="foreignkey",
    )

    for column_name in (
        "reversed_by_commit_id",
        "supersedes_commit_id",
        "commit_status",
        "commit_type",
        "subject_id",
        "class_id",
        "grading_period_id",
        "school_year_id",
        "grading_review_id",
        "student_work_submission_id",
        "assignment_id",
        "grade_record_id",
        "teacher_user_id",
        "tenant_id",
    ):
        op.drop_index(
            op.f(f"ix_teacher_assist_assignment_gradebook_commits_{column_name}"),
            table_name="teacher_assist_assignment_gradebook_commits",
        )
    op.drop_table("teacher_assist_assignment_gradebook_commits")

    for column_name in (
        "current_commit_id",
        "record_status",
        "subject_id",
        "class_id",
        "grading_period_id",
        "school_year_id",
        "grading_review_id",
        "student_work_submission_id",
        "assignment_id",
        "teacher_user_id",
        "tenant_id",
    ):
        op.drop_index(
            op.f(f"ix_teacher_assist_assignment_grade_records_{column_name}"),
            table_name="teacher_assist_assignment_grade_records",
        )
    op.drop_table("teacher_assist_assignment_grade_records")
