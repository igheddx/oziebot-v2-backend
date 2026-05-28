"""add teacher assist grading review foundation

Revision ID: 047_teacher_assist_grading_review_foundation
Revises: 046_teacher_assist_student_work_intake
Create Date: 2026-05-28 06:55:00.000000
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Union

from alembic import op
import sqlalchemy as sa


revision: str = "047_teacher_assist_grading_review_foundation"
down_revision: Union[str, None] = "046_teacher_assist_student_work_intake"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "assignment_grading_reviews",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("teacher_user_id", sa.Uuid(), nullable=False),
        sa.Column("assignment_id", sa.Uuid(), nullable=False),
        sa.Column("student_work_submission_id", sa.Uuid(), nullable=False),
        sa.Column("student_number", sa.Integer(), nullable=False),
        sa.Column("school_year_id", sa.Uuid(), nullable=False),
        sa.Column("grading_period_id", sa.Uuid(), nullable=True),
        sa.Column("class_id", sa.Uuid(), nullable=False),
        sa.Column("subject_id", sa.Uuid(), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("review_source", sa.String(length=32), nullable=False),
        sa.Column("provider_name", sa.String(length=64), nullable=True),
        sa.Column("provider_model", sa.String(length=128), nullable=True),
        sa.Column("prompt_version", sa.String(length=64), nullable=True),
        sa.Column("ai_usage_event_id", sa.Uuid(), nullable=True),
        sa.Column("score_suggestion", sa.Float(), nullable=True),
        sa.Column("max_score", sa.Float(), nullable=True),
        sa.Column("feedback_summary", sa.Text(), nullable=True),
        sa.Column("strengths", sa.JSON(), nullable=False),
        sa.Column("improvement_areas", sa.JSON(), nullable=False),
        sa.Column("teacher_notes", sa.Text(), nullable=True),
        sa.Column("teacher_confirmed_score", sa.Float(), nullable=True),
        sa.Column("teacher_confirmed_feedback", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["ai_usage_event_id"], ["teacher_assist_ai_usage_events.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["assignment_id"], ["assignments.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["class_id"], ["classes.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["grading_period_id"], ["grading_periods.id"], ondelete="SET NULL"),
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
        sa.UniqueConstraint("student_work_submission_id"),
    )
    for column_name in (
        "tenant_id",
        "teacher_user_id",
        "assignment_id",
        "student_work_submission_id",
        "school_year_id",
        "grading_period_id",
        "class_id",
        "subject_id",
        "ai_usage_event_id",
    ):
        op.create_index(
            op.f(f"ix_assignment_grading_reviews_{column_name}"),
            "assignment_grading_reviews",
            [column_name],
            unique=False,
        )

    op.create_table(
        "assignment_grading_review_items",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("grading_review_id", sa.Uuid(), nullable=False),
        sa.Column("criterion_title", sa.String(length=160), nullable=False),
        sa.Column("score_suggestion", sa.Float(), nullable=True),
        sa.Column("max_score", sa.Float(), nullable=True),
        sa.Column("feedback_summary", sa.Text(), nullable=True),
        sa.Column("strengths", sa.JSON(), nullable=False),
        sa.Column("improvement_areas", sa.JSON(), nullable=False),
        sa.Column("teacher_notes", sa.Text(), nullable=True),
        sa.Column("sort_order", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["grading_review_id"], ["assignment_grading_reviews.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        op.f("ix_assignment_grading_review_items_grading_review_id"),
        "assignment_grading_review_items",
        ["grading_review_id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        op.f("ix_assignment_grading_review_items_grading_review_id"),
        table_name="assignment_grading_review_items",
    )
    op.drop_table("assignment_grading_review_items")

    for column_name in (
        "ai_usage_event_id",
        "subject_id",
        "class_id",
        "grading_period_id",
        "school_year_id",
        "student_work_submission_id",
        "assignment_id",
        "teacher_user_id",
        "tenant_id",
    ):
        op.drop_index(
            op.f(f"ix_assignment_grading_reviews_{column_name}"),
            table_name="assignment_grading_reviews",
        )
    op.drop_table("assignment_grading_reviews")
