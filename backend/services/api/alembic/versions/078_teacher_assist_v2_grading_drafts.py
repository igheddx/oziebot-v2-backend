"""TeacherAssist v2 AI grading draft generation."""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "078_teacher_assist_v2_grading_drafts"
down_revision = "077_teacher_assist_v2_submission_intake"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "teacher_assist_v2_grading_jobs",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("teacher_user_id", sa.Uuid(), nullable=False),
        sa.Column("assignment_id", sa.Uuid(), nullable=False),
        sa.Column("student_submission_id", sa.Uuid(), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("provider", sa.String(length=64), nullable=False),
        sa.Column("model", sa.String(length=128), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["assignment_id"], ["teacher_assist_v2_assignments.id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(
            ["student_submission_id"],
            ["teacher_assist_v2_student_submissions.id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(["teacher_user_id"], ["users.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_ta_v2_grading_jobs_submission",
        "teacher_assist_v2_grading_jobs",
        ["student_submission_id"],
    )

    op.create_table(
        "teacher_assist_v2_grading_drafts",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("teacher_user_id", sa.Uuid(), nullable=False),
        sa.Column("assignment_id", sa.Uuid(), nullable=False),
        sa.Column("student_submission_id", sa.Uuid(), nullable=False),
        sa.Column("grading_job_id", sa.Uuid(), nullable=False),
        sa.Column("score", sa.Float(), nullable=False),
        sa.Column("max_score", sa.Float(), nullable=False),
        sa.Column("percentage", sa.Float(), nullable=False),
        sa.Column("rubric_json", sa.JSON(), nullable=False),
        sa.Column("teacher_comment_draft", sa.Text(), nullable=False),
        sa.Column("strengths", sa.JSON(), nullable=False),
        sa.Column("improvements", sa.JSON(), nullable=False),
        sa.Column("objective_evidence", sa.JSON(), nullable=False),
        sa.Column("confidence_score", sa.Float(), nullable=False),
        sa.Column("provider", sa.String(length=64), nullable=False),
        sa.Column("model", sa.String(length=128), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["assignment_id"], ["teacher_assist_v2_assignments.id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(
            ["grading_job_id"], ["teacher_assist_v2_grading_jobs.id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(
            ["student_submission_id"],
            ["teacher_assist_v2_student_submissions.id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(["teacher_user_id"], ["users.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("grading_job_id"),
    )
    op.create_index(
        "ix_ta_v2_grading_drafts_submission",
        "teacher_assist_v2_grading_drafts",
        ["student_submission_id"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_ta_v2_grading_drafts_submission", table_name="teacher_assist_v2_grading_drafts"
    )
    op.drop_table("teacher_assist_v2_grading_drafts")
    op.drop_index("ix_ta_v2_grading_jobs_submission", table_name="teacher_assist_v2_grading_jobs")
    op.drop_table("teacher_assist_v2_grading_jobs")
