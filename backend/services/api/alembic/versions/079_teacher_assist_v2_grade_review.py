"""TeacherAssist v2 teacher grade review and confirmation."""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "079_teacher_assist_v2_grade_review"
down_revision = "078_teacher_assist_v2_grading_drafts"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "teacher_assist_v2_assignment_grades",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("teacher_user_id", sa.Uuid(), nullable=False),
        sa.Column("assignment_id", sa.Uuid(), nullable=False),
        sa.Column("student_submission_id", sa.Uuid(), nullable=False),
        sa.Column("student_number", sa.Integer(), nullable=True),
        sa.Column("grading_draft_id", sa.Uuid(), nullable=True),
        sa.Column("score", sa.Float(), nullable=False),
        sa.Column("max_score", sa.Float(), nullable=False),
        sa.Column("percentage", sa.Float(), nullable=False),
        sa.Column("rubric_json", sa.JSON(), nullable=False),
        sa.Column("teacher_comment", sa.Text(), nullable=False),
        sa.Column("teacher_override_reason", sa.Text(), nullable=True),
        sa.Column("review_action", sa.String(length=32), nullable=False),
        sa.Column("confirmed_by", sa.Uuid(), nullable=True),
        sa.Column("confirmed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["assignment_id"], ["teacher_assist_v2_assignments.id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(["confirmed_by"], ["users.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(
            ["grading_draft_id"], ["teacher_assist_v2_grading_drafts.id"], ondelete="SET NULL"
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
        "ix_ta_v2_assignment_grades_submission",
        "teacher_assist_v2_assignment_grades",
        ["student_submission_id"],
    )

    op.create_table(
        "teacher_assist_v2_assignment_grade_audit_events",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("assignment_grade_id", sa.Uuid(), nullable=False),
        sa.Column("assignment_id", sa.Uuid(), nullable=False),
        sa.Column("student_submission_id", sa.Uuid(), nullable=False),
        sa.Column("grading_draft_id", sa.Uuid(), nullable=True),
        sa.Column("original_ai_score", sa.Float(), nullable=True),
        sa.Column("original_ai_max_score", sa.Float(), nullable=True),
        sa.Column("final_score", sa.Float(), nullable=False),
        sa.Column("final_max_score", sa.Float(), nullable=False),
        sa.Column("score_difference", sa.Float(), nullable=True),
        sa.Column("teacher_override_reason", sa.Text(), nullable=True),
        sa.Column("review_action", sa.String(length=32), nullable=False),
        sa.Column("teacher_comment", sa.Text(), nullable=False),
        sa.Column("rubric_json", sa.JSON(), nullable=False),
        sa.Column("reviewer_user_id", sa.Uuid(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["assignment_grade_id"], ["teacher_assist_v2_assignment_grades.id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(
            ["assignment_id"], ["teacher_assist_v2_assignments.id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(
            ["grading_draft_id"], ["teacher_assist_v2_grading_drafts.id"], ondelete="SET NULL"
        ),
        sa.ForeignKeyConstraint(["reviewer_user_id"], ["users.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(
            ["student_submission_id"],
            ["teacher_assist_v2_student_submissions.id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_ta_v2_grade_audit_submission",
        "teacher_assist_v2_assignment_grade_audit_events",
        ["student_submission_id"],
    )

    op.create_table(
        "teacher_assist_v2_submission_review_views",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("teacher_user_id", sa.Uuid(), nullable=False),
        sa.Column("student_submission_id", sa.Uuid(), nullable=False),
        sa.Column("first_viewed_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["student_submission_id"],
            ["teacher_assist_v2_student_submissions.id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(["teacher_user_id"], ["users.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "teacher_user_id", "student_submission_id", name="uq_ta_v2_submission_review_view"
        ),
    )


def downgrade() -> None:
    op.drop_table("teacher_assist_v2_submission_review_views")
    op.drop_index(
        "ix_ta_v2_grade_audit_submission",
        table_name="teacher_assist_v2_assignment_grade_audit_events",
    )
    op.drop_table("teacher_assist_v2_assignment_grade_audit_events")
    op.drop_index(
        "ix_ta_v2_assignment_grades_submission", table_name="teacher_assist_v2_assignment_grades"
    )
    op.drop_table("teacher_assist_v2_assignment_grades")
