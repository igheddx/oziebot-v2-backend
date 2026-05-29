"""add teacher assist mastery matrix foundation

Revision ID: 054_teacher_assist_mastery_matrix_foundation
Revises: 053_teacher_assist_gradebook_commit_foundation
Create Date: 2026-05-28 22:00:00.000000
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Union

from alembic import op
import sqlalchemy as sa


revision: str = "054_teacher_assist_mastery_matrix_foundation"
down_revision: Union[str, None] = "053_teacher_assist_gradebook_commit_foundation"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "teacher_assist_mastery_matrices",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("owner_user_id", sa.Uuid(), nullable=False),
        sa.Column("school_year_id", sa.Uuid(), nullable=False),
        sa.Column("grading_period_id", sa.Uuid(), nullable=True),
        sa.Column("class_id", sa.Uuid(), nullable=False),
        sa.Column("subject_id", sa.Uuid(), nullable=False),
        sa.Column("title", sa.String(length=255), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["class_id"], ["classes.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["grading_period_id"], ["grading_periods.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["owner_user_id"], ["users.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["school_year_id"], ["school_years.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["subject_id"], ["subjects.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    for column_name in (
        "tenant_id",
        "owner_user_id",
        "school_year_id",
        "grading_period_id",
        "class_id",
        "subject_id",
        "status",
    ):
        op.create_index(
            op.f(f"ix_teacher_assist_mastery_matrices_{column_name}"),
            "teacher_assist_mastery_matrices",
            [column_name],
            unique=False,
        )

    op.create_table(
        "teacher_assist_mastery_matrix_standards",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("mastery_matrix_id", sa.Uuid(), nullable=False),
        sa.Column("standard_id", sa.Uuid(), nullable=False),
        sa.Column("display_order", sa.Integer(), nullable=False),
        sa.Column("target_mastery_level", sa.String(length=32), nullable=False),
        sa.Column("assessment_count", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["mastery_matrix_id"],
            ["teacher_assist_mastery_matrices.id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(["standard_id"], ["standards.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("mastery_matrix_id", "standard_id", name="uq_mastery_matrix_standard"),
    )
    for column_name in ("tenant_id", "mastery_matrix_id", "standard_id"):
        op.create_index(
            op.f(f"ix_teacher_assist_mastery_matrix_standards_{column_name}"),
            "teacher_assist_mastery_matrix_standards",
            [column_name],
            unique=False,
        )

    op.create_table(
        "teacher_assist_mastery_evaluations",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("owner_user_id", sa.Uuid(), nullable=False),
        sa.Column("mastery_matrix_id", sa.Uuid(), nullable=False),
        sa.Column("student_number", sa.Integer(), nullable=False),
        sa.Column("standard_id", sa.Uuid(), nullable=False),
        sa.Column("evaluation_status", sa.String(length=32), nullable=False),
        sa.Column("mastery_level", sa.String(length=32), nullable=False),
        sa.Column("confidence_level", sa.String(length=32), nullable=True),
        sa.Column("evidence_source_type", sa.String(length=64), nullable=True),
        sa.Column("evidence_source_id", sa.Uuid(), nullable=True),
        sa.Column("teacher_notes", sa.Text(), nullable=True),
        sa.Column("confirmed_by_user_id", sa.Uuid(), nullable=True),
        sa.Column("confirmed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("current_commit_id", sa.Uuid(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["confirmed_by_user_id"], ["users.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(
            ["mastery_matrix_id"],
            ["teacher_assist_mastery_matrices.id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(["owner_user_id"], ["users.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["standard_id"], ["standards.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "mastery_matrix_id",
            "student_number",
            "standard_id",
            name="uq_mastery_evaluation_matrix_student_standard",
        ),
    )
    for column_name in (
        "tenant_id",
        "owner_user_id",
        "mastery_matrix_id",
        "student_number",
        "standard_id",
        "evaluation_status",
        "current_commit_id",
    ):
        op.create_index(
            op.f(f"ix_teacher_assist_mastery_evaluations_{column_name}"),
            "teacher_assist_mastery_evaluations",
            [column_name],
            unique=False,
        )

    op.create_table(
        "teacher_assist_mastery_commit_history",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("owner_user_id", sa.Uuid(), nullable=False),
        sa.Column("mastery_evaluation_id", sa.Uuid(), nullable=False),
        sa.Column("mastery_matrix_id", sa.Uuid(), nullable=False),
        sa.Column("student_number", sa.Integer(), nullable=False),
        sa.Column("standard_id", sa.Uuid(), nullable=False),
        sa.Column("commit_type", sa.String(length=32), nullable=False),
        sa.Column("commit_status", sa.String(length=32), nullable=False),
        sa.Column("previous_mastery_level", sa.String(length=32), nullable=True),
        sa.Column("new_mastery_level", sa.String(length=32), nullable=False),
        sa.Column("confidence_level", sa.String(length=32), nullable=True),
        sa.Column("evidence_source_type", sa.String(length=64), nullable=True),
        sa.Column("evidence_source_id", sa.Uuid(), nullable=True),
        sa.Column("teacher_notes", sa.Text(), nullable=True),
        sa.Column("commit_reason", sa.Text(), nullable=True),
        sa.Column("supersedes_commit_id", sa.Uuid(), nullable=True),
        sa.Column("reversed_by_commit_id", sa.Uuid(), nullable=True),
        sa.Column("reversed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("reversed_by_user_id", sa.Uuid(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["mastery_evaluation_id"],
            ["teacher_assist_mastery_evaluations.id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["mastery_matrix_id"],
            ["teacher_assist_mastery_matrices.id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(["owner_user_id"], ["users.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(
            ["reversed_by_commit_id"],
            ["teacher_assist_mastery_commit_history.id"],
            ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(["reversed_by_user_id"], ["users.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["standard_id"], ["standards.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(
            ["supersedes_commit_id"],
            ["teacher_assist_mastery_commit_history.id"],
            ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    for column_name in (
        "tenant_id",
        "owner_user_id",
        "mastery_evaluation_id",
        "mastery_matrix_id",
        "student_number",
        "standard_id",
        "commit_type",
        "commit_status",
        "supersedes_commit_id",
        "reversed_by_commit_id",
    ):
        op.create_index(
            op.f(f"ix_teacher_assist_mastery_commit_history_{column_name}"),
            "teacher_assist_mastery_commit_history",
            [column_name],
            unique=False,
        )

    op.create_foreign_key(
        "fk_teacher_assist_mastery_evaluations_current_commit_id",
        "teacher_assist_mastery_evaluations",
        "teacher_assist_mastery_commit_history",
        ["current_commit_id"],
        ["id"],
        ondelete="SET NULL",
    )

    op.create_table(
        "teacher_assist_mastery_audit_events",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("owner_user_id", sa.Uuid(), nullable=False),
        sa.Column("mastery_matrix_id", sa.Uuid(), nullable=False),
        sa.Column("mastery_evaluation_id", sa.Uuid(), nullable=True),
        sa.Column("mastery_commit_id", sa.Uuid(), nullable=True),
        sa.Column("student_number", sa.Integer(), nullable=True),
        sa.Column("standard_id", sa.Uuid(), nullable=True),
        sa.Column("event_type", sa.String(length=64), nullable=False),
        sa.Column("summary_text", sa.Text(), nullable=False),
        sa.Column("details_json", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["mastery_commit_id"],
            ["teacher_assist_mastery_commit_history.id"],
            ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(
            ["mastery_evaluation_id"],
            ["teacher_assist_mastery_evaluations.id"],
            ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(
            ["mastery_matrix_id"],
            ["teacher_assist_mastery_matrices.id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(["owner_user_id"], ["users.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["standard_id"], ["standards.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    for column_name in (
        "tenant_id",
        "owner_user_id",
        "mastery_matrix_id",
        "mastery_evaluation_id",
        "mastery_commit_id",
        "event_type",
    ):
        op.create_index(
            op.f(f"ix_teacher_assist_mastery_audit_events_{column_name}"),
            "teacher_assist_mastery_audit_events",
            [column_name],
            unique=False,
        )


def downgrade() -> None:
    for column_name in (
        "event_type",
        "mastery_commit_id",
        "mastery_evaluation_id",
        "mastery_matrix_id",
        "owner_user_id",
        "tenant_id",
    ):
        op.drop_index(
            op.f(f"ix_teacher_assist_mastery_audit_events_{column_name}"),
            table_name="teacher_assist_mastery_audit_events",
        )
    op.drop_table("teacher_assist_mastery_audit_events")

    op.drop_constraint(
        "fk_teacher_assist_mastery_evaluations_current_commit_id",
        "teacher_assist_mastery_evaluations",
        type_="foreignkey",
    )

    for column_name in (
        "reversed_by_commit_id",
        "supersedes_commit_id",
        "commit_status",
        "commit_type",
        "standard_id",
        "student_number",
        "mastery_matrix_id",
        "mastery_evaluation_id",
        "owner_user_id",
        "tenant_id",
    ):
        op.drop_index(
            op.f(f"ix_teacher_assist_mastery_commit_history_{column_name}"),
            table_name="teacher_assist_mastery_commit_history",
        )
    op.drop_table("teacher_assist_mastery_commit_history")

    for column_name in (
        "current_commit_id",
        "evaluation_status",
        "standard_id",
        "student_number",
        "mastery_matrix_id",
        "owner_user_id",
        "tenant_id",
    ):
        op.drop_index(
            op.f(f"ix_teacher_assist_mastery_evaluations_{column_name}"),
            table_name="teacher_assist_mastery_evaluations",
        )
    op.drop_table("teacher_assist_mastery_evaluations")

    for column_name in ("standard_id", "mastery_matrix_id", "tenant_id"):
        op.drop_index(
            op.f(f"ix_teacher_assist_mastery_matrix_standards_{column_name}"),
            table_name="teacher_assist_mastery_matrix_standards",
        )
    op.drop_table("teacher_assist_mastery_matrix_standards")

    for column_name in (
        "status",
        "subject_id",
        "class_id",
        "grading_period_id",
        "school_year_id",
        "owner_user_id",
        "tenant_id",
    ):
        op.drop_index(
            op.f(f"ix_teacher_assist_mastery_matrices_{column_name}"),
            table_name="teacher_assist_mastery_matrices",
        )
    op.drop_table("teacher_assist_mastery_matrices")
