"""add teacher assist student work intake

Revision ID: 046_teacher_assist_student_work_intake
Revises: 045_teacher_assist_print_packets
Create Date: 2026-05-28 05:05:00.000000
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Union

from alembic import op
import sqlalchemy as sa


revision: str = "046_teacher_assist_student_work_intake"
down_revision: Union[str, None] = "045_teacher_assist_print_packets"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "assignment_student_work_submissions",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("teacher_user_id", sa.Uuid(), nullable=False),
        sa.Column("assignment_id", sa.Uuid(), nullable=False),
        sa.Column("assignment_print_packet_id", sa.Uuid(), nullable=True),
        sa.Column("assignment_print_page_id", sa.Uuid(), nullable=True),
        sa.Column("school_year_id", sa.Uuid(), nullable=False),
        sa.Column("grading_period_id", sa.Uuid(), nullable=True),
        sa.Column("class_id", sa.Uuid(), nullable=False),
        sa.Column("subject_id", sa.Uuid(), nullable=False),
        sa.Column("student_number", sa.Integer(), nullable=False),
        sa.Column("original_filename", sa.String(length=255), nullable=False),
        sa.Column("mime_type", sa.String(length=255), nullable=False),
        sa.Column("file_size", sa.Integer(), nullable=False),
        sa.Column("storage_key", sa.String(length=512), nullable=False),
        sa.Column("upload_status", sa.String(length=32), nullable=False),
        sa.Column("processing_status", sa.String(length=32), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["assignment_id"], ["assignments.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(
            ["assignment_print_packet_id"],
            ["assignment_print_packets.id"],
            ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(
            ["assignment_print_page_id"],
            ["assignment_print_pages.id"],
            ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(["class_id"], ["classes.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["grading_period_id"], ["grading_periods.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["school_year_id"], ["school_years.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["subject_id"], ["subjects.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["teacher_user_id"], ["users.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        op.f("ix_assignment_student_work_submissions_assignment_id"),
        "assignment_student_work_submissions",
        ["assignment_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_assignment_student_work_submissions_assignment_print_packet_id"),
        "assignment_student_work_submissions",
        ["assignment_print_packet_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_assignment_student_work_submissions_assignment_print_page_id"),
        "assignment_student_work_submissions",
        ["assignment_print_page_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_assignment_student_work_submissions_class_id"),
        "assignment_student_work_submissions",
        ["class_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_assignment_student_work_submissions_grading_period_id"),
        "assignment_student_work_submissions",
        ["grading_period_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_assignment_student_work_submissions_school_year_id"),
        "assignment_student_work_submissions",
        ["school_year_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_assignment_student_work_submissions_subject_id"),
        "assignment_student_work_submissions",
        ["subject_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_assignment_student_work_submissions_teacher_user_id"),
        "assignment_student_work_submissions",
        ["teacher_user_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_assignment_student_work_submissions_tenant_id"),
        "assignment_student_work_submissions",
        ["tenant_id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        op.f("ix_assignment_student_work_submissions_tenant_id"),
        table_name="assignment_student_work_submissions",
    )
    op.drop_index(
        op.f("ix_assignment_student_work_submissions_teacher_user_id"),
        table_name="assignment_student_work_submissions",
    )
    op.drop_index(
        op.f("ix_assignment_student_work_submissions_subject_id"),
        table_name="assignment_student_work_submissions",
    )
    op.drop_index(
        op.f("ix_assignment_student_work_submissions_school_year_id"),
        table_name="assignment_student_work_submissions",
    )
    op.drop_index(
        op.f("ix_assignment_student_work_submissions_grading_period_id"),
        table_name="assignment_student_work_submissions",
    )
    op.drop_index(
        op.f("ix_assignment_student_work_submissions_class_id"),
        table_name="assignment_student_work_submissions",
    )
    op.drop_index(
        op.f("ix_assignment_student_work_submissions_assignment_print_page_id"),
        table_name="assignment_student_work_submissions",
    )
    op.drop_index(
        op.f("ix_assignment_student_work_submissions_assignment_print_packet_id"),
        table_name="assignment_student_work_submissions",
    )
    op.drop_index(
        op.f("ix_assignment_student_work_submissions_assignment_id"),
        table_name="assignment_student_work_submissions",
    )
    op.drop_table("assignment_student_work_submissions")
