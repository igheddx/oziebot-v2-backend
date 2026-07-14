"""add teacher assist assignment print packets

Revision ID: 045_teacher_assist_print_packets
Revises: 044_teacher_assist_assignments
Create Date: 2026-05-28 00:20:00.000000
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Union

from alembic import op
import sqlalchemy as sa


revision: str = "045_teacher_assist_print_packets"
down_revision: Union[str, None] = "044_teacher_assist_assignments"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "assignment_print_packets",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("teacher_user_id", sa.Uuid(), nullable=False),
        sa.Column("assignment_id", sa.Uuid(), nullable=False),
        sa.Column("class_id", sa.Uuid(), nullable=False),
        sa.Column("school_year_id", sa.Uuid(), nullable=False),
        sa.Column("grading_period_id", sa.Uuid(), nullable=True),
        sa.Column("subject_id", sa.Uuid(), nullable=False),
        sa.Column("packet_status", sa.String(length=32), nullable=False),
        sa.Column("pages_per_student", sa.Integer(), nullable=False),
        sa.Column("student_count", sa.Integer(), nullable=False),
        sa.Column("template_type", sa.String(length=64), nullable=False),
        sa.Column("output_format", sa.String(length=32), nullable=False),
        sa.Column("storage_key", sa.String(length=512), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["assignment_id"], ["assignments.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["class_id"], ["classes.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["grading_period_id"], ["grading_periods.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["school_year_id"], ["school_years.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["subject_id"], ["subjects.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["teacher_user_id"], ["users.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        op.f("ix_assignment_print_packets_assignment_id"),
        "assignment_print_packets",
        ["assignment_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_assignment_print_packets_class_id"),
        "assignment_print_packets",
        ["class_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_assignment_print_packets_grading_period_id"),
        "assignment_print_packets",
        ["grading_period_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_assignment_print_packets_school_year_id"),
        "assignment_print_packets",
        ["school_year_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_assignment_print_packets_subject_id"),
        "assignment_print_packets",
        ["subject_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_assignment_print_packets_teacher_user_id"),
        "assignment_print_packets",
        ["teacher_user_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_assignment_print_packets_tenant_id"),
        "assignment_print_packets",
        ["tenant_id"],
        unique=False,
    )

    op.create_table(
        "assignment_print_pages",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("packet_id", sa.Uuid(), nullable=False),
        sa.Column("assignment_id", sa.Uuid(), nullable=False),
        sa.Column("student_number", sa.Integer(), nullable=False),
        sa.Column("page_number", sa.Integer(), nullable=False),
        sa.Column("qr_payload_json", sa.JSON(), nullable=False),
        sa.Column("qr_token", sa.String(length=128), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["assignment_id"], ["assignments.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["packet_id"], ["assignment_print_packets.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        op.f("ix_assignment_print_pages_assignment_id"),
        "assignment_print_pages",
        ["assignment_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_assignment_print_pages_packet_id"),
        "assignment_print_pages",
        ["packet_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_assignment_print_pages_qr_token"),
        "assignment_print_pages",
        ["qr_token"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(op.f("ix_assignment_print_pages_qr_token"), table_name="assignment_print_pages")
    op.drop_index(op.f("ix_assignment_print_pages_packet_id"), table_name="assignment_print_pages")
    op.drop_index(
        op.f("ix_assignment_print_pages_assignment_id"), table_name="assignment_print_pages"
    )
    op.drop_table("assignment_print_pages")

    op.drop_index(
        op.f("ix_assignment_print_packets_tenant_id"),
        table_name="assignment_print_packets",
    )
    op.drop_index(
        op.f("ix_assignment_print_packets_teacher_user_id"),
        table_name="assignment_print_packets",
    )
    op.drop_index(
        op.f("ix_assignment_print_packets_subject_id"),
        table_name="assignment_print_packets",
    )
    op.drop_index(
        op.f("ix_assignment_print_packets_school_year_id"),
        table_name="assignment_print_packets",
    )
    op.drop_index(
        op.f("ix_assignment_print_packets_grading_period_id"),
        table_name="assignment_print_packets",
    )
    op.drop_index(
        op.f("ix_assignment_print_packets_class_id"),
        table_name="assignment_print_packets",
    )
    op.drop_index(
        op.f("ix_assignment_print_packets_assignment_id"),
        table_name="assignment_print_packets",
    )
    op.drop_table("assignment_print_packets")
