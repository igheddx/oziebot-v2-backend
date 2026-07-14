"""add teacher assist assignments foundation

Revision ID: 044_teacher_assist_assignments
Revises: 043_teacher_assist_worker_foundation
Create Date: 2026-05-27 23:40:00.000000
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Union

from alembic import op
import sqlalchemy as sa


revision: str = "044_teacher_assist_assignments"
down_revision: Union[str, None] = "043_teacher_assist_worker_foundation"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "assignments",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("teacher_user_id", sa.Uuid(), nullable=False),
        sa.Column("school_year_id", sa.Uuid(), nullable=False),
        sa.Column("grading_period_id", sa.Uuid(), nullable=True),
        sa.Column("class_id", sa.Uuid(), nullable=False),
        sa.Column("subject_id", sa.Uuid(), nullable=False),
        sa.Column("title", sa.String(length=160), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("assignment_type", sa.String(length=32), nullable=False),
        sa.Column("due_date", sa.Date(), nullable=True),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("instructions", sa.Text(), nullable=True),
        sa.Column("rubric_json", sa.JSON(), nullable=True),
        sa.Column("source_plan_id", sa.Uuid(), nullable=True),
        sa.Column("source_context_json", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["class_id"], ["classes.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["grading_period_id"], ["grading_periods.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["school_year_id"], ["school_years.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["source_plan_id"], ["weekly_plans.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["subject_id"], ["subjects.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["teacher_user_id"], ["users.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_assignments_class_id"), "assignments", ["class_id"], unique=False)
    op.create_index(
        op.f("ix_assignments_grading_period_id"), "assignments", ["grading_period_id"], unique=False
    )
    op.create_index(
        op.f("ix_assignments_school_year_id"), "assignments", ["school_year_id"], unique=False
    )
    op.create_index(
        op.f("ix_assignments_source_plan_id"), "assignments", ["source_plan_id"], unique=False
    )
    op.create_index(op.f("ix_assignments_subject_id"), "assignments", ["subject_id"], unique=False)
    op.create_index(
        op.f("ix_assignments_teacher_user_id"), "assignments", ["teacher_user_id"], unique=False
    )
    op.create_index(op.f("ix_assignments_tenant_id"), "assignments", ["tenant_id"], unique=False)
    op.create_index(
        "ix_assignments_teacher_user_status",
        "assignments",
        ["teacher_user_id", "status"],
        unique=False,
    )
    op.create_index(
        "ix_assignments_teacher_user_assignment_type",
        "assignments",
        ["teacher_user_id", "assignment_type"],
        unique=False,
    )

    op.create_table(
        "assignment_standards",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("assignment_id", sa.Uuid(), nullable=False),
        sa.Column("standard_id", sa.Uuid(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["assignment_id"], ["assignments.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["standard_id"], ["standards.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("assignment_id", "standard_id", name="uq_assignment_standard"),
    )
    op.create_index(
        op.f("ix_assignment_standards_assignment_id"),
        "assignment_standards",
        ["assignment_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_assignment_standards_standard_id"),
        "assignment_standards",
        ["standard_id"],
        unique=False,
    )

    op.create_table(
        "assignment_resources",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("assignment_id", sa.Uuid(), nullable=False),
        sa.Column("resource_library_item_id", sa.Uuid(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["assignment_id"], ["assignments.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(
            ["resource_library_item_id"], ["resource_library_items.id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "assignment_id",
            "resource_library_item_id",
            name="uq_assignment_resource",
        ),
    )
    op.create_index(
        op.f("ix_assignment_resources_assignment_id"),
        "assignment_resources",
        ["assignment_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_assignment_resources_resource_library_item_id"),
        "assignment_resources",
        ["resource_library_item_id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        op.f("ix_assignment_resources_resource_library_item_id"),
        table_name="assignment_resources",
    )
    op.drop_index(op.f("ix_assignment_resources_assignment_id"), table_name="assignment_resources")
    op.drop_table("assignment_resources")

    op.drop_index(op.f("ix_assignment_standards_standard_id"), table_name="assignment_standards")
    op.drop_index(op.f("ix_assignment_standards_assignment_id"), table_name="assignment_standards")
    op.drop_table("assignment_standards")

    op.drop_index("ix_assignments_teacher_user_assignment_type", table_name="assignments")
    op.drop_index("ix_assignments_teacher_user_status", table_name="assignments")
    op.drop_index(op.f("ix_assignments_tenant_id"), table_name="assignments")
    op.drop_index(op.f("ix_assignments_teacher_user_id"), table_name="assignments")
    op.drop_index(op.f("ix_assignments_subject_id"), table_name="assignments")
    op.drop_index(op.f("ix_assignments_source_plan_id"), table_name="assignments")
    op.drop_index(op.f("ix_assignments_school_year_id"), table_name="assignments")
    op.drop_index(op.f("ix_assignments_grading_period_id"), table_name="assignments")
    op.drop_index(op.f("ix_assignments_class_id"), table_name="assignments")
    op.drop_table("assignments")
