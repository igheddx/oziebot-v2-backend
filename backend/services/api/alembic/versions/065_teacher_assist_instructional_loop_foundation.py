"""add teacher assist instructional loop foundation (phase 38)

Revision ID: 065_teacher_assist_instructional_loop_foundation
Revises: 064_teacher_assist_instructional_week_foundation
Create Date: 2026-05-30 18:00:00.000000
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Union

from alembic import op
import sqlalchemy as sa


revision: str = "065_teacher_assist_instructional_loop_foundation"
down_revision: Union[str, None] = "064_teacher_assist_instructional_week_foundation"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "teacher_assist_instructional_evidence",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("owner_user_id", sa.Uuid(), nullable=False),
        sa.Column("class_id", sa.Uuid(), nullable=True),
        sa.Column("subject_id", sa.Uuid(), nullable=True),
        sa.Column("instructional_week_id", sa.Uuid(), nullable=True),
        sa.Column("student_identifier", sa.String(length=64), nullable=False),
        sa.Column("objective_id", sa.Uuid(), nullable=True),
        sa.Column("standard_id", sa.Uuid(), nullable=True),
        sa.Column("source_type", sa.String(length=64), nullable=False),
        sa.Column("source_id", sa.Uuid(), nullable=False),
        sa.Column("score", sa.Float(), nullable=True),
        sa.Column("mastery_level", sa.String(length=32), nullable=True),
        sa.Column("teacher_confirmed", sa.Boolean(), nullable=False, server_default="false"),
        sa.Column("teacher_notes", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["class_id"], ["classes.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(
            ["instructional_week_id"], ["instructional_weeks.id"], ondelete="SET NULL"
        ),
        sa.ForeignKeyConstraint(["objective_id"], ["education_objectives.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["owner_user_id"], ["users.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["standard_id"], ["standards.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["subject_id"], ["subjects.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    for column_name in (
        "tenant_id",
        "owner_user_id",
        "class_id",
        "subject_id",
        "instructional_week_id",
        "objective_id",
        "standard_id",
        "source_type",
        "source_id",
        "student_identifier",
    ):
        op.create_index(
            op.f(f"ix_teacher_assist_instructional_evidence_{column_name}"),
            "teacher_assist_instructional_evidence",
            [column_name],
            unique=False,
        )

    op.create_table(
        "teacher_assist_student_support_groups",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("owner_user_id", sa.Uuid(), nullable=False),
        sa.Column("class_id", sa.Uuid(), nullable=False),
        sa.Column("subject_id", sa.Uuid(), nullable=False),
        sa.Column("instructional_week_id", sa.Uuid(), nullable=True),
        sa.Column("objective_id", sa.Uuid(), nullable=True),
        sa.Column("standard_id", sa.Uuid(), nullable=True),
        sa.Column("title", sa.String(length=255), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("suggested_activities_json", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["class_id"], ["classes.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(
            ["instructional_week_id"], ["instructional_weeks.id"], ondelete="SET NULL"
        ),
        sa.ForeignKeyConstraint(["objective_id"], ["education_objectives.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["owner_user_id"], ["users.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["standard_id"], ["standards.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["subject_id"], ["subjects.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    for column_name in (
        "tenant_id",
        "owner_user_id",
        "class_id",
        "subject_id",
        "instructional_week_id",
        "status",
    ):
        op.create_index(
            op.f(f"ix_teacher_assist_student_support_groups_{column_name}"),
            "teacher_assist_student_support_groups",
            [column_name],
            unique=False,
        )

    op.create_table(
        "teacher_assist_student_support_group_members",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("support_group_id", sa.Uuid(), nullable=False),
        sa.Column("student_identifier", sa.String(length=64), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["support_group_id"],
            ["teacher_assist_student_support_groups.id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "support_group_id", "student_identifier", name="uq_support_group_student"
        ),
    )
    op.create_index(
        op.f("ix_teacher_assist_student_support_group_members_support_group_id"),
        "teacher_assist_student_support_group_members",
        ["support_group_id"],
        unique=False,
    )

    op.create_table(
        "teacher_assist_instructional_reflections",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("owner_user_id", sa.Uuid(), nullable=False),
        sa.Column("instructional_week_id", sa.Uuid(), nullable=True),
        sa.Column("class_id", sa.Uuid(), nullable=True),
        sa.Column("subject_id", sa.Uuid(), nullable=True),
        sa.Column("what_worked", sa.Text(), nullable=True),
        sa.Column("what_didnt_work", sa.Text(), nullable=True),
        sa.Column("student_challenges", sa.Text(), nullable=True),
        sa.Column("adjustments_needed", sa.Text(), nullable=True),
        sa.Column("future_recommendations", sa.Text(), nullable=True),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["class_id"], ["classes.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(
            ["instructional_week_id"], ["instructional_weeks.id"], ondelete="SET NULL"
        ),
        sa.ForeignKeyConstraint(["owner_user_id"], ["users.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["subject_id"], ["subjects.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    for column_name in ("tenant_id", "owner_user_id", "instructional_week_id", "status"):
        op.create_index(
            op.f(f"ix_teacher_assist_instructional_reflections_{column_name}"),
            "teacher_assist_instructional_reflections",
            [column_name],
            unique=False,
        )

    op.create_table(
        "teacher_assist_instructional_week_closures",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("owner_user_id", sa.Uuid(), nullable=False),
        sa.Column("instructional_week_id", sa.Uuid(), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("checklist_json", sa.JSON(), nullable=False),
        sa.Column("closed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["instructional_week_id"], ["instructional_weeks.id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(["owner_user_id"], ["users.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("instructional_week_id", "owner_user_id", name="uq_week_closure_owner"),
    )

    op.create_table(
        "teacher_assist_instructional_week_summaries",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("owner_user_id", sa.Uuid(), nullable=False),
        sa.Column("instructional_week_id", sa.Uuid(), nullable=False),
        sa.Column("summary_json", sa.JSON(), nullable=False),
        sa.Column("reusable_next_year", sa.Boolean(), nullable=False, server_default="true"),
        sa.Column("generated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["instructional_week_id"], ["instructional_weeks.id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(["owner_user_id"], ["users.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )

    op.create_table(
        "teacher_assist_reteach_effectiveness_records",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("owner_user_id", sa.Uuid(), nullable=False),
        sa.Column("reteach_plan_id", sa.Uuid(), nullable=False),
        sa.Column("before_mastery_pct", sa.Float(), nullable=True),
        sa.Column("after_mastery_pct", sa.Float(), nullable=True),
        sa.Column("improvement_pct", sa.Float(), nullable=True),
        sa.Column("teacher_reflection", sa.Text(), nullable=True),
        sa.Column("recorded_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["reteach_plan_id"],
            ["teacher_assist_reteach_plans.id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(["owner_user_id"], ["users.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )

    op.add_column(
        "teacher_assist_reteach_plans",
        sa.Column("instructional_week_id", sa.Uuid(), nullable=True),
    )
    op.add_column(
        "teacher_assist_reteach_plans",
        sa.Column("objective_id", sa.Uuid(), nullable=True),
    )
    op.add_column("teacher_assist_reteach_plans", sa.Column("reason", sa.Text(), nullable=True))
    op.add_column(
        "teacher_assist_reteach_plans", sa.Column("expected_outcome", sa.Text(), nullable=True)
    )
    op.create_foreign_key(
        "fk_reteach_plans_instructional_week_id",
        "teacher_assist_reteach_plans",
        "instructional_weeks",
        ["instructional_week_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_foreign_key(
        "fk_reteach_plans_objective_id",
        "teacher_assist_reteach_plans",
        "education_objectives",
        ["objective_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_index(
        op.f("ix_teacher_assist_reteach_plans_instructional_week_id"),
        "teacher_assist_reteach_plans",
        ["instructional_week_id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        op.f("ix_teacher_assist_reteach_plans_instructional_week_id"),
        table_name="teacher_assist_reteach_plans",
    )
    op.drop_constraint(
        "fk_reteach_plans_objective_id", "teacher_assist_reteach_plans", type_="foreignkey"
    )
    op.drop_constraint(
        "fk_reteach_plans_instructional_week_id", "teacher_assist_reteach_plans", type_="foreignkey"
    )
    op.drop_column("teacher_assist_reteach_plans", "expected_outcome")
    op.drop_column("teacher_assist_reteach_plans", "reason")
    op.drop_column("teacher_assist_reteach_plans", "objective_id")
    op.drop_column("teacher_assist_reteach_plans", "instructional_week_id")
    op.drop_table("teacher_assist_reteach_effectiveness_records")
    op.drop_table("teacher_assist_instructional_week_summaries")
    op.drop_table("teacher_assist_instructional_week_closures")
    op.drop_table("teacher_assist_instructional_reflections")
    op.drop_table("teacher_assist_student_support_group_members")
    op.drop_table("teacher_assist_student_support_groups")
    op.drop_table("teacher_assist_instructional_evidence")
