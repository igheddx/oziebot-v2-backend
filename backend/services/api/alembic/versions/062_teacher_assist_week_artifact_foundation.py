"""add week workspace and generated artifact foundation

Revision ID: 062_teacher_assist_week_artifact_foundation
Revises: 061_teacher_assist_current_week_foundation
Create Date: 2026-05-29 23:30:00.000000
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Union

from alembic import op
import sqlalchemy as sa


revision: str = "062_teacher_assist_week_artifact_foundation"
down_revision: Union[str, None] = "061_teacher_assist_current_week_foundation"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("assignments", sa.Column("pacing_guide_id", sa.Uuid(), nullable=True))
    op.add_column("assignments", sa.Column("pacing_guide_period_id", sa.Uuid(), nullable=True))
    op.create_foreign_key(
        "fk_assignments_pacing_guide_id",
        "assignments",
        "pacing_guides",
        ["pacing_guide_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_foreign_key(
        "fk_assignments_pacing_guide_period_id",
        "assignments",
        "pacing_guide_periods",
        ["pacing_guide_period_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_index(
        "ix_assignments_pacing_guide_period_id", "assignments", ["pacing_guide_period_id"]
    )

    op.add_column(
        "teacher_assist_newsletters", sa.Column("pacing_guide_id", sa.Uuid(), nullable=True)
    )
    op.add_column(
        "teacher_assist_newsletters", sa.Column("pacing_guide_period_id", sa.Uuid(), nullable=True)
    )
    op.create_foreign_key(
        "fk_teacher_assist_newsletters_pacing_guide_id",
        "teacher_assist_newsletters",
        "pacing_guides",
        ["pacing_guide_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_foreign_key(
        "fk_teacher_assist_newsletters_pacing_guide_period_id",
        "teacher_assist_newsletters",
        "pacing_guide_periods",
        ["pacing_guide_period_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_index(
        "ix_teacher_assist_newsletters_pacing_guide_period_id",
        "teacher_assist_newsletters",
        ["pacing_guide_period_id"],
    )

    op.add_column("weekly_plans", sa.Column("pacing_guide_id", sa.Uuid(), nullable=True))
    op.create_foreign_key(
        "fk_weekly_plans_pacing_guide_id",
        "weekly_plans",
        "pacing_guides",
        ["pacing_guide_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_index("ix_weekly_plans_pacing_guide_id", "weekly_plans", ["pacing_guide_id"])

    op.create_table(
        "teacher_assist_generated_artifacts",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("created_by_user_id", sa.Uuid(), nullable=False),
        sa.Column("pacing_guide_id", sa.Uuid(), nullable=False),
        sa.Column("pacing_guide_period_id", sa.Uuid(), nullable=False),
        sa.Column("artifact_type", sa.String(length=64), nullable=False),
        sa.Column("title", sa.String(length=255), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("instructional_plan_id", sa.Uuid(), nullable=True),
        sa.Column("planning_draft_id", sa.Uuid(), nullable=True),
        sa.Column("assignment_id", sa.Uuid(), nullable=True),
        sa.Column("export_artifact_id", sa.Uuid(), nullable=True),
        sa.Column("newsletter_id", sa.Uuid(), nullable=True),
        sa.Column("resource_links_json", sa.JSON(), nullable=True),
        sa.Column("metadata_json", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["assignment_id"], ["assignments.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["created_by_user_id"], ["users.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(
            ["export_artifact_id"], ["teacher_assist_export_artifacts.id"], ondelete="SET NULL"
        ),
        sa.ForeignKeyConstraint(
            ["instructional_plan_id"], ["weekly_plans.id"], ondelete="SET NULL"
        ),
        sa.ForeignKeyConstraint(
            ["newsletter_id"], ["teacher_assist_newsletters.id"], ondelete="SET NULL"
        ),
        sa.ForeignKeyConstraint(["pacing_guide_id"], ["pacing_guides.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(
            ["pacing_guide_period_id"], ["pacing_guide_periods.id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(
            ["planning_draft_id"], ["planning_input_drafts.id"], ondelete="SET NULL"
        ),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_teacher_assist_generated_artifacts_tenant_id",
        "teacher_assist_generated_artifacts",
        ["tenant_id"],
    )
    op.create_index(
        "ix_teacher_assist_generated_artifacts_period_id",
        "teacher_assist_generated_artifacts",
        ["pacing_guide_period_id"],
    )
    op.create_index(
        "ix_teacher_assist_generated_artifacts_artifact_type",
        "teacher_assist_generated_artifacts",
        ["artifact_type"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_teacher_assist_generated_artifacts_artifact_type",
        table_name="teacher_assist_generated_artifacts",
    )
    op.drop_index(
        "ix_teacher_assist_generated_artifacts_period_id",
        table_name="teacher_assist_generated_artifacts",
    )
    op.drop_index(
        "ix_teacher_assist_generated_artifacts_tenant_id",
        table_name="teacher_assist_generated_artifacts",
    )
    op.drop_table("teacher_assist_generated_artifacts")

    op.drop_index("ix_weekly_plans_pacing_guide_id", table_name="weekly_plans")
    op.drop_constraint("fk_weekly_plans_pacing_guide_id", "weekly_plans", type_="foreignkey")
    op.drop_column("weekly_plans", "pacing_guide_id")

    op.drop_index(
        "ix_teacher_assist_newsletters_pacing_guide_period_id",
        table_name="teacher_assist_newsletters",
    )
    op.drop_constraint(
        "fk_teacher_assist_newsletters_pacing_guide_period_id",
        "teacher_assist_newsletters",
        type_="foreignkey",
    )
    op.drop_constraint(
        "fk_teacher_assist_newsletters_pacing_guide_id",
        "teacher_assist_newsletters",
        type_="foreignkey",
    )
    op.drop_column("teacher_assist_newsletters", "pacing_guide_period_id")
    op.drop_column("teacher_assist_newsletters", "pacing_guide_id")

    op.drop_index("ix_assignments_pacing_guide_period_id", table_name="assignments")
    op.drop_constraint("fk_assignments_pacing_guide_period_id", "assignments", type_="foreignkey")
    op.drop_constraint("fk_assignments_pacing_guide_id", "assignments", type_="foreignkey")
    op.drop_column("assignments", "pacing_guide_period_id")
    op.drop_column("assignments", "pacing_guide_id")
