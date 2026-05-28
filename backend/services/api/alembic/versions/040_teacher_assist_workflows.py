"""add teacher assist workflows

Revision ID: 040_teacher_assist_workflows
Revises: 039_teacher_assist_planning_refinement
Create Date: 2026-05-27 07:45:00.000000
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Union

from alembic import op
import sqlalchemy as sa


revision: str = "040_teacher_assist_workflows"
down_revision: Union[str, None] = "039_teacher_assist_planning_refinement"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "teacher_assist_workflows",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("planning_input_draft_id", sa.Uuid(), nullable=True),
        sa.Column("workflow_type", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("input_snapshot_json", sa.JSON(), nullable=False),
        sa.Column("output_ref_type", sa.String(length=64), nullable=True),
        sa.Column("output_ref_id", sa.Uuid(), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("progress_percent", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["planning_input_draft_id"], ["planning_input_drafts.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_teacher_assist_workflows_tenant_id",
        "teacher_assist_workflows",
        ["tenant_id"],
    )
    op.create_index(
        "ix_teacher_assist_workflows_user_id",
        "teacher_assist_workflows",
        ["user_id"],
    )
    op.create_index(
        "ix_teacher_assist_workflows_planning_input_draft_id",
        "teacher_assist_workflows",
        ["planning_input_draft_id"],
    )
    op.create_index(
        "ix_teacher_assist_workflows_output_ref_id",
        "teacher_assist_workflows",
        ["output_ref_id"],
    )

    op.create_table(
        "teacher_assist_workflow_steps",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("workflow_id", sa.Uuid(), nullable=False),
        sa.Column("step_name", sa.String(length=128), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("metadata_json", sa.JSON(), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["workflow_id"], ["teacher_assist_workflows.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_teacher_assist_workflow_steps_workflow_id",
        "teacher_assist_workflow_steps",
        ["workflow_id"],
    )

    op.create_table(
        "weekly_plans",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("planning_input_draft_id", sa.Uuid(), nullable=False),
        sa.Column("workflow_id", sa.Uuid(), nullable=True),
        sa.Column("title", sa.String(length=160), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("content_json", sa.JSON(), nullable=False),
        sa.Column("source_context_json", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["planning_input_draft_id"], ["planning_input_drafts.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["workflow_id"], ["teacher_assist_workflows.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_weekly_plans_tenant_id", "weekly_plans", ["tenant_id"])
    op.create_index("ix_weekly_plans_user_id", "weekly_plans", ["user_id"])
    op.create_index(
        "ix_weekly_plans_planning_input_draft_id",
        "weekly_plans",
        ["planning_input_draft_id"],
    )
    op.create_index("ix_weekly_plans_workflow_id", "weekly_plans", ["workflow_id"])


def downgrade() -> None:
    op.drop_index("ix_weekly_plans_workflow_id", table_name="weekly_plans")
    op.drop_index("ix_weekly_plans_planning_input_draft_id", table_name="weekly_plans")
    op.drop_index("ix_weekly_plans_user_id", table_name="weekly_plans")
    op.drop_index("ix_weekly_plans_tenant_id", table_name="weekly_plans")
    op.drop_table("weekly_plans")
    op.drop_index(
        "ix_teacher_assist_workflow_steps_workflow_id",
        table_name="teacher_assist_workflow_steps",
    )
    op.drop_table("teacher_assist_workflow_steps")
    op.drop_index(
        "ix_teacher_assist_workflows_output_ref_id",
        table_name="teacher_assist_workflows",
    )
    op.drop_index(
        "ix_teacher_assist_workflows_planning_input_draft_id",
        table_name="teacher_assist_workflows",
    )
    op.drop_index(
        "ix_teacher_assist_workflows_user_id",
        table_name="teacher_assist_workflows",
    )
    op.drop_index(
        "ix_teacher_assist_workflows_tenant_id",
        table_name="teacher_assist_workflows",
    )
    op.drop_table("teacher_assist_workflows")
