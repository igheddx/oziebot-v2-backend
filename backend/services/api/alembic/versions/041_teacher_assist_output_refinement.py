"""add teacher assist versions and usage events

Revision ID: 041_teacher_assist_output_refinement
Revises: 040_teacher_assist_workflows
Create Date: 2026-05-27 08:00:00.000000
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Union

from alembic import op
import sqlalchemy as sa


revision: str = "041_teacher_assist_output_refinement"
down_revision: Union[str, None] = "040_teacher_assist_workflows"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "weekly_plan_versions",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("weekly_plan_id", sa.Uuid(), nullable=False),
        sa.Column("version_number", sa.Integer(), nullable=False),
        sa.Column("content_json", sa.JSON(), nullable=False),
        sa.Column("source_context_json", sa.JSON(), nullable=False),
        sa.Column("created_by_user_id", sa.Uuid(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("change_reason", sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(["weekly_plan_id"], ["weekly_plans.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["created_by_user_id"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("weekly_plan_id", "version_number", name="uq_weekly_plan_version_number"),
    )
    op.create_index("ix_weekly_plan_versions_weekly_plan_id", "weekly_plan_versions", ["weekly_plan_id"])
    op.create_index(
        "ix_weekly_plan_versions_created_by_user_id",
        "weekly_plan_versions",
        ["created_by_user_id"],
    )

    op.create_table(
        "teacher_assist_ai_usage_events",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("workflow_id", sa.Uuid(), nullable=True),
        sa.Column("provider", sa.String(length=64), nullable=False),
        sa.Column("model", sa.String(length=128), nullable=True),
        sa.Column("feature", sa.String(length=64), nullable=False),
        sa.Column("input_tokens", sa.Integer(), nullable=True),
        sa.Column("output_tokens", sa.Integer(), nullable=True),
        sa.Column("estimated_cost_cents", sa.Integer(), nullable=True),
        sa.Column("metadata_json", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["workflow_id"], ["teacher_assist_workflows.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_teacher_assist_ai_usage_events_tenant_id",
        "teacher_assist_ai_usage_events",
        ["tenant_id"],
    )
    op.create_index(
        "ix_teacher_assist_ai_usage_events_user_id",
        "teacher_assist_ai_usage_events",
        ["user_id"],
    )
    op.create_index(
        "ix_teacher_assist_ai_usage_events_workflow_id",
        "teacher_assist_ai_usage_events",
        ["workflow_id"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_teacher_assist_ai_usage_events_workflow_id",
        table_name="teacher_assist_ai_usage_events",
    )
    op.drop_index(
        "ix_teacher_assist_ai_usage_events_user_id",
        table_name="teacher_assist_ai_usage_events",
    )
    op.drop_index(
        "ix_teacher_assist_ai_usage_events_tenant_id",
        table_name="teacher_assist_ai_usage_events",
    )
    op.drop_table("teacher_assist_ai_usage_events")
    op.drop_index(
        "ix_weekly_plan_versions_created_by_user_id",
        table_name="weekly_plan_versions",
    )
    op.drop_index(
        "ix_weekly_plan_versions_weekly_plan_id",
        table_name="weekly_plan_versions",
    )
    op.drop_table("weekly_plan_versions")
