"""add teacher time savings foundation

Revision ID: 063_teacher_assist_time_savings_foundation
Revises: 062_teacher_assist_week_artifact_foundation
Create Date: 2026-05-30 00:00:00.000000
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Union

from alembic import op
import sqlalchemy as sa


revision: str = "063_teacher_assist_time_savings_foundation"
down_revision: Union[str, None] = "062_teacher_assist_week_artifact_foundation"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("pacing_guides", sa.Column("ownership_type", sa.String(length=32), nullable=False, server_default="TEACHER"))
    op.add_column("pacing_guides", sa.Column("visibility_scope", sa.String(length=32), nullable=False, server_default="PRIVATE"))
    op.add_column("pacing_guides", sa.Column("planning_group_id", sa.Uuid(), nullable=True))

    op.create_table(
        "teacher_assist_planning_groups",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("name", sa.String(length=160), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("subject", sa.String(length=64), nullable=True),
        sa.Column("grade_level", sa.String(length=32), nullable=True),
        sa.Column("visibility", sa.String(length=32), nullable=False),
        sa.Column("created_by_user_id", sa.Uuid(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["created_by_user_id"], ["users.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_teacher_assist_planning_groups_tenant_id", "teacher_assist_planning_groups", ["tenant_id"])

    op.create_table(
        "teacher_assist_planning_group_members",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("group_id", sa.Uuid(), nullable=False),
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("role", sa.String(length=32), nullable=False),
        sa.Column("joined_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["group_id"], ["teacher_assist_planning_groups.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_teacher_assist_planning_group_members_group_id", "teacher_assist_planning_group_members", ["group_id"])
    op.create_index("ix_teacher_assist_planning_group_members_user_id", "teacher_assist_planning_group_members", ["user_id"])

    op.create_foreign_key(
        "fk_pacing_guides_planning_group_id",
        "pacing_guides",
        "teacher_assist_planning_groups",
        ["planning_group_id"],
        ["id"],
        ondelete="SET NULL",
    )

    op.create_table(
        "teacher_assist_week_templates",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("created_by_user_id", sa.Uuid(), nullable=False),
        sa.Column("name", sa.String(length=160), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("subject", sa.String(length=64), nullable=True),
        sa.Column("grade_level", sa.String(length=32), nullable=True),
        sa.Column("artifact_type", sa.String(length=64), nullable=False),
        sa.Column("template_type", sa.String(length=32), nullable=False),
        sa.Column("visibility", sa.String(length=32), nullable=False),
        sa.Column("school_year_id", sa.Uuid(), nullable=True),
        sa.Column("source_period_id", sa.Uuid(), nullable=True),
        sa.Column("template_data", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["created_by_user_id"], ["users.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["school_year_id"], ["school_years.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["source_period_id"], ["pacing_guide_periods.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_teacher_assist_week_templates_tenant_id", "teacher_assist_week_templates", ["tenant_id"])
    op.create_index("ix_teacher_assist_week_templates_artifact_type", "teacher_assist_week_templates", ["artifact_type"])

    op.create_table(
        "teacher_assist_reuse_events",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("event_type", sa.String(length=64), nullable=False),
        sa.Column("artifact_type", sa.String(length=64), nullable=True),
        sa.Column("source_entity_type", sa.String(length=64), nullable=True),
        sa.Column("source_entity_id", sa.Uuid(), nullable=True),
        sa.Column("target_entity_id", sa.Uuid(), nullable=True),
        sa.Column("estimated_minutes_saved", sa.Integer(), nullable=False),
        sa.Column("metadata_json", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_teacher_assist_reuse_events_tenant_id", "teacher_assist_reuse_events", ["tenant_id"])
    op.create_index("ix_teacher_assist_reuse_events_user_id", "teacher_assist_reuse_events", ["user_id"])
    op.create_index("ix_teacher_assist_reuse_events_event_type", "teacher_assist_reuse_events", ["event_type"])


def downgrade() -> None:
    op.drop_index("ix_teacher_assist_reuse_events_event_type", table_name="teacher_assist_reuse_events")
    op.drop_index("ix_teacher_assist_reuse_events_user_id", table_name="teacher_assist_reuse_events")
    op.drop_index("ix_teacher_assist_reuse_events_tenant_id", table_name="teacher_assist_reuse_events")
    op.drop_table("teacher_assist_reuse_events")

    op.drop_index("ix_teacher_assist_week_templates_artifact_type", table_name="teacher_assist_week_templates")
    op.drop_index("ix_teacher_assist_week_templates_tenant_id", table_name="teacher_assist_week_templates")
    op.drop_table("teacher_assist_week_templates")

    op.drop_constraint("fk_pacing_guides_planning_group_id", "pacing_guides", type_="foreignkey")
    op.drop_index("ix_teacher_assist_planning_group_members_user_id", table_name="teacher_assist_planning_group_members")
    op.drop_index("ix_teacher_assist_planning_group_members_group_id", table_name="teacher_assist_planning_group_members")
    op.drop_table("teacher_assist_planning_group_members")
    op.drop_index("ix_teacher_assist_planning_groups_tenant_id", table_name="teacher_assist_planning_groups")
    op.drop_table("teacher_assist_planning_groups")

    op.drop_column("pacing_guides", "planning_group_id")
    op.drop_column("pacing_guides", "visibility_scope")
    op.drop_column("pacing_guides", "ownership_type")
