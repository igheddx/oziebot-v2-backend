"""add teacher assist lesson reflection foundation

Revision ID: 057_teacher_assist_lesson_reflection_foundation
Revises: 056_teacher_assist_newsletter_foundation
Create Date: 2026-05-29 04:00:00.000000
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Union

from alembic import op
import sqlalchemy as sa


revision: str = "057_teacher_assist_lesson_reflection_foundation"
down_revision: Union[str, None] = "056_teacher_assist_newsletter_foundation"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "teacher_assist_lesson_reflections",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("owner_user_id", sa.Uuid(), nullable=False),
        sa.Column("school_year_id", sa.Uuid(), nullable=False),
        sa.Column("grading_period_id", sa.Uuid(), nullable=True),
        sa.Column("class_id", sa.Uuid(), nullable=False),
        sa.Column("subject_id", sa.Uuid(), nullable=False),
        sa.Column("weekly_plan_id", sa.Uuid(), nullable=True),
        sa.Column("title", sa.String(length=255), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("lesson_date", sa.Date(), nullable=True),
        sa.Column("current_version_id", sa.Uuid(), nullable=True),
        sa.Column("latest_ai_usage_event_id", sa.Uuid(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["class_id"], ["classes.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["grading_period_id"], ["grading_periods.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(
            ["latest_ai_usage_event_id"],
            ["teacher_assist_ai_usage_events.id"],
            ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(["owner_user_id"], ["users.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["school_year_id"], ["school_years.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["subject_id"], ["subjects.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["weekly_plan_id"], ["weekly_plans.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
    )
    for column_name in (
        "tenant_id",
        "owner_user_id",
        "school_year_id",
        "grading_period_id",
        "class_id",
        "subject_id",
        "weekly_plan_id",
        "status",
        "current_version_id",
    ):
        op.create_index(
            op.f(f"ix_teacher_assist_lesson_reflections_{column_name}"),
            "teacher_assist_lesson_reflections",
            [column_name],
            unique=False,
        )

    op.create_table(
        "teacher_assist_lesson_reflection_versions",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("owner_user_id", sa.Uuid(), nullable=False),
        sa.Column("lesson_reflection_id", sa.Uuid(), nullable=False),
        sa.Column("version_number", sa.Integer(), nullable=False),
        sa.Column("version_source", sa.String(length=32), nullable=False),
        sa.Column("content_json", sa.JSON(), nullable=False),
        sa.Column("prompt_context_json", sa.JSON(), nullable=True),
        sa.Column("provider_name", sa.String(length=64), nullable=True),
        sa.Column("provider_model", sa.String(length=128), nullable=True),
        sa.Column("prompt_version", sa.String(length=64), nullable=True),
        sa.Column("ai_usage_event_id", sa.Uuid(), nullable=True),
        sa.Column("created_by_user_id", sa.Uuid(), nullable=False),
        sa.Column("change_reason", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["ai_usage_event_id"], ["teacher_assist_ai_usage_events.id"], ondelete="SET NULL"
        ),
        sa.ForeignKeyConstraint(["created_by_user_id"], ["users.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(
            ["lesson_reflection_id"],
            ["teacher_assist_lesson_reflections.id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(["owner_user_id"], ["users.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "lesson_reflection_id",
            "version_number",
            name="uq_lesson_reflection_version_number",
        ),
    )
    for column_name in (
        "tenant_id",
        "owner_user_id",
        "lesson_reflection_id",
        "version_source",
        "ai_usage_event_id",
    ):
        op.create_index(
            op.f(f"ix_teacher_assist_lesson_reflection_versions_{column_name}"),
            "teacher_assist_lesson_reflection_versions",
            [column_name],
            unique=False,
        )

    op.create_foreign_key(
        "fk_teacher_assist_lesson_reflections_current_version_id",
        "teacher_assist_lesson_reflections",
        "teacher_assist_lesson_reflection_versions",
        ["current_version_id"],
        ["id"],
        ondelete="SET NULL",
    )


def downgrade() -> None:
    op.drop_constraint(
        "fk_teacher_assist_lesson_reflections_current_version_id",
        "teacher_assist_lesson_reflections",
        type_="foreignkey",
    )
    op.drop_index(
        op.f("ix_teacher_assist_lesson_reflection_versions_ai_usage_event_id"),
        table_name="teacher_assist_lesson_reflection_versions",
    )
    op.drop_index(
        op.f("ix_teacher_assist_lesson_reflection_versions_version_source"),
        table_name="teacher_assist_lesson_reflection_versions",
    )
    op.drop_index(
        op.f("ix_teacher_assist_lesson_reflection_versions_lesson_reflection_id"),
        table_name="teacher_assist_lesson_reflection_versions",
    )
    op.drop_index(
        op.f("ix_teacher_assist_lesson_reflection_versions_owner_user_id"),
        table_name="teacher_assist_lesson_reflection_versions",
    )
    op.drop_index(
        op.f("ix_teacher_assist_lesson_reflection_versions_tenant_id"),
        table_name="teacher_assist_lesson_reflection_versions",
    )
    op.drop_table("teacher_assist_lesson_reflection_versions")
    op.drop_index(
        op.f("ix_teacher_assist_lesson_reflections_current_version_id"),
        table_name="teacher_assist_lesson_reflections",
    )
    op.drop_index(
        op.f("ix_teacher_assist_lesson_reflections_status"),
        table_name="teacher_assist_lesson_reflections",
    )
    op.drop_index(
        op.f("ix_teacher_assist_lesson_reflections_weekly_plan_id"),
        table_name="teacher_assist_lesson_reflections",
    )
    op.drop_index(
        op.f("ix_teacher_assist_lesson_reflections_subject_id"),
        table_name="teacher_assist_lesson_reflections",
    )
    op.drop_index(
        op.f("ix_teacher_assist_lesson_reflections_class_id"),
        table_name="teacher_assist_lesson_reflections",
    )
    op.drop_index(
        op.f("ix_teacher_assist_lesson_reflections_grading_period_id"),
        table_name="teacher_assist_lesson_reflections",
    )
    op.drop_index(
        op.f("ix_teacher_assist_lesson_reflections_school_year_id"),
        table_name="teacher_assist_lesson_reflections",
    )
    op.drop_index(
        op.f("ix_teacher_assist_lesson_reflections_owner_user_id"),
        table_name="teacher_assist_lesson_reflections",
    )
    op.drop_index(
        op.f("ix_teacher_assist_lesson_reflections_tenant_id"),
        table_name="teacher_assist_lesson_reflections",
    )
    op.drop_table("teacher_assist_lesson_reflections")
