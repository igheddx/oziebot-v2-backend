"""add teacher assist user preferences foundation

Revision ID: 058_teacher_assist_user_preferences_foundation
Revises: 057_teacher_assist_lesson_reflection_foundation
Create Date: 2026-05-29 06:00:00.000000
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Union

from alembic import op
import sqlalchemy as sa


revision: str = "058_teacher_assist_user_preferences_foundation"
down_revision: Union[str, None] = "057_teacher_assist_lesson_reflection_foundation"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "teacher_assist_user_preferences",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("last_class_id", sa.Uuid(), nullable=True),
        sa.Column("last_grading_period_id", sa.Uuid(), nullable=True),
        sa.Column("last_subject_id", sa.Uuid(), nullable=True),
        sa.Column("preferred_landing", sa.String(length=32), nullable=False, server_default="home"),
        sa.Column("recently_viewed_json", sa.JSON(), nullable=False),
        sa.Column("onboarding_progress_json", sa.JSON(), nullable=False),
        sa.Column("onboarding_completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["last_class_id"], ["classes.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(
            ["last_grading_period_id"], ["grading_periods.id"], ondelete="SET NULL"
        ),
        sa.ForeignKeyConstraint(["last_subject_id"], ["subjects.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "tenant_id", "user_id", name="uq_teacher_assist_user_preferences_tenant_user"
        ),
    )
    for column_name in (
        "tenant_id",
        "user_id",
        "last_class_id",
        "last_grading_period_id",
        "last_subject_id",
    ):
        op.create_index(
            op.f(f"ix_teacher_assist_user_preferences_{column_name}"),
            "teacher_assist_user_preferences",
            [column_name],
            unique=False,
        )


def downgrade() -> None:
    op.drop_index(
        op.f("ix_teacher_assist_user_preferences_last_subject_id"),
        table_name="teacher_assist_user_preferences",
    )
    op.drop_index(
        op.f("ix_teacher_assist_user_preferences_last_grading_period_id"),
        table_name="teacher_assist_user_preferences",
    )
    op.drop_index(
        op.f("ix_teacher_assist_user_preferences_last_class_id"),
        table_name="teacher_assist_user_preferences",
    )
    op.drop_index(
        op.f("ix_teacher_assist_user_preferences_user_id"),
        table_name="teacher_assist_user_preferences",
    )
    op.drop_index(
        op.f("ix_teacher_assist_user_preferences_tenant_id"),
        table_name="teacher_assist_user_preferences",
    )
    op.drop_table("teacher_assist_user_preferences")
