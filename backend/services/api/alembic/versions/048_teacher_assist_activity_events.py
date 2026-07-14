"""add teacher assist activity events

Revision ID: 048_teacher_assist_activity_events
Revises: 047_teacher_assist_grading_review_foundation
Create Date: 2026-05-28 07:10:00.000000
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Union

from alembic import op
import sqlalchemy as sa


revision: str = "048_teacher_assist_activity_events"
down_revision: Union[str, None] = "047_teacher_assist_grading_review_foundation"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "teacher_assist_activity_events",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("event_type", sa.String(length=64), nullable=False),
        sa.Column("event_category", sa.String(length=32), nullable=False),
        sa.Column("entity_type", sa.String(length=64), nullable=False),
        sa.Column("entity_id", sa.Uuid(), nullable=False),
        sa.Column("event_timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("school_year_id", sa.Uuid(), nullable=True),
        sa.Column("grading_period_id", sa.Uuid(), nullable=True),
        sa.Column("class_id", sa.Uuid(), nullable=True),
        sa.Column("subject_id", sa.Uuid(), nullable=True),
        sa.Column("summary_text", sa.Text(), nullable=False),
        sa.Column("details_json", sa.JSON(), nullable=True),
        sa.Column("workflow_id", sa.Uuid(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["class_id"], ["classes.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["grading_period_id"], ["grading_periods.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["school_year_id"], ["school_years.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["subject_id"], ["subjects.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(
            ["workflow_id"], ["teacher_assist_workflows.id"], ondelete="SET NULL"
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    for column_name in (
        "tenant_id",
        "user_id",
        "event_type",
        "event_category",
        "entity_type",
        "entity_id",
        "event_timestamp",
        "school_year_id",
        "grading_period_id",
        "class_id",
        "subject_id",
        "workflow_id",
    ):
        op.create_index(
            op.f(f"ix_teacher_assist_activity_events_{column_name}"),
            "teacher_assist_activity_events",
            [column_name],
            unique=False,
        )


def downgrade() -> None:
    for column_name in (
        "workflow_id",
        "subject_id",
        "class_id",
        "grading_period_id",
        "school_year_id",
        "event_timestamp",
        "entity_id",
        "entity_type",
        "event_category",
        "event_type",
        "user_id",
        "tenant_id",
    ):
        op.drop_index(
            op.f(f"ix_teacher_assist_activity_events_{column_name}"),
            table_name="teacher_assist_activity_events",
        )
    op.drop_table("teacher_assist_activity_events")
