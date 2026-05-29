"""add current week pacing workspace foundation

Revision ID: 061_teacher_assist_current_week_foundation
Revises: 060_teacher_assist_pacing_guide_foundation
Create Date: 2026-05-29 22:00:00.000000
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Union

from alembic import op
import sqlalchemy as sa


revision: str = "061_teacher_assist_current_week_foundation"
down_revision: Union[str, None] = "060_teacher_assist_pacing_guide_foundation"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "teacher_assist_user_preferences",
        sa.Column("active_pacing_guide_id", sa.Uuid(), nullable=True),
    )
    op.add_column(
        "teacher_assist_user_preferences",
        sa.Column("manual_pacing_period_id", sa.Uuid(), nullable=True),
    )
    op.create_foreign_key(
        "fk_teacher_assist_user_preferences_active_pacing_guide_id",
        "teacher_assist_user_preferences",
        "pacing_guides",
        ["active_pacing_guide_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_foreign_key(
        "fk_teacher_assist_user_preferences_manual_pacing_period_id",
        "teacher_assist_user_preferences",
        "pacing_guide_periods",
        ["manual_pacing_period_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_index(
        "ix_teacher_assist_user_preferences_active_pacing_guide_id",
        "teacher_assist_user_preferences",
        ["active_pacing_guide_id"],
    )

    op.add_column(
        "planning_input_drafts",
        sa.Column("pacing_guide_period_id", sa.Uuid(), nullable=True),
    )
    op.create_foreign_key(
        "fk_planning_input_drafts_pacing_guide_period_id",
        "planning_input_drafts",
        "pacing_guide_periods",
        ["pacing_guide_period_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_index(
        "ix_planning_input_drafts_pacing_guide_period_id",
        "planning_input_drafts",
        ["pacing_guide_period_id"],
    )

    op.create_table(
        "pacing_guide_period_notes",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("period_id", sa.Uuid(), nullable=False),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["period_id"], ["pacing_guide_periods.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("tenant_id", "user_id", "period_id", name="uq_pacing_guide_period_notes_user_period"),
    )
    op.create_index("ix_pacing_guide_period_notes_period_id", "pacing_guide_period_notes", ["period_id"])


def downgrade() -> None:
    op.drop_index("ix_pacing_guide_period_notes_period_id", table_name="pacing_guide_period_notes")
    op.drop_table("pacing_guide_period_notes")

    op.drop_index("ix_planning_input_drafts_pacing_guide_period_id", table_name="planning_input_drafts")
    op.drop_constraint("fk_planning_input_drafts_pacing_guide_period_id", "planning_input_drafts", type_="foreignkey")
    op.drop_column("planning_input_drafts", "pacing_guide_period_id")

    op.drop_index(
        "ix_teacher_assist_user_preferences_active_pacing_guide_id",
        table_name="teacher_assist_user_preferences",
    )
    op.drop_constraint(
        "fk_teacher_assist_user_preferences_manual_pacing_period_id",
        "teacher_assist_user_preferences",
        type_="foreignkey",
    )
    op.drop_constraint(
        "fk_teacher_assist_user_preferences_active_pacing_guide_id",
        "teacher_assist_user_preferences",
        type_="foreignkey",
    )
    op.drop_column("teacher_assist_user_preferences", "manual_pacing_period_id")
    op.drop_column("teacher_assist_user_preferences", "active_pacing_guide_id")
