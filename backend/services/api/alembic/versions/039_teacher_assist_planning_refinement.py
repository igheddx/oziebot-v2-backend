"""refine teacher assist planning drafts

Revision ID: 039_teacher_assist_planning_refinement
Revises: 038_teacher_assist_planning_context
Create Date: 2026-05-27 06:40:00.000000
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Union

from alembic import op
import sqlalchemy as sa


revision: str = "039_teacher_assist_planning_refinement"
down_revision: Union[str, None] = "038_teacher_assist_planning_context"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "planning_input_draft_subjects",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("planning_input_draft_id", sa.Uuid(), nullable=False),
        sa.Column("subject_id", sa.Uuid(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["planning_input_draft_id"],
            ["planning_input_drafts.id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(["subject_id"], ["subjects.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "planning_input_draft_id",
            "subject_id",
            name="uq_planning_draft_subject",
        ),
    )
    op.create_index(
        "ix_planning_input_draft_subjects_planning_input_draft_id",
        "planning_input_draft_subjects",
        ["planning_input_draft_id"],
    )
    op.create_index(
        "ix_planning_input_draft_subjects_subject_id",
        "planning_input_draft_subjects",
        ["subject_id"],
    )

    op.create_table(
        "planning_input_draft_pacing_items",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("planning_input_draft_id", sa.Uuid(), nullable=False),
        sa.Column("pacing_item_id", sa.Uuid(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["planning_input_draft_id"],
            ["planning_input_drafts.id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(["pacing_item_id"], ["pacing_items.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "planning_input_draft_id",
            "pacing_item_id",
            name="uq_planning_draft_pacing_item",
        ),
    )
    op.create_index(
        "ix_planning_input_draft_pacing_items_planning_input_draft_id",
        "planning_input_draft_pacing_items",
        ["planning_input_draft_id"],
    )
    op.create_index(
        "ix_planning_input_draft_pacing_items_pacing_item_id",
        "planning_input_draft_pacing_items",
        ["pacing_item_id"],
    )

    op.create_table(
        "planning_input_draft_standards",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("planning_input_draft_id", sa.Uuid(), nullable=False),
        sa.Column("standard_id", sa.Uuid(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["planning_input_draft_id"],
            ["planning_input_drafts.id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(["standard_id"], ["standards.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "planning_input_draft_id",
            "standard_id",
            name="uq_planning_draft_standard",
        ),
    )
    op.create_index(
        "ix_planning_input_draft_standards_planning_input_draft_id",
        "planning_input_draft_standards",
        ["planning_input_draft_id"],
    )
    op.create_index(
        "ix_planning_input_draft_standards_standard_id",
        "planning_input_draft_standards",
        ["standard_id"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_planning_input_draft_standards_standard_id",
        table_name="planning_input_draft_standards",
    )
    op.drop_index(
        "ix_planning_input_draft_standards_planning_input_draft_id",
        table_name="planning_input_draft_standards",
    )
    op.drop_table("planning_input_draft_standards")
    op.drop_index(
        "ix_planning_input_draft_pacing_items_pacing_item_id",
        table_name="planning_input_draft_pacing_items",
    )
    op.drop_index(
        "ix_planning_input_draft_pacing_items_planning_input_draft_id",
        table_name="planning_input_draft_pacing_items",
    )
    op.drop_table("planning_input_draft_pacing_items")
    op.drop_index(
        "ix_planning_input_draft_subjects_subject_id",
        table_name="planning_input_draft_subjects",
    )
    op.drop_index(
        "ix_planning_input_draft_subjects_planning_input_draft_id",
        table_name="planning_input_draft_subjects",
    )
    op.drop_table("planning_input_draft_subjects")
