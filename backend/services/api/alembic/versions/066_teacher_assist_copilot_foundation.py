"""add teacher copilot foundation (phase 39)

Revision ID: 066_teacher_assist_copilot_foundation
Revises: 065_teacher_assist_instructional_loop_foundation
Create Date: 2026-05-31 12:00:00.000000
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Union

from alembic import op
import sqlalchemy as sa


revision: str = "066_teacher_assist_copilot_foundation"
down_revision: Union[str, None] = "065_teacher_assist_instructional_loop_foundation"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "teacher_copilot_sessions",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("teacher_id", sa.Uuid(), nullable=False),
        sa.Column("title", sa.String(length=255), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["teacher_id"], ["users.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_teacher_copilot_sessions_tenant_id"), "teacher_copilot_sessions", ["tenant_id"])
    op.create_index(op.f("ix_teacher_copilot_sessions_teacher_id"), "teacher_copilot_sessions", ["teacher_id"])

    op.create_table(
        "teacher_copilot_messages",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("session_id", sa.Uuid(), nullable=False),
        sa.Column("role", sa.String(length=32), nullable=False),
        sa.Column("content", sa.Text(), nullable=False),
        sa.Column("context_snapshot", sa.JSON(), nullable=True),
        sa.Column("ai_usage_event_id", sa.Uuid(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["ai_usage_event_id"], ["teacher_assist_ai_usage_events.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["session_id"], ["teacher_copilot_sessions.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_teacher_copilot_messages_session_id"), "teacher_copilot_messages", ["session_id"])
    op.create_index(op.f("ix_teacher_copilot_messages_tenant_id"), "teacher_copilot_messages", ["tenant_id"])


def downgrade() -> None:
    op.drop_table("teacher_copilot_messages")
    op.drop_table("teacher_copilot_sessions")
