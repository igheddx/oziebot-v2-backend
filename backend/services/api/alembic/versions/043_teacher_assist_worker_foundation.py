"""add teacher assist worker foundation fields

Revision ID: 043_teacher_assist_worker_foundation
Revises: 042_teacher_assist_instructional_planning
Create Date: 2026-05-27 21:30:00.000000
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Union

from alembic import op
import sqlalchemy as sa


revision: str = "043_teacher_assist_worker_foundation"
down_revision: Union[str, None] = "042_teacher_assist_instructional_planning"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "teacher_assist_workflows",
        sa.Column("leased_by_worker", sa.String(length=128), nullable=True),
    )
    op.add_column(
        "teacher_assist_workflows",
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "teacher_assist_workflows",
        sa.Column("heartbeat_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "teacher_assist_workflows",
        sa.Column("retry_count", sa.Integer(), nullable=False, server_default="0"),
    )
    op.add_column(
        "teacher_assist_workflows",
        sa.Column("max_retries", sa.Integer(), nullable=False, server_default="3"),
    )
    op.add_column(
        "teacher_assist_workflows",
        sa.Column("timeout_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "teacher_assist_workflows",
        sa.Column("provider_name", sa.String(length=64), nullable=True),
    )
    op.add_column(
        "teacher_assist_workflows",
        sa.Column("provider_model", sa.String(length=128), nullable=True),
    )
    op.add_column(
        "teacher_assist_workflows",
        sa.Column("prompt_version", sa.String(length=64), nullable=True),
    )
    op.add_column(
        "teacher_assist_workflows",
        sa.Column("input_tokens_total", sa.Integer(), nullable=False, server_default="0"),
    )
    op.add_column(
        "teacher_assist_workflows",
        sa.Column("output_tokens_total", sa.Integer(), nullable=False, server_default="0"),
    )
    op.add_column(
        "teacher_assist_workflows",
        sa.Column("estimated_cost_cents_total", sa.Integer(), nullable=False, server_default="0"),
    )
    op.add_column(
        "teacher_assist_workflows",
        sa.Column("last_error_code", sa.String(length=64), nullable=True),
    )
    op.add_column(
        "teacher_assist_workflows",
        sa.Column("execution_log_json", sa.JSON(), nullable=True),
    )
    op.create_index(
        "ix_teacher_assist_workflows_status_workflow_type",
        "teacher_assist_workflows",
        ["status", "workflow_type"],
    )
    op.create_index(
        "ix_teacher_assist_workflows_lease_expires_at",
        "teacher_assist_workflows",
        ["lease_expires_at"],
    )


def downgrade() -> None:
    op.drop_index("ix_teacher_assist_workflows_lease_expires_at", table_name="teacher_assist_workflows")
    op.drop_index(
        "ix_teacher_assist_workflows_status_workflow_type",
        table_name="teacher_assist_workflows",
    )
    op.drop_column("teacher_assist_workflows", "execution_log_json")
    op.drop_column("teacher_assist_workflows", "last_error_code")
    op.drop_column("teacher_assist_workflows", "estimated_cost_cents_total")
    op.drop_column("teacher_assist_workflows", "output_tokens_total")
    op.drop_column("teacher_assist_workflows", "input_tokens_total")
    op.drop_column("teacher_assist_workflows", "prompt_version")
    op.drop_column("teacher_assist_workflows", "provider_model")
    op.drop_column("teacher_assist_workflows", "provider_name")
    op.drop_column("teacher_assist_workflows", "timeout_at")
    op.drop_column("teacher_assist_workflows", "max_retries")
    op.drop_column("teacher_assist_workflows", "retry_count")
    op.drop_column("teacher_assist_workflows", "heartbeat_at")
    op.drop_column("teacher_assist_workflows", "lease_expires_at")
    op.drop_column("teacher_assist_workflows", "leased_by_worker")
