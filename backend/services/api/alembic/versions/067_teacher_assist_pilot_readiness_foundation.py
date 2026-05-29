"""add teacher assist pilot readiness foundation (phase 41)

Revision ID: 067_teacher_assist_pilot_readiness_foundation
Revises: 066_teacher_assist_copilot_foundation
Create Date: 2026-05-31 18:00:00.000000
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Union

from alembic import op
import sqlalchemy as sa


revision: str = "067_teacher_assist_pilot_readiness_foundation"
down_revision: Union[str, None] = "066_teacher_assist_copilot_foundation"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "teacher_assist_pilot_feedback",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("category", sa.String(length=64), nullable=False),
        sa.Column("severity", sa.String(length=32), nullable=False),
        sa.Column("feature_area", sa.String(length=128), nullable=False),
        sa.Column("description", sa.Text(), nullable=False),
        sa.Column("requested_improvement", sa.Text(), nullable=True),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        op.f("ix_teacher_assist_pilot_feedback_tenant_id"),
        "teacher_assist_pilot_feedback",
        ["tenant_id"],
    )
    op.create_index(
        op.f("ix_teacher_assist_pilot_feedback_user_id"),
        "teacher_assist_pilot_feedback",
        ["user_id"],
    )
    op.create_index(
        op.f("ix_teacher_assist_pilot_feedback_status"),
        "teacher_assist_pilot_feedback",
        ["status"],
    )

    op.create_table(
        "teacher_assist_usage_metrics",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("user_id", sa.Uuid(), nullable=True),
        sa.Column("metric_key", sa.String(length=128), nullable=False),
        sa.Column("metric_value", sa.Integer(), nullable=False),
        sa.Column("period_date", sa.Date(), nullable=False),
        sa.Column("metadata_json", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "tenant_id",
            "user_id",
            "metric_key",
            "period_date",
            name="uq_teacher_assist_usage_metrics_scope",
        ),
    )
    op.create_index(
        op.f("ix_teacher_assist_usage_metrics_tenant_id"),
        "teacher_assist_usage_metrics",
        ["tenant_id"],
    )
    op.create_index(
        op.f("ix_teacher_assist_usage_metrics_metric_key"),
        "teacher_assist_usage_metrics",
        ["metric_key"],
    )
    op.create_index(
        op.f("ix_teacher_assist_usage_metrics_period_date"),
        "teacher_assist_usage_metrics",
        ["period_date"],
    )


def downgrade() -> None:
    op.drop_table("teacher_assist_usage_metrics")
    op.drop_table("teacher_assist_pilot_feedback")
