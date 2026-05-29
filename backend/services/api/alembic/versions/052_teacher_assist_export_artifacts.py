"""add teacher assist export artifacts foundation

Revision ID: 052_teacher_assist_export_artifacts
Revises: 051_teacher_assist_ocr_provider_metadata
Create Date: 2026-05-28 18:00:00.000000
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Union

from alembic import op
import sqlalchemy as sa


revision: str = "052_teacher_assist_export_artifacts"
down_revision: Union[str, None] = "051_teacher_assist_ocr_provider_metadata"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "teacher_assist_export_artifacts",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("source_plan_id", sa.Uuid(), nullable=False),
        sa.Column("source_assignment_id", sa.Uuid(), nullable=True),
        sa.Column("workflow_id", sa.Uuid(), nullable=True),
        sa.Column("artifact_type", sa.String(length=64), nullable=False),
        sa.Column("artifact_status", sa.String(length=32), nullable=False, server_default="queued"),
        sa.Column("title", sa.String(length=160), nullable=False),
        sa.Column("export_format", sa.String(length=32), nullable=False),
        sa.Column("storage_key", sa.Text(), nullable=True),
        sa.Column("preview_json", sa.JSON(), nullable=False),
        sa.Column("metadata_json", sa.JSON(), nullable=True),
        sa.Column("provider_name", sa.String(length=64), nullable=True),
        sa.Column("provider_model", sa.String(length=128), nullable=True),
        sa.Column("prompt_version", sa.String(length=64), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["source_assignment_id"], ["assignments.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["source_plan_id"], ["weekly_plans.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["workflow_id"], ["teacher_assist_workflows.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_ta_export_artifacts_tenant_id",
        "teacher_assist_export_artifacts",
        ["tenant_id"],
    )
    op.create_index(
        "ix_ta_export_artifacts_user_id",
        "teacher_assist_export_artifacts",
        ["user_id"],
    )
    op.create_index(
        "ix_ta_export_artifacts_source_plan_id",
        "teacher_assist_export_artifacts",
        ["source_plan_id"],
    )
    op.create_index(
        "ix_ta_export_artifacts_workflow_id",
        "teacher_assist_export_artifacts",
        ["workflow_id"],
    )
    op.create_index(
        "ix_ta_export_artifacts_artifact_type",
        "teacher_assist_export_artifacts",
        ["artifact_type"],
    )
    op.create_index(
        "ix_ta_export_artifacts_artifact_status",
        "teacher_assist_export_artifacts",
        ["artifact_status"],
    )


def downgrade() -> None:
    op.drop_index("ix_ta_export_artifacts_artifact_status", table_name="teacher_assist_export_artifacts")
    op.drop_index("ix_ta_export_artifacts_artifact_type", table_name="teacher_assist_export_artifacts")
    op.drop_index("ix_ta_export_artifacts_workflow_id", table_name="teacher_assist_export_artifacts")
    op.drop_index("ix_ta_export_artifacts_source_plan_id", table_name="teacher_assist_export_artifacts")
    op.drop_index("ix_ta_export_artifacts_user_id", table_name="teacher_assist_export_artifacts")
    op.drop_index("ix_ta_export_artifacts_tenant_id", table_name="teacher_assist_export_artifacts")
    op.drop_table("teacher_assist_export_artifacts")
