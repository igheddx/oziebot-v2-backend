"""add ai diagnostic review tables

Revision ID: 033_ai_diagnostic_reviews
Revises: 032_strategy_lifecycle_trace
Create Date: 2026-05-09 12:30:00.000000
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Union

from alembic import op
import sqlalchemy as sa


revision: str = "033_ai_diagnostic_reviews"
down_revision: Union[str, None] = "032_strategy_lifecycle_trace"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "diagnostic_snapshots",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=True),
        sa.Column("generated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("trading_mode", sa.String(length=16), nullable=True),
        sa.Column("strategy_filter", sa.String(length=128), nullable=True),
        sa.Column("token_filter", sa.String(length=32), nullable=True),
        sa.Column("days_filter", sa.Integer(), nullable=False, server_default="7"),
        sa.Column("raw_json", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_diagnostic_snapshots_tenant_id", "diagnostic_snapshots", ["tenant_id"])
    op.create_index(
        "ix_diagnostic_snapshots_generated_at", "diagnostic_snapshots", ["generated_at"]
    )
    op.create_index(
        "ix_diagnostic_snapshots_trading_mode", "diagnostic_snapshots", ["trading_mode"]
    )
    op.create_index(
        "ix_diagnostic_snapshots_strategy_filter", "diagnostic_snapshots", ["strategy_filter"]
    )
    op.create_index(
        "ix_diagnostic_snapshots_token_filter", "diagnostic_snapshots", ["token_filter"]
    )
    op.create_index("ix_diagnostic_snapshots_created_at", "diagnostic_snapshots", ["created_at"])

    op.create_table(
        "ai_diagnostic_reviews",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=True),
        sa.Column("snapshot_id", sa.Uuid(), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("overall_health", sa.String(length=32), nullable=True),
        sa.Column("confidence_score", sa.Float(), nullable=True),
        sa.Column("summary", sa.Text(), nullable=True),
        sa.Column("model_name", sa.String(length=128), nullable=False),
        sa.Column("prompt_version", sa.String(length=64), nullable=False),
        sa.Column("created_by_admin_id", sa.Uuid(), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["created_by_admin_id"], ["users.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["snapshot_id"], ["diagnostic_snapshots.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_ai_diagnostic_reviews_tenant_id", "ai_diagnostic_reviews", ["tenant_id"])
    op.create_index(
        "ix_ai_diagnostic_reviews_snapshot_id", "ai_diagnostic_reviews", ["snapshot_id"]
    )
    op.create_index("ix_ai_diagnostic_reviews_status", "ai_diagnostic_reviews", ["status"])
    op.create_index(
        "ix_ai_diagnostic_reviews_overall_health", "ai_diagnostic_reviews", ["overall_health"]
    )
    op.create_index(
        "ix_ai_diagnostic_reviews_created_by_admin_id",
        "ai_diagnostic_reviews",
        ["created_by_admin_id"],
    )
    op.create_index("ix_ai_diagnostic_reviews_created_at", "ai_diagnostic_reviews", ["created_at"])

    op.create_table(
        "ai_diagnostic_findings",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("review_id", sa.Uuid(), nullable=False),
        sa.Column("severity", sa.String(length=32), nullable=False),
        sa.Column("category", sa.String(length=64), nullable=False),
        sa.Column("strategy", sa.String(length=128), nullable=True),
        sa.Column("token", sa.String(length=32), nullable=True),
        sa.Column("finding_title", sa.String(length=255), nullable=False),
        sa.Column("finding_detail", sa.Text(), nullable=False),
        sa.Column("evidence_json", sa.JSON(), nullable=False),
        sa.Column("recommendation", sa.Text(), nullable=False),
        sa.Column("risk_if_ignored", sa.Text(), nullable=True),
        sa.Column("confidence_score", sa.Float(), nullable=True),
        sa.Column("automation_eligibility", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column(
            "future_config_change_candidate", sa.Boolean(), nullable=False, server_default="false"
        ),
        sa.Column("proposed_config_change_json", sa.JSON(), nullable=True),
        sa.Column("approval_required", sa.Boolean(), nullable=False, server_default="false"),
        sa.Column("eligible_for_auto_tune", sa.Boolean(), nullable=False, server_default="false"),
        sa.Column("rollback_plan", sa.Text(), nullable=True),
        sa.Column("expected_impact", sa.Text(), nullable=True),
        sa.Column("risk_level", sa.String(length=32), nullable=True),
        sa.Column("affected_strategy", sa.String(length=128), nullable=True),
        sa.Column("affected_token", sa.String(length=32), nullable=True),
        sa.Column("parameter_name", sa.String(length=128), nullable=True),
        sa.Column("current_value_json", sa.JSON(), nullable=True),
        sa.Column("proposed_value_json", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["review_id"], ["ai_diagnostic_reviews.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_ai_diagnostic_findings_review_id", "ai_diagnostic_findings", ["review_id"])
    op.create_index("ix_ai_diagnostic_findings_severity", "ai_diagnostic_findings", ["severity"])
    op.create_index("ix_ai_diagnostic_findings_category", "ai_diagnostic_findings", ["category"])
    op.create_index("ix_ai_diagnostic_findings_strategy", "ai_diagnostic_findings", ["strategy"])
    op.create_index("ix_ai_diagnostic_findings_token", "ai_diagnostic_findings", ["token"])
    op.create_index("ix_ai_diagnostic_findings_status", "ai_diagnostic_findings", ["status"])
    op.create_index(
        "ix_ai_diagnostic_findings_created_at", "ai_diagnostic_findings", ["created_at"]
    )

    op.create_table(
        "ai_diagnostic_recommendation_audit",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("finding_id", sa.Uuid(), nullable=False),
        sa.Column("action", sa.String(length=64), nullable=False),
        sa.Column("previous_status", sa.String(length=32), nullable=True),
        sa.Column("new_status", sa.String(length=32), nullable=False),
        sa.Column("admin_id", sa.Uuid(), nullable=False),
        sa.Column("note", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["admin_id"], ["users.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["finding_id"], ["ai_diagnostic_findings.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_ai_diagnostic_recommendation_audit_finding_id",
        "ai_diagnostic_recommendation_audit",
        ["finding_id"],
    )
    op.create_index(
        "ix_ai_diagnostic_recommendation_audit_admin_id",
        "ai_diagnostic_recommendation_audit",
        ["admin_id"],
    )
    op.create_index(
        "ix_ai_diagnostic_recommendation_audit_created_at",
        "ai_diagnostic_recommendation_audit",
        ["created_at"],
    )


def downgrade() -> None:
    op.drop_table("ai_diagnostic_recommendation_audit")
    op.drop_table("ai_diagnostic_findings")
    op.drop_table("ai_diagnostic_reviews")
    op.drop_table("diagnostic_snapshots")
