"""add teacher assist newsletter foundation

Revision ID: 056_teacher_assist_newsletter_foundation
Revises: 055_teacher_assist_reteach_plan_foundation
Create Date: 2026-05-29 02:30:00.000000
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Union

from alembic import op
import sqlalchemy as sa


revision: str = "056_teacher_assist_newsletter_foundation"
down_revision: Union[str, None] = "055_teacher_assist_reteach_plan_foundation"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "teacher_assist_newsletters",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("owner_user_id", sa.Uuid(), nullable=False),
        sa.Column("school_year_id", sa.Uuid(), nullable=False),
        sa.Column("grading_period_id", sa.Uuid(), nullable=True),
        sa.Column("class_id", sa.Uuid(), nullable=False),
        sa.Column("subject_id", sa.Uuid(), nullable=False),
        sa.Column("title", sa.String(length=255), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("week_start_date", sa.Date(), nullable=True),
        sa.Column("week_end_date", sa.Date(), nullable=True),
        sa.Column("teacher_notes", sa.Text(), nullable=True),
        sa.Column("current_version_id", sa.Uuid(), nullable=True),
        sa.Column("latest_ai_usage_event_id", sa.Uuid(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["class_id"], ["classes.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["grading_period_id"], ["grading_periods.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["latest_ai_usage_event_id"], ["teacher_assist_ai_usage_events.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["owner_user_id"], ["users.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["school_year_id"], ["school_years.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["subject_id"], ["subjects.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    for column_name in (
        "tenant_id",
        "owner_user_id",
        "school_year_id",
        "grading_period_id",
        "class_id",
        "subject_id",
        "status",
        "current_version_id",
    ):
        op.create_index(
            op.f(f"ix_teacher_assist_newsletters_{column_name}"),
            "teacher_assist_newsletters",
            [column_name],
            unique=False,
        )

    op.create_table(
        "teacher_assist_newsletter_versions",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("owner_user_id", sa.Uuid(), nullable=False),
        sa.Column("newsletter_id", sa.Uuid(), nullable=False),
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
        sa.ForeignKeyConstraint(["ai_usage_event_id"], ["teacher_assist_ai_usage_events.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["created_by_user_id"], ["users.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(
            ["newsletter_id"],
            ["teacher_assist_newsletters.id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(["owner_user_id"], ["users.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("newsletter_id", "version_number", name="uq_newsletter_version_number"),
    )
    for column_name in ("tenant_id", "owner_user_id", "newsletter_id", "version_source", "ai_usage_event_id"):
        op.create_index(
            op.f(f"ix_teacher_assist_newsletter_versions_{column_name}"),
            "teacher_assist_newsletter_versions",
            [column_name],
            unique=False,
        )

    op.create_table(
        "teacher_assist_newsletter_exports",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("owner_user_id", sa.Uuid(), nullable=False),
        sa.Column("newsletter_id", sa.Uuid(), nullable=False),
        sa.Column("newsletter_version_id", sa.Uuid(), nullable=True),
        sa.Column("export_format", sa.String(length=16), nullable=False),
        sa.Column("storage_key", sa.Text(), nullable=False),
        sa.Column("file_size_bytes", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["newsletter_id"], ["teacher_assist_newsletters.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(
            ["newsletter_version_id"],
            ["teacher_assist_newsletter_versions.id"],
            ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(["owner_user_id"], ["users.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    for column_name in ("tenant_id", "owner_user_id", "newsletter_id", "export_format"):
        op.create_index(
            op.f(f"ix_teacher_assist_newsletter_exports_{column_name}"),
            "teacher_assist_newsletter_exports",
            [column_name],
            unique=False,
        )

    op.create_foreign_key(
        "fk_teacher_assist_newsletters_current_version_id",
        "teacher_assist_newsletters",
        "teacher_assist_newsletter_versions",
        ["current_version_id"],
        ["id"],
        ondelete="SET NULL",
    )


def downgrade() -> None:
    op.drop_constraint(
        "fk_teacher_assist_newsletters_current_version_id",
        "teacher_assist_newsletters",
        type_="foreignkey",
    )
    op.drop_index(
        op.f("ix_teacher_assist_newsletter_exports_export_format"),
        table_name="teacher_assist_newsletter_exports",
    )
    op.drop_index(
        op.f("ix_teacher_assist_newsletter_exports_newsletter_id"),
        table_name="teacher_assist_newsletter_exports",
    )
    op.drop_index(
        op.f("ix_teacher_assist_newsletter_exports_owner_user_id"),
        table_name="teacher_assist_newsletter_exports",
    )
    op.drop_index(
        op.f("ix_teacher_assist_newsletter_exports_tenant_id"),
        table_name="teacher_assist_newsletter_exports",
    )
    op.drop_table("teacher_assist_newsletter_exports")
    op.drop_index(
        op.f("ix_teacher_assist_newsletter_versions_ai_usage_event_id"),
        table_name="teacher_assist_newsletter_versions",
    )
    op.drop_index(
        op.f("ix_teacher_assist_newsletter_versions_version_source"),
        table_name="teacher_assist_newsletter_versions",
    )
    op.drop_index(
        op.f("ix_teacher_assist_newsletter_versions_newsletter_id"),
        table_name="teacher_assist_newsletter_versions",
    )
    op.drop_index(
        op.f("ix_teacher_assist_newsletter_versions_owner_user_id"),
        table_name="teacher_assist_newsletter_versions",
    )
    op.drop_index(
        op.f("ix_teacher_assist_newsletter_versions_tenant_id"),
        table_name="teacher_assist_newsletter_versions",
    )
    op.drop_table("teacher_assist_newsletter_versions")
    op.drop_index(op.f("ix_teacher_assist_newsletters_current_version_id"), table_name="teacher_assist_newsletters")
    op.drop_index(op.f("ix_teacher_assist_newsletters_status"), table_name="teacher_assist_newsletters")
    op.drop_index(op.f("ix_teacher_assist_newsletters_subject_id"), table_name="teacher_assist_newsletters")
    op.drop_index(op.f("ix_teacher_assist_newsletters_class_id"), table_name="teacher_assist_newsletters")
    op.drop_index(op.f("ix_teacher_assist_newsletters_grading_period_id"), table_name="teacher_assist_newsletters")
    op.drop_index(op.f("ix_teacher_assist_newsletters_school_year_id"), table_name="teacher_assist_newsletters")
    op.drop_index(op.f("ix_teacher_assist_newsletters_owner_user_id"), table_name="teacher_assist_newsletters")
    op.drop_index(op.f("ix_teacher_assist_newsletters_tenant_id"), table_name="teacher_assist_newsletters")
    op.drop_table("teacher_assist_newsletters")
