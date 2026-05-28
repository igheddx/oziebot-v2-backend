"""add teacher assist planning context foundation

Revision ID: 038_teacher_assist_planning_context
Revises: 037_teacher_assist_foundation
Create Date: 2026-05-27 05:55:00.000000
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Union

from alembic import op
import sqlalchemy as sa


revision: str = "038_teacher_assist_planning_context"
down_revision: Union[str, None] = "037_teacher_assist_foundation"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "pacing_guides",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("school_year_id", sa.Uuid(), nullable=False),
        sa.Column("title", sa.String(length=128), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("grade_level", sa.String(length=32), nullable=True),
        sa.Column("subject_id", sa.Uuid(), nullable=True),
        sa.Column("is_shared", sa.Boolean(), nullable=False, server_default="false"),
        sa.Column("created_by_user_id", sa.Uuid(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["created_by_user_id"], ["users.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["school_year_id"], ["school_years.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["subject_id"], ["subjects.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_pacing_guides_tenant_id", "pacing_guides", ["tenant_id"])
    op.create_index("ix_pacing_guides_school_year_id", "pacing_guides", ["school_year_id"])
    op.create_index("ix_pacing_guides_subject_id", "pacing_guides", ["subject_id"])
    op.create_index("ix_pacing_guides_created_by_user_id", "pacing_guides", ["created_by_user_id"])

    op.create_table(
        "pacing_items",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("pacing_guide_id", sa.Uuid(), nullable=False),
        sa.Column("grading_period_id", sa.Uuid(), nullable=True),
        sa.Column("subject_id", sa.Uuid(), nullable=True),
        sa.Column("week_number", sa.Integer(), nullable=True),
        sa.Column("day_number", sa.Integer(), nullable=True),
        sa.Column("instructional_date", sa.Date(), nullable=True),
        sa.Column("title", sa.String(length=160), nullable=False),
        sa.Column("instructional_focus", sa.Text(), nullable=True),
        sa.Column("objectives", sa.Text(), nullable=True),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("sort_order", sa.Integer(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["grading_period_id"], ["grading_periods.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["pacing_guide_id"], ["pacing_guides.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["subject_id"], ["subjects.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_pacing_items_pacing_guide_id", "pacing_items", ["pacing_guide_id"])
    op.create_index("ix_pacing_items_grading_period_id", "pacing_items", ["grading_period_id"])
    op.create_index("ix_pacing_items_subject_id", "pacing_items", ["subject_id"])

    op.create_table(
        "pacing_item_standards",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("pacing_item_id", sa.Uuid(), nullable=False),
        sa.Column("standard_id", sa.Uuid(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["pacing_item_id"], ["pacing_items.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["standard_id"], ["standards.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("pacing_item_id", "standard_id", name="uq_pacing_item_standard"),
    )
    op.create_index("ix_pacing_item_standards_pacing_item_id", "pacing_item_standards", ["pacing_item_id"])
    op.create_index("ix_pacing_item_standards_standard_id", "pacing_item_standards", ["standard_id"])

    op.create_table(
        "resource_library_items",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("uploaded_by_user_id", sa.Uuid(), nullable=False),
        sa.Column("title", sa.String(length=160), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("resource_type", sa.String(length=32), nullable=False),
        sa.Column("storage_key", sa.String(length=512), nullable=True),
        sa.Column("original_filename", sa.String(length=255), nullable=True),
        sa.Column("mime_type", sa.String(length=255), nullable=True),
        sa.Column("file_size", sa.Integer(), nullable=True),
        sa.Column("external_url", sa.String(length=2048), nullable=True),
        sa.Column("uploaded_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["uploaded_by_user_id"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_resource_library_items_tenant_id", "resource_library_items", ["tenant_id"])
    op.create_index(
        "ix_resource_library_items_uploaded_by_user_id",
        "resource_library_items",
        ["uploaded_by_user_id"],
    )

    op.create_table(
        "pacing_item_resources",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("pacing_item_id", sa.Uuid(), nullable=False),
        sa.Column("resource_library_item_id", sa.Uuid(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["pacing_item_id"], ["pacing_items.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(
            ["resource_library_item_id"],
            ["resource_library_items.id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "pacing_item_id",
            "resource_library_item_id",
            name="uq_pacing_item_resource",
        ),
    )
    op.create_index("ix_pacing_item_resources_pacing_item_id", "pacing_item_resources", ["pacing_item_id"])
    op.create_index(
        "ix_pacing_item_resources_resource_library_item_id",
        "pacing_item_resources",
        ["resource_library_item_id"],
    )

    op.create_table(
        "planning_input_drafts",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("school_year_id", sa.Uuid(), nullable=True),
        sa.Column("grading_period_id", sa.Uuid(), nullable=True),
        sa.Column("class_id", sa.Uuid(), nullable=True),
        sa.Column("subject_id", sa.Uuid(), nullable=True),
        sa.Column("title", sa.String(length=160), nullable=True),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("status", sa.String(length=32), nullable=False, server_default="draft"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["class_id"], ["classes.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["grading_period_id"], ["grading_periods.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["school_year_id"], ["school_years.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["subject_id"], ["subjects.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_planning_input_drafts_tenant_id", "planning_input_drafts", ["tenant_id"])
    op.create_index("ix_planning_input_drafts_user_id", "planning_input_drafts", ["user_id"])
    op.create_index("ix_planning_input_drafts_school_year_id", "planning_input_drafts", ["school_year_id"])
    op.create_index(
        "ix_planning_input_drafts_grading_period_id",
        "planning_input_drafts",
        ["grading_period_id"],
    )
    op.create_index("ix_planning_input_drafts_class_id", "planning_input_drafts", ["class_id"])
    op.create_index("ix_planning_input_drafts_subject_id", "planning_input_drafts", ["subject_id"])

    op.create_table(
        "planning_input_draft_resources",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("planning_input_draft_id", sa.Uuid(), nullable=False),
        sa.Column("resource_library_item_id", sa.Uuid(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["planning_input_draft_id"],
            ["planning_input_drafts.id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["resource_library_item_id"],
            ["resource_library_items.id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "planning_input_draft_id",
            "resource_library_item_id",
            name="uq_planning_draft_resource",
        ),
    )
    op.create_index(
        "ix_planning_input_draft_resources_planning_input_draft_id",
        "planning_input_draft_resources",
        ["planning_input_draft_id"],
    )
    op.create_index(
        "ix_planning_input_draft_resources_resource_library_item_id",
        "planning_input_draft_resources",
        ["resource_library_item_id"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_planning_input_draft_resources_resource_library_item_id",
        table_name="planning_input_draft_resources",
    )
    op.drop_index(
        "ix_planning_input_draft_resources_planning_input_draft_id",
        table_name="planning_input_draft_resources",
    )
    op.drop_table("planning_input_draft_resources")
    op.drop_index("ix_planning_input_drafts_subject_id", table_name="planning_input_drafts")
    op.drop_index("ix_planning_input_drafts_class_id", table_name="planning_input_drafts")
    op.drop_index("ix_planning_input_drafts_grading_period_id", table_name="planning_input_drafts")
    op.drop_index("ix_planning_input_drafts_school_year_id", table_name="planning_input_drafts")
    op.drop_index("ix_planning_input_drafts_user_id", table_name="planning_input_drafts")
    op.drop_index("ix_planning_input_drafts_tenant_id", table_name="planning_input_drafts")
    op.drop_table("planning_input_drafts")
    op.drop_index(
        "ix_pacing_item_resources_resource_library_item_id",
        table_name="pacing_item_resources",
    )
    op.drop_index("ix_pacing_item_resources_pacing_item_id", table_name="pacing_item_resources")
    op.drop_table("pacing_item_resources")
    op.drop_index(
        "ix_resource_library_items_uploaded_by_user_id",
        table_name="resource_library_items",
    )
    op.drop_index("ix_resource_library_items_tenant_id", table_name="resource_library_items")
    op.drop_table("resource_library_items")
    op.drop_index("ix_pacing_item_standards_standard_id", table_name="pacing_item_standards")
    op.drop_index("ix_pacing_item_standards_pacing_item_id", table_name="pacing_item_standards")
    op.drop_table("pacing_item_standards")
    op.drop_index("ix_pacing_items_subject_id", table_name="pacing_items")
    op.drop_index("ix_pacing_items_grading_period_id", table_name="pacing_items")
    op.drop_index("ix_pacing_items_pacing_guide_id", table_name="pacing_items")
    op.drop_table("pacing_items")
    op.drop_index("ix_pacing_guides_created_by_user_id", table_name="pacing_guides")
    op.drop_index("ix_pacing_guides_subject_id", table_name="pacing_guides")
    op.drop_index("ix_pacing_guides_school_year_id", table_name="pacing_guides")
    op.drop_index("ix_pacing_guides_tenant_id", table_name="pacing_guides")
    op.drop_table("pacing_guides")
