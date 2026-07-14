"""extend pacing guides with catalog-aligned foundation

Revision ID: 060_teacher_assist_pacing_guide_foundation
Revises: 059_education_catalog_foundation
Create Date: 2026-05-29 18:00:00.000000
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Union

from alembic import op
import sqlalchemy as sa


revision: str = "060_teacher_assist_pacing_guide_foundation"
down_revision: Union[str, None] = "059_education_catalog_foundation"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "pacing_guides",
        sa.Column("guide_type", sa.String(length=32), nullable=False, server_default="TEACHER"),
    )
    op.add_column(
        "pacing_guides", sa.Column("school_year_label", sa.String(length=32), nullable=True)
    )
    op.add_column("pacing_guides", sa.Column("catalog_state_id", sa.Uuid(), nullable=True))
    op.add_column("pacing_guides", sa.Column("catalog_district_id", sa.Uuid(), nullable=True))
    op.add_column("pacing_guides", sa.Column("catalog_school_id", sa.Uuid(), nullable=True))
    op.add_column("pacing_guides", sa.Column("catalog_grade_id", sa.Uuid(), nullable=True))
    op.add_column("pacing_guides", sa.Column("catalog_subject_id", sa.Uuid(), nullable=True))
    op.add_column(
        "pacing_guides",
        sa.Column("is_template", sa.Boolean(), nullable=False, server_default="false"),
    )
    op.add_column(
        "pacing_guides", sa.Column("is_active", sa.Boolean(), nullable=False, server_default="true")
    )
    op.add_column(
        "pacing_guides",
        sa.Column("updated_by_user_id", sa.Uuid(), nullable=True),
    )
    op.create_foreign_key(
        "fk_pacing_guides_catalog_state_id",
        "pacing_guides",
        "education_states",
        ["catalog_state_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_foreign_key(
        "fk_pacing_guides_catalog_district_id",
        "pacing_guides",
        "education_districts",
        ["catalog_district_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_foreign_key(
        "fk_pacing_guides_catalog_school_id",
        "pacing_guides",
        "education_schools",
        ["catalog_school_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_foreign_key(
        "fk_pacing_guides_catalog_grade_id",
        "pacing_guides",
        "education_grades",
        ["catalog_grade_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_foreign_key(
        "fk_pacing_guides_catalog_subject_id",
        "pacing_guides",
        "education_subjects",
        ["catalog_subject_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_foreign_key(
        "fk_pacing_guides_updated_by_user_id",
        "pacing_guides",
        "users",
        ["updated_by_user_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_index("ix_pacing_guides_guide_type", "pacing_guides", ["guide_type"])
    op.create_index("ix_pacing_guides_catalog_school_id", "pacing_guides", ["catalog_school_id"])
    op.create_index("ix_pacing_guides_is_active", "pacing_guides", ["is_active"])

    op.create_table(
        "pacing_guide_periods",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("pacing_guide_id", sa.Uuid(), nullable=False),
        sa.Column("period_type", sa.String(length=32), nullable=False),
        sa.Column("title", sa.String(length=160), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("sequence_number", sa.Integer(), nullable=False),
        sa.Column("start_date", sa.Date(), nullable=True),
        sa.Column("end_date", sa.Date(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["pacing_guide_id"], ["pacing_guides.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "pacing_guide_id",
            "sequence_number",
            name="uq_pacing_guide_periods_guide_sequence",
        ),
    )
    op.create_index(
        "ix_pacing_guide_periods_pacing_guide_id", "pacing_guide_periods", ["pacing_guide_id"]
    )

    op.create_table(
        "pacing_guide_objectives",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("period_id", sa.Uuid(), nullable=False),
        sa.Column("objective_id", sa.Uuid(), nullable=False),
        sa.Column("is_required", sa.Boolean(), nullable=False, server_default="true"),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["objective_id"], ["education_objectives.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["period_id"], ["pacing_guide_periods.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "period_id", "objective_id", name="uq_pacing_guide_objectives_period_objective"
        ),
    )
    op.create_index(
        "ix_pacing_guide_objectives_period_id", "pacing_guide_objectives", ["period_id"]
    )
    op.create_index(
        "ix_pacing_guide_objectives_objective_id", "pacing_guide_objectives", ["objective_id"]
    )

    op.create_table(
        "pacing_guide_resources",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("period_id", sa.Uuid(), nullable=False),
        sa.Column("catalog_resource_id", sa.Uuid(), nullable=True),
        sa.Column("resource_library_item_id", sa.Uuid(), nullable=True),
        sa.Column("is_primary", sa.Boolean(), nullable=False, server_default="false"),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["catalog_resource_id"], ["education_curriculum_resources.id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(["period_id"], ["pacing_guide_periods.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(
            ["resource_library_item_id"], ["resource_library_items.id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_pacing_guide_resources_period_id", "pacing_guide_resources", ["period_id"])

    op.add_column("weekly_plans", sa.Column("pacing_guide_period_id", sa.Uuid(), nullable=True))
    op.create_foreign_key(
        "fk_weekly_plans_pacing_guide_period_id",
        "weekly_plans",
        "pacing_guide_periods",
        ["pacing_guide_period_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_index(
        "ix_weekly_plans_pacing_guide_period_id", "weekly_plans", ["pacing_guide_period_id"]
    )


def downgrade() -> None:
    op.drop_index("ix_weekly_plans_pacing_guide_period_id", table_name="weekly_plans")
    op.drop_constraint("fk_weekly_plans_pacing_guide_period_id", "weekly_plans", type_="foreignkey")
    op.drop_column("weekly_plans", "pacing_guide_period_id")

    op.drop_index("ix_pacing_guide_resources_period_id", table_name="pacing_guide_resources")
    op.drop_table("pacing_guide_resources")
    op.drop_index("ix_pacing_guide_objectives_objective_id", table_name="pacing_guide_objectives")
    op.drop_index("ix_pacing_guide_objectives_period_id", table_name="pacing_guide_objectives")
    op.drop_table("pacing_guide_objectives")
    op.drop_index("ix_pacing_guide_periods_pacing_guide_id", table_name="pacing_guide_periods")
    op.drop_table("pacing_guide_periods")

    op.drop_index("ix_pacing_guides_is_active", table_name="pacing_guides")
    op.drop_index("ix_pacing_guides_catalog_school_id", table_name="pacing_guides")
    op.drop_index("ix_pacing_guides_guide_type", table_name="pacing_guides")
    op.drop_constraint("fk_pacing_guides_updated_by_user_id", "pacing_guides", type_="foreignkey")
    op.drop_constraint("fk_pacing_guides_catalog_subject_id", "pacing_guides", type_="foreignkey")
    op.drop_constraint("fk_pacing_guides_catalog_grade_id", "pacing_guides", type_="foreignkey")
    op.drop_constraint("fk_pacing_guides_catalog_school_id", "pacing_guides", type_="foreignkey")
    op.drop_constraint("fk_pacing_guides_catalog_district_id", "pacing_guides", type_="foreignkey")
    op.drop_constraint("fk_pacing_guides_catalog_state_id", "pacing_guides", type_="foreignkey")
    op.drop_column("pacing_guides", "updated_by_user_id")
    op.drop_column("pacing_guides", "is_active")
    op.drop_column("pacing_guides", "is_template")
    op.drop_column("pacing_guides", "catalog_subject_id")
    op.drop_column("pacing_guides", "catalog_grade_id")
    op.drop_column("pacing_guides", "catalog_school_id")
    op.drop_column("pacing_guides", "catalog_district_id")
    op.drop_column("pacing_guides", "catalog_state_id")
    op.drop_column("pacing_guides", "school_year_label")
    op.drop_column("pacing_guides", "guide_type")
