"""add teacher assist instructional planning fields

Revision ID: 042_teacher_assist_instructional_planning
Revises: 041_teacher_assist_output_refinement
Create Date: 2026-05-27 20:00:00.000000
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Union

from alembic import op
import sqlalchemy as sa


revision: str = "042_teacher_assist_instructional_planning"
down_revision: Union[str, None] = "041_teacher_assist_output_refinement"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "planning_input_drafts",
        sa.Column("planning_scope", sa.String(length=32), nullable=False, server_default="weekly"),
    )
    op.add_column(
        "planning_input_drafts", sa.Column("module_title", sa.String(length=160), nullable=True)
    )
    op.add_column("planning_input_drafts", sa.Column("start_date", sa.Date(), nullable=True))
    op.add_column("planning_input_drafts", sa.Column("end_date", sa.Date(), nullable=True))
    op.add_column(
        "planning_input_drafts", sa.Column("estimated_weeks", sa.Integer(), nullable=True)
    )
    op.add_column(
        "planning_input_drafts", sa.Column("instructional_days_count", sa.Integer(), nullable=True)
    )

    op.add_column(
        "weekly_plans",
        sa.Column("planning_scope", sa.String(length=32), nullable=False, server_default="weekly"),
    )
    op.add_column("weekly_plans", sa.Column("module_title", sa.String(length=160), nullable=True))
    op.add_column("weekly_plans", sa.Column("start_date", sa.Date(), nullable=True))
    op.add_column("weekly_plans", sa.Column("end_date", sa.Date(), nullable=True))
    op.add_column("weekly_plans", sa.Column("estimated_weeks", sa.Integer(), nullable=True))
    op.add_column(
        "weekly_plans", sa.Column("instructional_days_count", sa.Integer(), nullable=True)
    )
    op.add_column("weekly_plans", sa.Column("owner_user_id", sa.Uuid(), nullable=True))
    op.add_column("weekly_plans", sa.Column("source_plan_id", sa.Uuid(), nullable=True))
    op.add_column("weekly_plans", sa.Column("derived_from_plan_id", sa.Uuid(), nullable=True))
    op.add_column(
        "weekly_plans",
        sa.Column("is_template", sa.Boolean(), nullable=False, server_default=sa.false()),
    )
    op.add_column(
        "weekly_plans",
        sa.Column(
            "visibility_scope", sa.String(length=32), nullable=False, server_default="private"
        ),
    )
    op.add_column(
        "weekly_plans",
        sa.Column("reuse_status", sa.String(length=32), nullable=False, server_default="active"),
    )
    op.add_column("weekly_plans", sa.Column("school_year_origin_id", sa.Uuid(), nullable=True))

    op.execute("UPDATE weekly_plans SET owner_user_id = user_id WHERE owner_user_id IS NULL")
    op.alter_column("weekly_plans", "owner_user_id", nullable=False)

    op.create_foreign_key(
        "fk_weekly_plans_owner_user_id_users",
        "weekly_plans",
        "users",
        ["owner_user_id"],
        ["id"],
        ondelete="CASCADE",
    )
    op.create_foreign_key(
        "fk_weekly_plans_source_plan_id_weekly_plans",
        "weekly_plans",
        "weekly_plans",
        ["source_plan_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_foreign_key(
        "fk_weekly_plans_derived_from_plan_id_weekly_plans",
        "weekly_plans",
        "weekly_plans",
        ["derived_from_plan_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_foreign_key(
        "fk_weekly_plans_school_year_origin_id_school_years",
        "weekly_plans",
        "school_years",
        ["school_year_origin_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_index("ix_weekly_plans_owner_user_id", "weekly_plans", ["owner_user_id"])
    op.create_index("ix_weekly_plans_source_plan_id", "weekly_plans", ["source_plan_id"])
    op.create_index(
        "ix_weekly_plans_derived_from_plan_id", "weekly_plans", ["derived_from_plan_id"]
    )
    op.create_index(
        "ix_weekly_plans_school_year_origin_id", "weekly_plans", ["school_year_origin_id"]
    )


def downgrade() -> None:
    op.drop_index("ix_weekly_plans_school_year_origin_id", table_name="weekly_plans")
    op.drop_index("ix_weekly_plans_derived_from_plan_id", table_name="weekly_plans")
    op.drop_index("ix_weekly_plans_source_plan_id", table_name="weekly_plans")
    op.drop_index("ix_weekly_plans_owner_user_id", table_name="weekly_plans")
    op.drop_constraint(
        "fk_weekly_plans_school_year_origin_id_school_years", "weekly_plans", type_="foreignkey"
    )
    op.drop_constraint(
        "fk_weekly_plans_derived_from_plan_id_weekly_plans", "weekly_plans", type_="foreignkey"
    )
    op.drop_constraint(
        "fk_weekly_plans_source_plan_id_weekly_plans", "weekly_plans", type_="foreignkey"
    )
    op.drop_constraint("fk_weekly_plans_owner_user_id_users", "weekly_plans", type_="foreignkey")
    op.drop_column("weekly_plans", "school_year_origin_id")
    op.drop_column("weekly_plans", "reuse_status")
    op.drop_column("weekly_plans", "visibility_scope")
    op.drop_column("weekly_plans", "is_template")
    op.drop_column("weekly_plans", "derived_from_plan_id")
    op.drop_column("weekly_plans", "source_plan_id")
    op.drop_column("weekly_plans", "owner_user_id")
    op.drop_column("weekly_plans", "instructional_days_count")
    op.drop_column("weekly_plans", "estimated_weeks")
    op.drop_column("weekly_plans", "end_date")
    op.drop_column("weekly_plans", "start_date")
    op.drop_column("weekly_plans", "module_title")
    op.drop_column("weekly_plans", "planning_scope")
    op.drop_column("planning_input_drafts", "instructional_days_count")
    op.drop_column("planning_input_drafts", "estimated_weeks")
    op.drop_column("planning_input_drafts", "end_date")
    op.drop_column("planning_input_drafts", "start_date")
    op.drop_column("planning_input_drafts", "module_title")
    op.drop_column("planning_input_drafts", "planning_scope")
