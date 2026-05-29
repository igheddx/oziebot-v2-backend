"""add instructional week foundation

Revision ID: 064_teacher_assist_instructional_week_foundation
Revises: 063_teacher_assist_time_savings_foundation
Create Date: 2026-05-30 12:00:00.000000
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Union

from alembic import op
import sqlalchemy as sa


revision: str = "064_teacher_assist_instructional_week_foundation"
down_revision: Union[str, None] = "063_teacher_assist_time_savings_foundation"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "instructional_weeks",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("school_year_id", sa.Uuid(), nullable=False),
        sa.Column("grading_period_id", sa.Uuid(), nullable=True),
        sa.Column("pacing_guide_id", sa.Uuid(), nullable=False),
        sa.Column("pacing_guide_period_id", sa.Uuid(), nullable=False),
        sa.Column("week_number", sa.Integer(), nullable=False),
        sa.Column("title", sa.String(length=160), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("start_date", sa.Date(), nullable=True),
        sa.Column("end_date", sa.Date(), nullable=True),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("created_by_user_id", sa.Uuid(), nullable=False),
        sa.Column("updated_by_user_id", sa.Uuid(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["created_by_user_id"], ["users.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["grading_period_id"], ["grading_periods.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["pacing_guide_id"], ["pacing_guides.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["pacing_guide_period_id"], ["pacing_guide_periods.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["school_year_id"], ["school_years.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["updated_by_user_id"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_instructional_weeks_tenant_id", "instructional_weeks", ["tenant_id"])
    op.create_index("ix_instructional_weeks_pacing_guide_period_id", "instructional_weeks", ["pacing_guide_period_id"])
    op.create_index("ix_instructional_weeks_created_by_user_id", "instructional_weeks", ["created_by_user_id"])
    op.create_index(
        "uq_instructional_weeks_teacher_period",
        "instructional_weeks",
        ["tenant_id", "created_by_user_id", "pacing_guide_period_id"],
        unique=True,
    )

    op.create_table(
        "instructional_week_objectives",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("instructional_week_id", sa.Uuid(), nullable=False),
        sa.Column("objective_id", sa.Uuid(), nullable=True),
        sa.Column("objective_code", sa.String(length=64), nullable=True),
        sa.Column("source_type", sa.String(length=32), nullable=False),
        sa.Column("is_required", sa.Boolean(), nullable=False, server_default="true"),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default="true"),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["instructional_week_id"], ["instructional_weeks.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["objective_id"], ["education_objectives.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_instructional_week_objectives_week_id",
        "instructional_week_objectives",
        ["instructional_week_id"],
    )

    op.create_table(
        "instructional_week_snapshots",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("instructional_week_id", sa.Uuid(), nullable=False),
        sa.Column("name", sa.String(length=160), nullable=False),
        sa.Column("snapshot_data", sa.JSON(), nullable=False),
        sa.Column("created_by_user_id", sa.Uuid(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["created_by_user_id"], ["users.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["instructional_week_id"], ["instructional_weeks.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_instructional_week_snapshots_week_id",
        "instructional_week_snapshots",
        ["instructional_week_id"],
    )

    for table in ("weekly_plans", "assignments", "teacher_assist_newsletters", "teacher_assist_generated_artifacts"):
        op.add_column(table, sa.Column("instructional_week_id", sa.Uuid(), nullable=True))
        op.create_index(f"ix_{table}_instructional_week_id", table, ["instructional_week_id"])
        op.create_foreign_key(
            f"fk_{table}_instructional_week_id",
            table,
            "instructional_weeks",
            ["instructional_week_id"],
            ["id"],
            ondelete="SET NULL",
        )


def downgrade() -> None:
    for table in ("teacher_assist_generated_artifacts", "teacher_assist_newsletters", "assignments", "weekly_plans"):
        op.drop_constraint(f"fk_{table}_instructional_week_id", table, type_="foreignkey")
        op.drop_index(f"ix_{table}_instructional_week_id", table_name=table)
        op.drop_column(table, "instructional_week_id")

    op.drop_index("ix_instructional_week_snapshots_week_id", table_name="instructional_week_snapshots")
    op.drop_table("instructional_week_snapshots")
    op.drop_index("ix_instructional_week_objectives_week_id", table_name="instructional_week_objectives")
    op.drop_table("instructional_week_objectives")
    op.drop_index("uq_instructional_weeks_teacher_period", table_name="instructional_weeks")
    op.drop_index("ix_instructional_weeks_created_by_user_id", table_name="instructional_weeks")
    op.drop_index("ix_instructional_weeks_pacing_guide_period_id", table_name="instructional_weeks")
    op.drop_index("ix_instructional_weeks_tenant_id", table_name="instructional_weeks")
    op.drop_table("instructional_weeks")
