"""Instructional package lifecycle fields and close-out support."""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "075_instructional_package_lifecycle"
down_revision = "074_teacher_assist_v2_instructional_packages"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "teacher_assist_v2_instructional_packages",
        sa.Column("title", sa.String(length=256), nullable=True),
    )
    op.add_column(
        "teacher_assist_v2_instructional_packages",
        sa.Column("primary_pacing_guide_id", sa.Uuid(), nullable=True),
    )
    op.add_column(
        "teacher_assist_v2_instructional_packages",
        sa.Column("plan_start_date", sa.Date(), nullable=True),
    )
    op.add_column(
        "teacher_assist_v2_instructional_packages",
        sa.Column("plan_end_date", sa.Date(), nullable=True),
    )
    op.add_column(
        "teacher_assist_v2_instructional_packages",
        sa.Column("closed_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "teacher_assist_v2_instructional_packages",
        sa.Column("closed_by_user_id", sa.Uuid(), nullable=True),
    )
    op.add_column(
        "teacher_assist_v2_instructional_packages",
        sa.Column("close_out_notes", sa.Text(), nullable=True),
    )
    op.create_foreign_key(
        "fk_ta_v2_packages_primary_pacing_guide",
        "teacher_assist_v2_instructional_packages",
        "pacing_guides",
        ["primary_pacing_guide_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_foreign_key(
        "fk_ta_v2_packages_closed_by",
        "teacher_assist_v2_instructional_packages",
        "users",
        ["closed_by_user_id"],
        ["id"],
        ondelete="SET NULL",
    )

    op.execute(
        sa.text(
            """
            UPDATE teacher_assist_v2_instructional_packages
            SET plan_start_date = (created_at AT TIME ZONE 'UTC')::date,
                plan_end_date = ((created_at AT TIME ZONE 'UTC')::date + INTERVAL '6 days')::date,
                title = CASE
                    WHEN week_start = week_end THEN 'Week ' || week_start::text || ' Instructional Package'
                    ELSE 'Weeks ' || week_start::text || '-' || week_end::text || ' Instructional Package'
                END,
                status = CASE WHEN status = 'ready' THEN 'generated' ELSE status END
            WHERE plan_start_date IS NULL
            """
        )
    )

    op.alter_column("teacher_assist_v2_instructional_packages", "plan_start_date", nullable=False)
    op.alter_column("teacher_assist_v2_instructional_packages", "plan_end_date", nullable=False)
    op.alter_column("teacher_assist_v2_instructional_packages", "title", nullable=False)


def downgrade() -> None:
    op.drop_constraint(
        "fk_ta_v2_packages_closed_by",
        "teacher_assist_v2_instructional_packages",
        type_="foreignkey",
    )
    op.drop_constraint(
        "fk_ta_v2_packages_primary_pacing_guide",
        "teacher_assist_v2_instructional_packages",
        type_="foreignkey",
    )
    op.drop_column("teacher_assist_v2_instructional_packages", "close_out_notes")
    op.drop_column("teacher_assist_v2_instructional_packages", "closed_by_user_id")
    op.drop_column("teacher_assist_v2_instructional_packages", "closed_at")
    op.drop_column("teacher_assist_v2_instructional_packages", "plan_end_date")
    op.drop_column("teacher_assist_v2_instructional_packages", "plan_start_date")
    op.drop_column("teacher_assist_v2_instructional_packages", "primary_pacing_guide_id")
    op.drop_column("teacher_assist_v2_instructional_packages", "title")
