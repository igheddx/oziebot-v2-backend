"""TeacherAssist v2 teacher onboarding, temp password, and pacing guide assignments."""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "072_teacher_assist_v2_teacher_onboarding"
down_revision = "071_v2_instructional_foundation"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "users",
        sa.Column("must_change_password", sa.Boolean(), nullable=False, server_default=sa.text("false")),
    )

    op.create_table(
        "teacher_assist_v2_onboarding",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("school_year_id", sa.Uuid(), nullable=True),
        sa.Column("state_id", sa.Uuid(), nullable=True),
        sa.Column("district_id", sa.Uuid(), nullable=True),
        sa.Column("school_id", sa.Uuid(), nullable=True),
        sa.Column("grade_id", sa.Uuid(), nullable=True),
        sa.Column("selected_subject_ids", sa.JSON(), nullable=False, server_default=sa.text("'[]'")),
        sa.Column("student_count", sa.Integer(), nullable=True),
        sa.Column("onboarding_completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("pacing_guide_setup_completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["district_id"], ["education_districts.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["grade_id"], ["education_grades.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["school_id"], ["education_schools.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["school_year_id"], ["education_school_years.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["state_id"], ["education_states.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("user_id"),
    )
    op.create_index("ix_teacher_assist_v2_onboarding_user_id", "teacher_assist_v2_onboarding", ["user_id"])
    op.create_index("ix_teacher_assist_v2_onboarding_tenant_id", "teacher_assist_v2_onboarding", ["tenant_id"])

    op.create_table(
        "teacher_assist_v2_pacing_guide_assignments",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("school_year_id", sa.Uuid(), nullable=False),
        sa.Column("grade_id", sa.Uuid(), nullable=False),
        sa.Column("subject_id", sa.Uuid(), nullable=False),
        sa.Column("pacing_guide_id", sa.Uuid(), nullable=False),
        sa.Column("guide_scope", sa.String(length=32), nullable=False),
        sa.Column("active", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["grade_id"], ["education_grades.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["pacing_guide_id"], ["pacing_guides.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["school_year_id"], ["school_years.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["subject_id"], ["education_subjects.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "user_id",
            "school_year_id",
            "subject_id",
            name="uq_teacher_assist_v2_pacing_assignment_user_year_subject",
        ),
    )
    op.create_index(
        "ix_teacher_assist_v2_pacing_guide_assignments_user_id",
        "teacher_assist_v2_pacing_guide_assignments",
        ["user_id"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_teacher_assist_v2_pacing_guide_assignments_user_id",
        table_name="teacher_assist_v2_pacing_guide_assignments",
    )
    op.drop_table("teacher_assist_v2_pacing_guide_assignments")
    op.drop_index("ix_teacher_assist_v2_onboarding_tenant_id", table_name="teacher_assist_v2_onboarding")
    op.drop_index("ix_teacher_assist_v2_onboarding_user_id", table_name="teacher_assist_v2_onboarding")
    op.drop_table("teacher_assist_v2_onboarding")
    op.drop_column("users", "must_change_password")
