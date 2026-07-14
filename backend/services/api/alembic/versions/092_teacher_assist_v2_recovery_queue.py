"""TeacherAssist v2 Instructional Recovery Queue."""

from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB
from alembic import op

revision = "092_teacher_assist_v2_recovery_queue"
down_revision = "091_teacher_assist_v2_coaching_summary"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "teacher_assist_v2_recovery_queue",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("teacher_user_id", sa.Uuid(), nullable=False),
        # Source context — at least one populated
        sa.Column("assignment_id", sa.Uuid(), nullable=True),
        sa.Column("instructional_package_id", sa.Uuid(), nullable=True),
        sa.Column("education_objective_id", sa.Uuid(), nullable=True),
        # Denormalized label (stable even if objective is renamed)
        sa.Column("objective_code", sa.String(length=64), nullable=True),
        # Recommendation data (snapshot at creation — immutable)
        sa.Column("recommendation_type", sa.String(length=32), nullable=False),
        sa.Column("students_affected_json", JSONB(), nullable=False),
        sa.Column("misconception_text", sa.Text(), nullable=True),
        sa.Column("reason", sa.Text(), nullable=False),
        sa.Column("evidence_snapshot_json", JSONB(), nullable=True),
        sa.Column("mastery_snapshot_json", JSONB(), nullable=True),
        sa.Column("strategy_metadata_json", JSONB(), nullable=True),
        # Priority
        sa.Column("priority", sa.String(length=16), nullable=False, server_default="MEDIUM"),
        sa.Column("suggested_priority", sa.String(length=16), nullable=True),
        # Knowledge-dependency deadline
        sa.Column("best_before", sa.Date(), nullable=True),
        sa.Column("best_before_reason", sa.Text(), nullable=True),
        # Teacher workflow
        sa.Column("status", sa.String(length=32), nullable=False, server_default="pending"),
        sa.Column("teacher_response", sa.String(length=64), nullable=True),
        sa.Column("teacher_notes", sa.Text(), nullable=True),
        sa.Column("scheduled_for", sa.Date(), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        # Phase 8 stubs — nullable, not used in Phase 7
        sa.Column("success_criteria_json", JSONB(), nullable=True),
        sa.Column("timeline_phase", sa.String(length=64), nullable=True),
        sa.Column("post_recovery_mastery_snapshot_json", JSONB(), nullable=True),
        # Timestamps
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["assignment_id"], ["teacher_assist_v2_assignments.id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(
            ["education_objective_id"], ["education_objectives.id"], ondelete="SET NULL"
        ),
        sa.ForeignKeyConstraint(
            ["instructional_package_id"],
            ["teacher_assist_v2_instructional_packages.id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(["teacher_user_id"], ["users.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_ta_v2_recovery_queue_teacher_status",
        "teacher_assist_v2_recovery_queue",
        ["teacher_user_id", "status"],
    )
    op.create_index(
        "ix_ta_v2_recovery_queue_assignment",
        "teacher_assist_v2_recovery_queue",
        ["assignment_id"],
    )
    op.create_index(
        "ix_ta_v2_recovery_queue_package",
        "teacher_assist_v2_recovery_queue",
        ["instructional_package_id"],
    )


def downgrade() -> None:
    op.drop_index("ix_ta_v2_recovery_queue_package", table_name="teacher_assist_v2_recovery_queue")
    op.drop_index(
        "ix_ta_v2_recovery_queue_assignment", table_name="teacher_assist_v2_recovery_queue"
    )
    op.drop_index(
        "ix_ta_v2_recovery_queue_teacher_status", table_name="teacher_assist_v2_recovery_queue"
    )
    op.drop_table("teacher_assist_v2_recovery_queue")
