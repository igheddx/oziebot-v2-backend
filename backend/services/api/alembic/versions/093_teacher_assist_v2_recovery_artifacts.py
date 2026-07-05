"""TeacherAssist v2 Recovery Artifacts — Phase 8 Learning Recovery Planner."""

from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB
from alembic import op

revision = "093_teacher_assist_v2_recovery_artifacts"
down_revision = "092_teacher_assist_v2_recovery_queue"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "teacher_assist_v2_recovery_artifacts",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("teacher_user_id", sa.Uuid(), nullable=False),
        sa.Column("recovery_queue_id", sa.Uuid(), nullable=True),
        sa.Column("artifact_type", sa.Text(), nullable=False),
        sa.Column("title", sa.Text(), nullable=False),
        sa.Column("content_json", JSONB(), nullable=True),
        sa.Column("generation_context_snapshot_json", JSONB(), nullable=True),
        sa.Column("validation_result_json", JSONB(), nullable=True),
        sa.Column("provider", sa.Text(), nullable=True),
        sa.Column("model", sa.Text(), nullable=True),
        sa.Column("status", sa.String(length=32), nullable=False, server_default="ready"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["teacher_user_id"], ["users.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(
            ["recovery_queue_id"],
            ["teacher_assist_v2_recovery_queue.id"],
            ondelete="SET NULL",
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_ta_v2_recovery_artifacts_queue",
        "teacher_assist_v2_recovery_artifacts",
        ["recovery_queue_id"],
    )
    op.create_index(
        "ix_ta_v2_recovery_artifacts_teacher_status",
        "teacher_assist_v2_recovery_artifacts",
        ["teacher_user_id", "status"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_ta_v2_recovery_artifacts_teacher_status",
        table_name="teacher_assist_v2_recovery_artifacts",
    )
    op.drop_index(
        "ix_ta_v2_recovery_artifacts_queue",
        table_name="teacher_assist_v2_recovery_artifacts",
    )
    op.drop_table("teacher_assist_v2_recovery_artifacts")
