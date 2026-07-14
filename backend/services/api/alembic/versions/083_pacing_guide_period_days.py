"""Pacing guide period days and day-level supporting material linkage."""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime

import sqlalchemy as sa
from alembic import op

revision = "083_pacing_guide_period_days"
down_revision = "082_pacing_guide_builder_metadata"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "pacing_guide_period_days",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("period_id", sa.Uuid(), nullable=False),
        sa.Column("day_label", sa.String(length=32), nullable=False),
        sa.Column("sequence_number", sa.Integer(), nullable=False),
        sa.Column("daily_topic", sa.Text(), nullable=False),
        sa.Column("objective_focus", sa.Text(), nullable=True),
        sa.Column("teacher_notes", sa.Text(), nullable=True),
        sa.Column("materials_needed", sa.Text(), nullable=True),
        sa.Column("assessment_check", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["period_id"], ["pacing_guide_periods.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_pacing_guide_period_days_period_id", "pacing_guide_period_days", ["period_id"]
    )

    op.add_column(
        "pacing_guide_supporting_materials",
        sa.Column("period_day_id", sa.Uuid(), nullable=True),
    )
    op.add_column(
        "pacing_guide_supporting_materials",
        sa.Column("source_resource_id", sa.Uuid(), nullable=True),
    )
    op.add_column(
        "pacing_guide_supporting_materials",
        sa.Column("source_pacing_guide_id", sa.Uuid(), nullable=True),
    )
    op.add_column(
        "pacing_guide_supporting_materials",
        sa.Column("archived_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_foreign_key(
        "fk_pacing_guide_supporting_materials_period_day_id",
        "pacing_guide_supporting_materials",
        "pacing_guide_period_days",
        ["period_day_id"],
        ["id"],
        ondelete="CASCADE",
    )
    op.create_foreign_key(
        "fk_pacing_guide_supporting_materials_source_resource_id",
        "pacing_guide_supporting_materials",
        "pacing_guide_supporting_materials",
        ["source_resource_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_foreign_key(
        "fk_pacing_guide_supporting_materials_source_pacing_guide_id",
        "pacing_guide_supporting_materials",
        "pacing_guides",
        ["source_pacing_guide_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_index(
        "ix_pacing_guide_supporting_materials_period_day_id",
        "pacing_guide_supporting_materials",
        ["period_day_id"],
    )

    connection = op.get_bind()
    periods = connection.execute(
        sa.text(
            "SELECT id, metadata_json FROM pacing_guide_periods WHERE metadata_json IS NOT NULL"
        )
    ).fetchall()
    now = datetime.now(UTC)
    for period_id, metadata_json in periods:
        metadata = (
            metadata_json if isinstance(metadata_json, dict) else json.loads(metadata_json or "{}")
        )
        daily_plans = metadata.get("daily_plans") or []
        if not isinstance(daily_plans, list):
            continue
        for index, item in enumerate(daily_plans, start=1):
            if not isinstance(item, dict):
                continue
            day_label = str(item.get("day_label") or "").strip()
            daily_topic = str(item.get("daily_topic") or "").strip()
            if not day_label or not daily_topic:
                continue
            connection.execute(
                sa.text(
                    """
                    INSERT INTO pacing_guide_period_days (
                        id, period_id, day_label, sequence_number, daily_topic,
                        objective_focus, teacher_notes, materials_needed, assessment_check,
                        created_at, updated_at
                    ) VALUES (
                        :id, :period_id, :day_label, :sequence_number, :daily_topic,
                        :objective_focus, :teacher_notes, :materials_needed, :assessment_check,
                        :created_at, :updated_at
                    )
                    """
                ),
                {
                    "id": str(uuid.uuid4()),
                    "period_id": str(period_id),
                    "day_label": day_label,
                    "sequence_number": index,
                    "daily_topic": daily_topic,
                    "objective_focus": item.get("objective_focus"),
                    "teacher_notes": item.get("teacher_notes"),
                    "materials_needed": item.get("materials_needed"),
                    "assessment_check": item.get("assessment_check"),
                    "created_at": now,
                    "updated_at": now,
                },
            )


def downgrade() -> None:
    op.drop_index(
        "ix_pacing_guide_supporting_materials_period_day_id", "pacing_guide_supporting_materials"
    )
    op.drop_constraint(
        "fk_pacing_guide_supporting_materials_source_pacing_guide_id",
        "pacing_guide_supporting_materials",
        type_="foreignkey",
    )
    op.drop_constraint(
        "fk_pacing_guide_supporting_materials_source_resource_id",
        "pacing_guide_supporting_materials",
        type_="foreignkey",
    )
    op.drop_constraint(
        "fk_pacing_guide_supporting_materials_period_day_id",
        "pacing_guide_supporting_materials",
        type_="foreignkey",
    )
    op.drop_column("pacing_guide_supporting_materials", "archived_at")
    op.drop_column("pacing_guide_supporting_materials", "source_pacing_guide_id")
    op.drop_column("pacing_guide_supporting_materials", "source_resource_id")
    op.drop_column("pacing_guide_supporting_materials", "period_day_id")
    op.drop_index("ix_pacing_guide_period_days_period_id", "pacing_guide_period_days")
    op.drop_table("pacing_guide_period_days")
