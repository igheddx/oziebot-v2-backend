from __future__ import annotations

import uuid
from datetime import date, datetime
from typing import Any

from sqlalchemy import Date, DateTime, ForeignKey, JSON, String, Text, Uuid
from sqlalchemy.orm import Mapped, mapped_column

from oziebot_api.db.base import Base


class TeacherAssistV2RecoveryQueue(Base):
    __tablename__ = "teacher_assist_v2_recovery_queue"

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False
    )
    teacher_user_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True
    )

    # Source context
    assignment_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("teacher_assist_v2_assignments.id", ondelete="CASCADE"),
        nullable=True,
        index=True,
    )
    instructional_package_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("teacher_assist_v2_instructional_packages.id", ondelete="CASCADE"),
        nullable=True,
        index=True,
    )
    education_objective_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("education_objectives.id", ondelete="SET NULL"),
        nullable=True,
    )
    objective_code: Mapped[str | None] = mapped_column(String(64), nullable=True)

    # Recommendation data — immutable snapshot at creation time
    recommendation_type: Mapped[str] = mapped_column(String(32), nullable=False)
    students_affected_json: Mapped[list[Any]] = mapped_column(JSON(), nullable=False, default=list)
    misconception_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    reason: Mapped[str] = mapped_column(Text, nullable=False)
    evidence_snapshot_json: Mapped[dict[str, Any] | None] = mapped_column(JSON(), nullable=True)
    mastery_snapshot_json: Mapped[dict[str, Any] | None] = mapped_column(JSON(), nullable=True)
    strategy_metadata_json: Mapped[dict[str, Any] | None] = mapped_column(JSON(), nullable=True)

    # Priority
    priority: Mapped[str] = mapped_column(String(16), nullable=False, default="MEDIUM")
    suggested_priority: Mapped[str | None] = mapped_column(String(16), nullable=True)

    # Knowledge-dependency deadline
    best_before: Mapped[date | None] = mapped_column(Date, nullable=True)
    best_before_reason: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Teacher workflow
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="pending")
    teacher_response: Mapped[str | None] = mapped_column(String(64), nullable=True)
    teacher_notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    scheduled_for: Mapped[date | None] = mapped_column(Date, nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    # Phase 8 stubs — nullable, unused in Phase 7
    success_criteria_json: Mapped[dict[str, Any] | None] = mapped_column(JSON(), nullable=True)
    timeline_phase: Mapped[str | None] = mapped_column(String(64), nullable=True)
    post_recovery_mastery_snapshot_json: Mapped[dict[str, Any] | None] = mapped_column(JSON(), nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
