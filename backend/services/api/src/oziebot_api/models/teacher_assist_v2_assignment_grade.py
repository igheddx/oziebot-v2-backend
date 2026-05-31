from __future__ import annotations

import uuid
from datetime import datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy import DateTime, Float, ForeignKey, Integer, JSON, String, Text, Uuid
from sqlalchemy.orm import Mapped, mapped_column, relationship

from oziebot_api.db.base import Base


class TeacherAssistV2AssignmentGrade(Base):
    __tablename__ = "teacher_assist_v2_assignment_grades"

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False, index=True
    )
    teacher_user_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True
    )
    assignment_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("teacher_assist_v2_assignments.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    student_submission_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("teacher_assist_v2_student_submissions.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    student_number: Mapped[int | None] = mapped_column(Integer, nullable=True)
    grading_draft_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("teacher_assist_v2_grading_drafts.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    score: Mapped[float] = mapped_column(Float(), nullable=False)
    max_score: Mapped[float] = mapped_column(Float(), nullable=False)
    percentage: Mapped[float] = mapped_column(Float(), nullable=False)
    rubric_json: Mapped[dict[str, Any]] = mapped_column(JSON(), nullable=False)
    teacher_comment: Mapped[str] = mapped_column(Text(), nullable=False)
    teacher_override_reason: Mapped[str | None] = mapped_column(Text(), nullable=True)
    review_action: Mapped[str] = mapped_column(String(32), nullable=False)
    confirmed_by: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("users.id", ondelete="SET NULL"), nullable=True
    )
    confirmed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    audit_events: Mapped[list["TeacherAssistV2AssignmentGradeAuditEvent"]] = relationship(
        "TeacherAssistV2AssignmentGradeAuditEvent",
        back_populates="assignment_grade",
    )


if TYPE_CHECKING:
    from oziebot_api.models.teacher_assist_v2_assignment_grade_audit_event import (
        TeacherAssistV2AssignmentGradeAuditEvent,
    )
