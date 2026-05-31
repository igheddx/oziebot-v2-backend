from __future__ import annotations

import uuid
from datetime import datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy import DateTime, Float, ForeignKey, JSON, String, Text, Uuid
from sqlalchemy.orm import Mapped, mapped_column, relationship

from oziebot_api.db.base import Base


class TeacherAssistV2AssignmentGradeAuditEvent(Base):
    __tablename__ = "teacher_assist_v2_assignment_grade_audit_events"

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False, index=True
    )
    assignment_grade_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("teacher_assist_v2_assignment_grades.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
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
    grading_draft_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("teacher_assist_v2_grading_drafts.id", ondelete="SET NULL"),
        nullable=True,
    )
    original_ai_score: Mapped[float | None] = mapped_column(Float(), nullable=True)
    original_ai_max_score: Mapped[float | None] = mapped_column(Float(), nullable=True)
    final_score: Mapped[float] = mapped_column(Float(), nullable=False)
    final_max_score: Mapped[float] = mapped_column(Float(), nullable=False)
    score_difference: Mapped[float | None] = mapped_column(Float(), nullable=True)
    teacher_override_reason: Mapped[str | None] = mapped_column(Text(), nullable=True)
    review_action: Mapped[str] = mapped_column(String(32), nullable=False)
    teacher_comment: Mapped[str] = mapped_column(Text(), nullable=False)
    rubric_json: Mapped[dict[str, Any]] = mapped_column(JSON(), nullable=False)
    reviewer_user_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    assignment_grade: Mapped["TeacherAssistV2AssignmentGrade"] = relationship(
        "TeacherAssistV2AssignmentGrade",
        back_populates="audit_events",
    )


if TYPE_CHECKING:
    from oziebot_api.models.teacher_assist_v2_assignment_grade import TeacherAssistV2AssignmentGrade
