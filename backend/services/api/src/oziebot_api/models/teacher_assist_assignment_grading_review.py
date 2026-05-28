from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import JSON, DateTime, Float, ForeignKey, String, Text, Uuid
from sqlalchemy.orm import Mapped, mapped_column, relationship

from oziebot_api.db.base import Base


class TeacherAssistAssignmentGradingReview(Base):
    __tablename__ = "assignment_grading_reviews"

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("tenants.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    teacher_user_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    assignment_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("assignments.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    student_work_submission_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("assignment_student_work_submissions.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
        unique=True,
    )
    student_number: Mapped[int] = mapped_column(nullable=False)
    school_year_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("school_years.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    grading_period_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("grading_periods.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    class_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("classes.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    subject_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("subjects.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    review_source: Mapped[str] = mapped_column(String(32), nullable=False)
    provider_name: Mapped[str | None] = mapped_column(String(64), nullable=True)
    provider_model: Mapped[str | None] = mapped_column(String(128), nullable=True)
    prompt_version: Mapped[str | None] = mapped_column(String(64), nullable=True)
    ai_usage_event_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("teacher_assist_ai_usage_events.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    score_suggestion: Mapped[float | None] = mapped_column(Float, nullable=True)
    max_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    feedback_summary: Mapped[str | None] = mapped_column(Text(), nullable=True)
    strengths: Mapped[list[str]] = mapped_column(JSON(), nullable=False, default=list)
    improvement_areas: Mapped[list[str]] = mapped_column(JSON(), nullable=False, default=list)
    teacher_notes: Mapped[str | None] = mapped_column(Text(), nullable=True)
    teacher_confirmed_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    teacher_confirmed_feedback: Mapped[str | None] = mapped_column(Text(), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    tenant: Mapped["Tenant"] = relationship("Tenant")
    teacher_user: Mapped["User"] = relationship("User")
    assignment: Mapped["TeacherAssistAssignment"] = relationship(
        "TeacherAssistAssignment", back_populates="grading_reviews"
    )
    student_work_submission: Mapped["TeacherAssistStudentWorkSubmission"] = relationship(
        "TeacherAssistStudentWorkSubmission",
        back_populates="grading_reviews",
    )
    school_year: Mapped["TeacherAssistSchoolYear"] = relationship("TeacherAssistSchoolYear")
    grading_period: Mapped["TeacherAssistGradingPeriod | None"] = relationship("TeacherAssistGradingPeriod")
    teacher_class: Mapped["TeacherAssistClass"] = relationship("TeacherAssistClass")
    subject: Mapped["TeacherAssistSubject"] = relationship("TeacherAssistSubject")
    ai_usage_event: Mapped["TeacherAssistAIUsageEvent | None"] = relationship("TeacherAssistAIUsageEvent")
    items: Mapped[list["TeacherAssistAssignmentGradingReviewItem"]] = relationship(
        "TeacherAssistAssignmentGradingReviewItem",
        back_populates="grading_review",
        cascade="all, delete-orphan",
        order_by="TeacherAssistAssignmentGradingReviewItem.sort_order.asc(), TeacherAssistAssignmentGradingReviewItem.created_at.asc()",
    )


from typing import TYPE_CHECKING  # noqa: E402

if TYPE_CHECKING:
    from oziebot_api.models.teacher_assist_ai_usage_event import TeacherAssistAIUsageEvent
    from oziebot_api.models.teacher_assist_assignment import TeacherAssistAssignment
    from oziebot_api.models.teacher_assist_assignment_grading_review_item import (
        TeacherAssistAssignmentGradingReviewItem,
    )
    from oziebot_api.models.teacher_assist_class import TeacherAssistClass
    from oziebot_api.models.teacher_assist_grading_period import TeacherAssistGradingPeriod
    from oziebot_api.models.teacher_assist_school_year import TeacherAssistSchoolYear
    from oziebot_api.models.teacher_assist_student_work_submission import (
        TeacherAssistStudentWorkSubmission,
    )
    from oziebot_api.models.teacher_assist_subject import TeacherAssistSubject
    from oziebot_api.models.tenant import Tenant
    from oziebot_api.models.user import User
