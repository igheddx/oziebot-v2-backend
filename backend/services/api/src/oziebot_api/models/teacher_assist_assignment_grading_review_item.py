from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import JSON, DateTime, Float, ForeignKey, Integer, String, Text, Uuid
from sqlalchemy.orm import Mapped, mapped_column, relationship

from oziebot_api.db.base import Base


class TeacherAssistAssignmentGradingReviewItem(Base):
    __tablename__ = "assignment_grading_review_items"

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    grading_review_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("assignment_grading_reviews.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    criterion_title: Mapped[str] = mapped_column(String(160), nullable=False)
    score_suggestion: Mapped[float | None] = mapped_column(Float, nullable=True)
    max_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    feedback_summary: Mapped[str | None] = mapped_column(Text(), nullable=True)
    strengths: Mapped[list[str]] = mapped_column(JSON(), nullable=False, default=list)
    improvement_areas: Mapped[list[str]] = mapped_column(JSON(), nullable=False, default=list)
    teacher_notes: Mapped[str | None] = mapped_column(Text(), nullable=True)
    sort_order: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    grading_review: Mapped["TeacherAssistAssignmentGradingReview"] = relationship(
        "TeacherAssistAssignmentGradingReview",
        back_populates="items",
    )


from typing import TYPE_CHECKING  # noqa: E402

if TYPE_CHECKING:
    from oziebot_api.models.teacher_assist_assignment_grading_review import (
        TeacherAssistAssignmentGradingReview,
    )
