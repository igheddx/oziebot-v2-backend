from __future__ import annotations

import uuid
from datetime import datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy import DateTime, Float, ForeignKey, JSON, String, Text, Uuid
from sqlalchemy.orm import Mapped, mapped_column, relationship

from oziebot_api.db.base import Base


class TeacherAssistV2GradingDraft(Base):
    __tablename__ = "teacher_assist_v2_grading_drafts"

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
    grading_job_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("teacher_assist_v2_grading_jobs.id", ondelete="CASCADE"),
        nullable=False,
        unique=True,
    )
    score: Mapped[float] = mapped_column(Float(), nullable=False)
    max_score: Mapped[float] = mapped_column(Float(), nullable=False)
    percentage: Mapped[float] = mapped_column(Float(), nullable=False)
    rubric_json: Mapped[dict[str, Any]] = mapped_column(JSON(), nullable=False)
    teacher_comment_draft: Mapped[str] = mapped_column(Text(), nullable=False)
    strengths: Mapped[list[Any]] = mapped_column(JSON(), nullable=False)
    improvements: Mapped[list[Any]] = mapped_column(JSON(), nullable=False)
    objective_evidence: Mapped[list[Any]] = mapped_column(JSON(), nullable=False)
    confidence_score: Mapped[float] = mapped_column(Float(), nullable=False)
    provider: Mapped[str] = mapped_column(String(64), nullable=False)
    model: Mapped[str | None] = mapped_column(String(128), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    grading_job: Mapped["TeacherAssistV2GradingJob"] = relationship(
        "TeacherAssistV2GradingJob",
        back_populates="draft",
    )


if TYPE_CHECKING:
    from oziebot_api.models.teacher_assist_v2_grading_job import TeacherAssistV2GradingJob
