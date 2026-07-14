from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import JSON, Boolean, DateTime, Float, ForeignKey, Integer, String, Text, Uuid
from sqlalchemy.orm import Mapped, mapped_column, relationship

from oziebot_api.db.base import Base


class TeacherAssistExtractedTextRecord(Base):
    __tablename__ = "teacher_assist_extracted_text_records"

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
    extraction_job_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("teacher_assist_extraction_jobs.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    artifact_type: Mapped[str] = mapped_column(Text(), nullable=False, index=True)
    resource_library_item_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("resource_library_items.id", ondelete="CASCADE"),
        nullable=True,
        index=True,
    )
    student_work_submission_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("assignment_student_work_submissions.id", ondelete="CASCADE"),
        nullable=True,
        index=True,
    )
    assignment_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("assignments.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    school_year_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("school_years.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    grading_period_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("grading_periods.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    class_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("classes.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    subject_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("subjects.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    student_number: Mapped[int | None] = mapped_column(Integer, nullable=True)
    extracted_text: Mapped[str] = mapped_column(Text(), nullable=False)
    preview_text: Mapped[str] = mapped_column(Text(), nullable=False)
    text_char_count: Mapped[int] = mapped_column(Integer, nullable=False)
    pii_flagged: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default="0"
    )
    redaction_applied: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default="0"
    )
    review_status: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        default="pending_review",
        server_default="pending_review",
        index=True,
    )
    provider_confidence_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    confidence_level: Mapped[str] = mapped_column(
        String(16), nullable=False, default="unknown", server_default="unknown", index=True
    )
    teacher_corrected_text: Mapped[str | None] = mapped_column(Text(), nullable=True)
    approved_text: Mapped[str | None] = mapped_column(Text(), nullable=True)
    reviewed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    reviewed_by_user_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("users.id", ondelete="SET NULL"),
        nullable=True,
    )
    source_extraction_job_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("teacher_assist_extraction_jobs.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    metadata_json: Mapped[dict[str, Any] | None] = mapped_column(JSON(), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    tenant: Mapped["Tenant"] = relationship("Tenant")
    teacher_user: Mapped["User"] = relationship("User", foreign_keys=[teacher_user_id])
    reviewed_by_user: Mapped["User | None"] = relationship(
        "User", foreign_keys=[reviewed_by_user_id]
    )
    extraction_job: Mapped["TeacherAssistExtractionJob"] = relationship(
        "TeacherAssistExtractionJob",
        foreign_keys=[extraction_job_id],
        back_populates="extracted_text_records",
    )
    resource_library_item: Mapped["TeacherAssistResourceLibraryItem | None"] = relationship(
        "TeacherAssistResourceLibraryItem",
        back_populates="extracted_text_records",
    )
    student_work_submission: Mapped["TeacherAssistStudentWorkSubmission | None"] = relationship(
        "TeacherAssistStudentWorkSubmission",
        back_populates="extracted_text_records",
    )


from typing import TYPE_CHECKING  # noqa: E402

if TYPE_CHECKING:
    from oziebot_api.models.teacher_assist_extraction_job import TeacherAssistExtractionJob
    from oziebot_api.models.teacher_assist_resource_library_item import (
        TeacherAssistResourceLibraryItem,
    )
    from oziebot_api.models.teacher_assist_student_work_submission import (
        TeacherAssistStudentWorkSubmission,
    )
    from oziebot_api.models.tenant import Tenant
    from oziebot_api.models.user import User
