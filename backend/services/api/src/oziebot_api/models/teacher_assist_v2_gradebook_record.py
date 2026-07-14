from __future__ import annotations

import uuid
from datetime import datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy import DateTime, Float, ForeignKey, Integer, JSON, String, Text, Uuid
from sqlalchemy.orm import Mapped, mapped_column, relationship

from oziebot_api.db.base import Base


class TeacherAssistV2GradebookRecord(Base):
    __tablename__ = "teacher_assist_v2_gradebook_records"

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False, index=True
    )
    teacher_user_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True
    )
    platform_school_year_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("education_school_years.id", ondelete="CASCADE"),
        nullable=False,
    )
    catalog_district_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("education_districts.id", ondelete="CASCADE"), nullable=False
    )
    catalog_school_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("education_schools.id", ondelete="SET NULL"), nullable=True
    )
    catalog_grade_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("education_grades.id", ondelete="CASCADE"), nullable=False
    )
    catalog_subject_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("education_subjects.id", ondelete="CASCADE"), nullable=False
    )
    pacing_guide_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("pacing_guides.id", ondelete="CASCADE"), nullable=False
    )
    instructional_package_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("teacher_assist_v2_instructional_packages.id", ondelete="CASCADE"),
        nullable=False,
    )
    assignment_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("teacher_assist_v2_assignments.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    student_number: Mapped[int] = mapped_column(Integer, nullable=False)
    education_objective_ids_json: Mapped[list[Any]] = mapped_column(JSON(), nullable=False)
    assignment_grade_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("teacher_assist_v2_assignment_grades.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    score: Mapped[float] = mapped_column(Float(), nullable=False)
    max_score: Mapped[float] = mapped_column(Float(), nullable=False)
    percentage: Mapped[float] = mapped_column(Float(), nullable=False)
    mastery_level: Mapped[str] = mapped_column(String(32), nullable=False)
    teacher_comment: Mapped[str] = mapped_column(Text(), nullable=False)
    confirmed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    sync_status: Mapped[str] = mapped_column(
        String(32), nullable=False, default="SYNCED", server_default="SYNCED"
    )
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    revisions: Mapped[list["TeacherAssistV2GradebookRecordRevision"]] = relationship(
        "TeacherAssistV2GradebookRecordRevision",
        back_populates="gradebook_record",
    )
    mastery_evidence: Mapped[list["TeacherAssistV2MasteryEvidence"]] = relationship(
        "TeacherAssistV2MasteryEvidence",
        back_populates="gradebook_record",
    )


if TYPE_CHECKING:
    from oziebot_api.models.teacher_assist_v2_gradebook_record_revision import (
        TeacherAssistV2GradebookRecordRevision,
    )
    from oziebot_api.models.teacher_assist_v2_mastery_evidence import TeacherAssistV2MasteryEvidence
