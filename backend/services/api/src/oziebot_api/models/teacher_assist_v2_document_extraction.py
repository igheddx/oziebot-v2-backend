from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, Integer, String, Text, Uuid
from sqlalchemy.orm import Mapped, mapped_column

from oziebot_api.db.base import Base


class TeacherAssistV2DocumentExtraction(Base):
    __tablename__ = "teacher_assist_v2_document_extractions"

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False, index=True
    )
    teacher_user_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True
    )
    source_type: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    supporting_material_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("pacing_guide_supporting_materials.id", ondelete="CASCADE"),
        nullable=True,
        unique=True,
        index=True,
    )
    planning_supplemental_material_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("teacher_assist_v2_planning_supplemental_materials.id", ondelete="CASCADE"),
        nullable=True,
        unique=True,
        index=True,
    )
    student_submission_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("teacher_assist_v2_student_submissions.id", ondelete="CASCADE"),
        nullable=True,
        unique=True,
        index=True,
    )
    pacing_guide_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("pacing_guides.id", ondelete="SET NULL"), nullable=True, index=True
    )
    package_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("teacher_assist_v2_instructional_packages.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    assignment_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("teacher_assist_v2_assignments.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    platform_school_year_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("education_school_years.id", ondelete="SET NULL"), nullable=True
    )
    catalog_state_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("education_states.id", ondelete="SET NULL"), nullable=True
    )
    catalog_district_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("education_districts.id", ondelete="SET NULL"), nullable=True
    )
    catalog_school_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("education_schools.id", ondelete="SET NULL"), nullable=True
    )
    catalog_grade_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("education_grades.id", ondelete="SET NULL"), nullable=True
    )
    catalog_subject_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("education_subjects.id", ondelete="SET NULL"), nullable=True
    )
    week_start: Mapped[int | None] = mapped_column(Integer, nullable=True)
    week_end: Mapped[int | None] = mapped_column(Integer, nullable=True)
    student_number: Mapped[int | None] = mapped_column(Integer, nullable=True)
    storage_key: Mapped[str] = mapped_column(String(512), nullable=False)
    original_filename: Mapped[str] = mapped_column(String(255), nullable=False)
    mime_type: Mapped[str] = mapped_column(String(255), nullable=False)
    file_size: Mapped[int] = mapped_column(Integer, nullable=False)
    extraction_status: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    extracted_text: Mapped[str | None] = mapped_column(Text(), nullable=True)
    extracted_text_preview: Mapped[str | None] = mapped_column(Text(), nullable=True)
    character_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    token_estimate: Mapped[int | None] = mapped_column(Integer, nullable=True)
    provider: Mapped[str | None] = mapped_column(String(128), nullable=True)
    teacher_edited_text: Mapped[str | None] = mapped_column(Text(), nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text(), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
