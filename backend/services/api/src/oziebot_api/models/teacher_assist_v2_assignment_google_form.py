from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import DateTime, ForeignKey, Integer, JSON, String, Uuid
from sqlalchemy.orm import Mapped, mapped_column

from oziebot_api.db.base import Base


class TeacherAssistV2AssignmentGoogleForm(Base):
    __tablename__ = "teacher_assist_v2_assignment_google_forms"

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
        unique=True,
    )
    artifact_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("teacher_assist_v2_instructional_package_artifacts.id", ondelete="CASCADE"),
        nullable=False,
    )
    instructional_package_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("teacher_assist_v2_instructional_packages.id", ondelete="CASCADE"),
        nullable=False,
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
    education_objective_ids_json: Mapped[list[Any]] = mapped_column(JSON(), nullable=False)
    google_form_id: Mapped[str] = mapped_column(String(128), nullable=False, unique=True)
    google_form_url: Mapped[str] = mapped_column(String(1024), nullable=False)
    google_edit_url: Mapped[str] = mapped_column(String(1024), nullable=False)
    google_response_url: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    google_created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    google_created_by_user_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    google_sync_status: Mapped[str] = mapped_column(String(32), nullable=False, default="CREATED")
    question_mapping_json: Mapped[dict[str, Any]] = mapped_column(JSON(), nullable=False)
    last_import_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_import_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
