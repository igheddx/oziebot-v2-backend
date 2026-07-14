from __future__ import annotations

import uuid
from datetime import date, datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy import Date, DateTime, ForeignKey, Integer, JSON, String, Text, Uuid
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from oziebot_api.db.base import Base

if TYPE_CHECKING:
    from oziebot_api.models.teacher_assist_v2_assignment import TeacherAssistV2Assignment
    from oziebot_api.models.teacher_assist_v2_slide_visual_asset import TeacherAssistV2SlideVisualAsset


class TeacherAssistV2InstructionalPackage(Base):
    __tablename__ = "teacher_assist_v2_instructional_packages"

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False, index=True
    )
    teacher_user_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True
    )
    platform_school_year_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("education_school_years.id", ondelete="CASCADE"), nullable=False
    )
    catalog_state_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("education_states.id", ondelete="CASCADE"), nullable=False
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
    subject_ids_json: Mapped[list[Any]] = mapped_column(JSON(), nullable=False)
    pacing_guide_ids_json: Mapped[list[Any]] = mapped_column(JSON(), nullable=False)
    primary_pacing_guide_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("pacing_guides.id", ondelete="SET NULL"), nullable=True
    )
    title: Mapped[str] = mapped_column(String(256), nullable=False)
    week_start: Mapped[int] = mapped_column(Integer, nullable=False)
    week_end: Mapped[int] = mapped_column(Integer, nullable=False)
    plan_start_date: Mapped[date] = mapped_column(Date(), nullable=False)
    plan_end_date: Mapped[date] = mapped_column(Date(), nullable=False)
    teaching_order_json: Mapped[list[Any]] = mapped_column(JSON(), nullable=False)
    selected_outputs_json: Mapped[list[Any]] = mapped_column(JSON(), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="generated", server_default="generated")
    provider_name: Mapped[str | None] = mapped_column(String(64), nullable=True)
    metadata_json: Mapped[dict[str, Any] | None] = mapped_column(JSON(), nullable=True)
    # Backward-designed instructional plan generated once before artifact generation.
    # Stores unit_mastery_arc, knowledge_dependency_graph, district_anchors, and
    # AI-generated instructional_design per week × subject. JSONB for queryability.
    instructional_design_plan_json: Mapped[dict[str, Any] | None] = mapped_column(
        JSONB(), nullable=True
    )
    # Set after Phase 0d (validation + revision) completes. The plan is final and
    # immutable from this point; artifact generation uses the locked, validated plan.
    instructional_design_plan_locked_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    # Validation report produced by Phase 0d. Stores per-subject scores, issue lists
    # (separated into educational_issues and generation_issues), revision history, and
    # the overall confidence_label (Excellent/Very Good/Needs Review). JSONB for
    # future admin analytics queries.
    instructional_validation_report_json: Mapped[dict[str, Any] | None] = mapped_column(
        JSONB(), nullable=True
    )
    # Structured index of every curriculum resource extracted from attached PDFs, pacing
    # guide structured fields, and Phase 0a AI-confirmed mentor texts. Persisted here so
    # single-artifact regeneration can reference the same bank without re-running Phase 0a.
    # Schema: {"version": 2, "entries": [...], "extraction_summary": {...}}
    instructional_resource_bank_json: Mapped[dict[str, Any] | None] = mapped_column(
        JSONB(), nullable=True
    )
    # Classroom delivery constraint — controls how instructional strands are distributed
    # across available time. Separates "how instruction is delivered" from "what students
    # learn". Null → AI Optimized (AI decides distribution; existing behavior preserved).
    # Set at package creation; consumed by Phase 0c and all artifact generators.
    # Structure: {"mode": str, "strands": [{"strand_name", "minutes_per_day", "days"}]}
    instructional_delivery_profile: Mapped[dict[str, Any] | None] = mapped_column(
        JSONB(), nullable=True
    )
    # Cross-artifact alignment report produced by Phase 3b (per week, after all artifacts
    # for that week are committed). Accumulates week-by-week: {"weeks": [{...}, {...}]}.
    # Each week entry contains five deterministic checks: exit_ticket_stem_compliance,
    # rubric_criterion_alignment, quiz_objective_coverage, vocabulary_sequence, and
    # student_prohibition_scan. Each check includes an alignment_explanation.
    instructional_alignment_report_json: Mapped[dict[str, Any] | None] = mapped_column(
        JSONB(), nullable=True
    )
    # Today's Teaching Brief assembled deterministically (Phase 5, zero AI calls) from
    # the plan, validation report, alignment report, and generated artifacts. Stored
    # once at the end of package generation. Structure: {"generated_at", "days": [...]}.
    teacher_coaching_summary_json: Mapped[dict[str, Any] | None] = mapped_column(
        JSONB(), nullable=True
    )
    closed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    closed_by_user_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("users.id", ondelete="SET NULL"), nullable=True
    )
    close_out_notes: Mapped[str | None] = mapped_column(Text(), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    artifacts: Mapped[list["TeacherAssistV2InstructionalPackageArtifact"]] = relationship(
        "TeacherAssistV2InstructionalPackageArtifact",
        back_populates="package",
        cascade="all, delete-orphan",
    )


class TeacherAssistV2InstructionalPackageArtifact(Base):
    __tablename__ = "teacher_assist_v2_instructional_package_artifacts"

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False, index=True
    )
    package_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("teacher_assist_v2_instructional_packages.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    artifact_type: Mapped[str] = mapped_column(String(64), nullable=False)
    subject_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("education_subjects.id", ondelete="SET NULL"), nullable=True
    )
    period_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("pacing_guide_periods.id", ondelete="SET NULL"), nullable=True
    )
    day_label: Mapped[str | None] = mapped_column(String(32), nullable=True)
    sequence_number: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    title: Mapped[str] = mapped_column(String(256), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="ready", server_default="ready")
    content_json: Mapped[dict[str, Any]] = mapped_column(JSON(), nullable=False)
    preview_html: Mapped[str | None] = mapped_column(Text(), nullable=True)
    storage_key: Mapped[str | None] = mapped_column(String(512), nullable=True)
    export_format: Mapped[str | None] = mapped_column(String(32), nullable=True)
    metadata_json: Mapped[dict[str, Any] | None] = mapped_column(JSON(), nullable=True)
    assignment_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("teacher_assist_v2_assignments.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    package: Mapped["TeacherAssistV2InstructionalPackage"] = relationship(
        "TeacherAssistV2InstructionalPackage", back_populates="artifacts"
    )
    assignment: Mapped["TeacherAssistV2Assignment | None"] = relationship(
        "TeacherAssistV2Assignment", back_populates="artifacts"
    )
    slide_visual_assets: Mapped[list["TeacherAssistV2SlideVisualAsset"]] = relationship(
        "TeacherAssistV2SlideVisualAsset",
        back_populates="artifact",
        cascade="all, delete-orphan",
    )


class TeacherAssistV2PlanningSupplementalMaterial(Base):
    __tablename__ = "teacher_assist_v2_planning_supplemental_materials"

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False, index=True
    )
    teacher_user_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True
    )
    platform_school_year_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("education_school_years.id", ondelete="CASCADE"), nullable=False
    )
    catalog_state_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("education_states.id", ondelete="CASCADE"), nullable=False
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
    week_start: Mapped[int] = mapped_column(Integer, nullable=False)
    week_end: Mapped[int] = mapped_column(Integer, nullable=False)
    package_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("teacher_assist_v2_instructional_packages.id", ondelete="SET NULL"),
        nullable=True,
    )
    material_kind: Mapped[str] = mapped_column(String(16), nullable=False)
    resource_type: Mapped[str] = mapped_column(String(64), nullable=False)
    title: Mapped[str] = mapped_column(String(256), nullable=False)
    description: Mapped[str | None] = mapped_column(Text(), nullable=True)
    note_body: Mapped[str | None] = mapped_column(Text(), nullable=True)
    external_url: Mapped[str | None] = mapped_column(String(2048), nullable=True)
    storage_key: Mapped[str | None] = mapped_column(String(512), nullable=True)
    original_filename: Mapped[str | None] = mapped_column(String(512), nullable=True)
    mime_type: Mapped[str | None] = mapped_column(String(128), nullable=True)
    file_size: Mapped[int | None] = mapped_column(Integer, nullable=True)
    uploaded_by_user_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    active: Mapped[bool] = mapped_column(nullable=False, default=True, server_default="true")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
