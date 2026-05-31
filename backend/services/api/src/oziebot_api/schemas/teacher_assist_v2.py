from __future__ import annotations

import uuid
from datetime import date, datetime, UTC

from pydantic import BaseModel, ConfigDict, Field

from oziebot_api.services.teacher_assist.education_catalog_constants import (
    CoverageTypeLiteral,
    ObjectiveTypeLiteral,
)


class EducationSchoolYearOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    state_id: uuid.UUID
    district_id: uuid.UUID | None = None
    school_id: uuid.UUID | None = None
    title: str
    start_date: date
    end_date: date
    active: bool
    created_at: datetime
    updated_at: datetime


class EducationSchoolYearCreate(BaseModel):
    state_id: uuid.UUID
    district_id: uuid.UUID | None = None
    school_id: uuid.UUID | None = None
    title: str = Field(min_length=1, max_length=64)
    start_date: date
    end_date: date
    active: bool = False


class EducationObjectiveV2Out(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    state_id: uuid.UUID
    district_id: uuid.UUID | None = None
    school_id: uuid.UUID | None = None
    grade_id: uuid.UUID | None = None
    subject_id: uuid.UUID | None = None
    school_year_id: uuid.UUID | None = None
    grade_level: str
    subject_code: str
    objective_type: ObjectiveTypeLiteral
    objective_id: str
    description: str
    coverage_type: CoverageTypeLiteral
    active: bool
    created_at: datetime
    updated_at: datetime


class EducationObjectiveV2Create(BaseModel):
    state_id: uuid.UUID
    district_id: uuid.UUID | None = None
    school_id: uuid.UUID | None = None
    grade_id: uuid.UUID
    subject_id: uuid.UUID
    school_year_id: uuid.UUID
    objective_type: ObjectiveTypeLiteral
    objective_id: str = Field(min_length=1, max_length=64)
    description: str = Field(min_length=1)
    is_required: bool = True
    active: bool = True


class V2PacingGuideCopyIn(BaseModel):
    title: str | None = Field(default=None, max_length=128)
    school_year_id: uuid.UUID | None = None


class V2PacingGuidePeriodUpdate(BaseModel):
    title: str = Field(min_length=1, max_length=160)
    description: str | None = Field(default=None, max_length=4000)


class V2TeacherOnboardingSaveIn(BaseModel):
    school_year_id: uuid.UUID
    grade_id: uuid.UUID
    student_count: int = Field(ge=1, le=100)
    selected_subject_ids: list[uuid.UUID] = Field(min_length=1)


class V2PacingGuideSelectionIn(BaseModel):
    subject_id: uuid.UUID
    source_guide_id: uuid.UUID
    mode: str = Field(default="district", pattern="^(district|teacher_copy)$")


class V2PacingGuideSetupSaveIn(BaseModel):
    selections: list[V2PacingGuideSelectionIn] = Field(min_length=1)


class V2TeacherProvisionIn(BaseModel):
    email: str | None = None
    full_name: str | None = None
    user_id: uuid.UUID | None = None
    state_id: uuid.UUID
    district_id: uuid.UUID
    school_id: uuid.UUID
    catalog_grade_id: uuid.UUID | None = None
    tenant_name: str | None = None


class V2SupportingMaterialOut(BaseModel):
    id: uuid.UUID
    pacing_guide_id: uuid.UUID
    period_id: uuid.UUID | None = None
    education_objective_id: uuid.UUID | None = None
    material_kind: str
    resource_type: str
    title: str
    description: str | None = None
    note_body: str | None = None
    external_url: str | None = None
    original_filename: str | None = None
    mime_type: str | None = None
    file_size: int | None = None
    storage_key: str | None = None
    download_url: str | None = None
    visibility_scope: str
    uploaded_by_user_id: uuid.UUID
    active: bool
    created_at: datetime
    updated_at: datetime


class V2SupportingLinkCreate(BaseModel):
    title: str = Field(min_length=1, max_length=256)
    external_url: str = Field(min_length=1, max_length=2048)
    resource_type: str = Field(min_length=1, max_length=64)
    description: str | None = Field(default=None, max_length=4000)
    period_id: uuid.UUID | None = None
    education_objective_id: uuid.UUID | None = None


class V2SupportingNoteCreate(BaseModel):
    note_body: str = Field(min_length=1)
    title: str | None = Field(default=None, max_length=256)
    period_id: uuid.UUID | None = None
    education_objective_id: uuid.UUID | None = None


class V2PlanningGenerateIn(BaseModel):
    week_start: int = Field(ge=1)
    week_end: int = Field(ge=1)
    teaching_order: list[uuid.UUID] = Field(min_length=1)
    selected_outputs: list[str] = Field(min_length=1)
    plan_start_date: date | None = None
    plan_end_date: date | None = None


class V2PackageCloseOutIn(BaseModel):
    close_out_notes: str | None = None
    completed_date: date | None = None


class V2PlanningSupplementalLinkCreate(BaseModel):
    week_start: int = Field(ge=1)
    week_end: int = Field(ge=1)
    title: str = Field(min_length=1, max_length=256)
    external_url: str = Field(min_length=1, max_length=2048)
    resource_type: str = Field(default="reference_link", max_length=64)
    description: str | None = Field(default=None, max_length=4000)


class V2PlanningSupplementalNoteCreate(BaseModel):
    week_start: int = Field(ge=1)
    week_end: int = Field(ge=1)
    note_body: str = Field(min_length=1)
    title: str | None = Field(default=None, max_length=256)


class V2StudentSubmissionManualMatchIn(BaseModel):
    student_number: int = Field(ge=1, le=100)


class V2StudentSubmissionStatusIn(BaseModel):
    status: str = Field(min_length=1, max_length=32)
