from __future__ import annotations

from datetime import date, datetime
from typing import Literal
import uuid

from pydantic import BaseModel, ConfigDict, Field

PacingGuideTypeLiteral = Literal["DISTRICT", "GRADE_LEVEL", "TEACHER"]
PacingGuidePeriodTypeLiteral = Literal["YEAR", "GRADING_PERIOD", "UNIT", "WEEK"]
PacingSchoolYearRoleLiteral = Literal["current", "next", "above_next"]


class PacingSchoolYearOptionOut(BaseModel):
    id: uuid.UUID
    title: str
    role: PacingSchoolYearRoleLiteral
    start_date: date
    end_date: date
    is_default: bool
    is_active: bool


class PacingSchoolYearOptionsOut(BaseModel):
    options: list[PacingSchoolYearOptionOut]
    default_school_year_id: uuid.UUID | None = None


class CatalogPacingGuideCreate(BaseModel):
    school_year_id: uuid.UUID
    guide_type: PacingGuideTypeLiteral
    title: str = Field(min_length=1, max_length=128)
    description: str | None = Field(default=None, max_length=4000)
    catalog_state_id: uuid.UUID | None = None
    catalog_district_id: uuid.UUID | None = None
    catalog_school_id: uuid.UUID | None = None
    catalog_grade_id: uuid.UUID | None = None
    catalog_subject_id: uuid.UUID | None = None
    is_template: bool = False
    is_shared: bool = False


class CatalogPacingGuideUpdate(CatalogPacingGuideCreate):
    is_active: bool = True


class CatalogPacingGuideSummaryOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    tenant_id: uuid.UUID
    school_year_id: uuid.UUID
    school_year_label: str | None = None
    guide_type: PacingGuideTypeLiteral
    title: str
    description: str | None = None
    catalog_state_id: uuid.UUID | None = None
    catalog_district_id: uuid.UUID | None = None
    catalog_school_id: uuid.UUID | None = None
    catalog_grade_id: uuid.UUID | None = None
    catalog_subject_id: uuid.UUID | None = None
    is_template: bool
    is_active: bool
    is_shared: bool
    created_by_user_id: uuid.UUID
    updated_by_user_id: uuid.UUID | None = None
    period_count: int = 0
    objective_count: int = 0
    resource_count: int = 0
    scope_label: str | None = None
    grade_name: str | None = None
    subject_name: str | None = None
    platform_school_year_id: uuid.UUID | None = None
    ownership_scope: Literal["district", "school"] | None = None
    unit_title: str | None = None
    estimated_duration_weeks: int | None = None
    start_week: int | None = None
    end_week: int | None = None
    created_at: datetime
    updated_at: datetime


class CatalogPacingGuideObjectiveOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    objective_id: uuid.UUID
    is_required: bool
    notes: str | None = None
    objective_code: str | None = None
    objective_description: str | None = None


class CatalogPacingGuideResourceOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    catalog_resource_id: uuid.UUID | None = None
    resource_library_item_id: uuid.UUID | None = None
    is_primary: bool
    notes: str | None = None
    resource_title: str | None = None
    resource_type: str | None = None


class CatalogPacingGuideDailyPlanOut(BaseModel):
    id: uuid.UUID | None = None
    day_label: str
    sequence_number: int | None = None
    daily_topic: str | None = None
    objective_focus: str | None = None
    teacher_notes: str | None = None
    materials_needed: str | None = None
    assessment_check: str | None = None


class CatalogPacingGuidePeriodOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    pacing_guide_id: uuid.UUID
    period_type: PacingGuidePeriodTypeLiteral
    title: str
    description: str | None = None
    sequence_number: int
    start_date: date | None = None
    end_date: date | None = None
    daily_plans: list[CatalogPacingGuideDailyPlanOut] = Field(default_factory=list)
    unit_title: str | None = None
    objectives: list[CatalogPacingGuideObjectiveOut] = Field(default_factory=list)
    resources: list[CatalogPacingGuideResourceOut] = Field(default_factory=list)
    created_at: datetime
    updated_at: datetime


class CatalogPacingGuideDetailOut(CatalogPacingGuideSummaryOut):
    periods: list[CatalogPacingGuidePeriodOut] = Field(default_factory=list)


class CatalogPacingGuidePeriodCreate(BaseModel):
    period_type: PacingGuidePeriodTypeLiteral
    title: str = Field(min_length=1, max_length=160)
    description: str | None = Field(default=None, max_length=4000)
    sequence_number: int | None = Field(default=None, ge=1)
    start_date: date | None = None
    end_date: date | None = None


class CatalogPacingGuidePeriodUpdate(CatalogPacingGuidePeriodCreate):
    sequence_number: int = Field(ge=1)


class CatalogPacingGuidePeriodReorderIn(BaseModel):
    ordered_period_ids: list[uuid.UUID] = Field(min_length=1)


class CatalogPacingGuideObjectiveCreate(BaseModel):
    objective_id: uuid.UUID
    is_required: bool = True
    notes: str | None = Field(default=None, max_length=4000)


class CatalogPacingGuideResourceCreate(BaseModel):
    catalog_resource_id: uuid.UUID | None = None
    resource_library_item_id: uuid.UUID | None = None
    is_primary: bool = False
    notes: str | None = Field(default=None, max_length=4000)


class CatalogPacingGuideCopyIn(BaseModel):
    target_guide_type: PacingGuideTypeLiteral
    title: str | None = Field(default=None, max_length=128)
    school_year_id: uuid.UUID | None = None


class CatalogPacingGuideRolloverIn(BaseModel):
    source_school_year_id: uuid.UUID
    target_school_year_id: uuid.UUID
    guide_ids: list[uuid.UUID] | None = None


class CatalogPacingGuidePeriodNoteUpdate(BaseModel):
    notes: str | None = Field(default=None, max_length=4000)


class CatalogPacingGuideActiveGuideUpdate(BaseModel):
    active_pacing_guide_id: uuid.UUID | None = None
    manual_pacing_period_id: uuid.UUID | None = None


class WeekArtifactGenerateIn(BaseModel):
    artifact_type: str = Field(min_length=1, max_length=64)
    class_id: uuid.UUID | None = None
