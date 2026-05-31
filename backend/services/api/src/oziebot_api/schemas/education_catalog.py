from __future__ import annotations

from datetime import datetime
import uuid

from pydantic import BaseModel, ConfigDict, Field

from oziebot_api.services.teacher_assist.education_catalog_constants import (
    CatalogResourceTypeLiteral,
    CoverageTypeLiteral,
    ObjectiveTypeLiteral,
)


class EducationStateOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    name: str
    abbreviation: str
    active: bool
    created_at: datetime
    updated_at: datetime


class EducationStateCreate(BaseModel):
    name: str = Field(min_length=1, max_length=128)
    abbreviation: str = Field(min_length=1, max_length=16)
    active: bool = True


class EducationDistrictOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    state_id: uuid.UUID
    name: str
    district_code: str | None = None
    active: bool
    created_at: datetime
    updated_at: datetime


class EducationDistrictCreate(BaseModel):
    state_id: uuid.UUID
    name: str = Field(min_length=1, max_length=256)
    district_code: str | None = Field(default=None, max_length=32)
    active: bool = True


class EducationSchoolOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    district_id: uuid.UUID
    name: str
    school_type: str | None = None
    active: bool
    created_at: datetime
    updated_at: datetime


class EducationSchoolCreate(BaseModel):
    district_id: uuid.UUID
    name: str = Field(min_length=1, max_length=256)
    school_type: str | None = None
    active: bool = True


class EducationGradeOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    school_id: uuid.UUID | None = None
    grade_code: str
    display_name: str
    active: bool


class EducationGradeCreate(BaseModel):
    school_id: uuid.UUID | None = None
    grade_code: str = Field(min_length=1, max_length=16)
    display_name: str = Field(min_length=1, max_length=64)
    active: bool = True


class EducationSubjectOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    grade_id: uuid.UUID | None = None
    subject_code: str
    display_name: str
    active: bool


class EducationSubjectCreate(BaseModel):
    grade_id: uuid.UUID | None = None
    subject_code: str = Field(min_length=1, max_length=64)
    display_name: str = Field(min_length=1, max_length=128)
    active: bool = True


class EducationObjectiveOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    state_id: uuid.UUID
    grade_level: str
    subject_code: str
    objective_type: ObjectiveTypeLiteral
    objective_id: str
    description: str
    coverage_type: CoverageTypeLiteral
    active: bool
    created_at: datetime
    updated_at: datetime


class EducationObjectiveCreate(BaseModel):
    state_id: uuid.UUID
    grade_level: str = Field(min_length=1, max_length=16)
    subject_code: str = Field(min_length=1, max_length=64)
    objective_type: ObjectiveTypeLiteral
    objective_id: str = Field(min_length=1, max_length=64)
    description: str = Field(min_length=1)
    coverage_type: CoverageTypeLiteral
    active: bool = True


class EducationCurriculumResourceOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    state_id: uuid.UUID | None = None
    district_id: uuid.UUID | None = None
    school_id: uuid.UUID | None = None
    grade_level: str
    subject_code: str
    resource_type: CatalogResourceTypeLiteral
    title: str
    description: str | None = None
    storage_key: str | None = None
    active: bool
    created_at: datetime
    updated_at: datetime


class EducationCurriculumResourceCreate(BaseModel):
    state_id: uuid.UUID | None = None
    district_id: uuid.UUID | None = None
    school_id: uuid.UUID | None = None
    grade_level: str = Field(min_length=1, max_length=16)
    subject_code: str = Field(min_length=1, max_length=64)
    resource_type: CatalogResourceTypeLiteral
    title: str = Field(min_length=1, max_length=256)
    description: str | None = None
    storage_key: str | None = None
    active: bool = True


class EducationResourceLinkOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    curriculum_resource_id: uuid.UUID
    link_title: str
    url: str
    active: bool
    created_at: datetime
    updated_at: datetime


class EducationResourceLinkCreate(BaseModel):
    curriculum_resource_id: uuid.UUID
    link_title: str = Field(min_length=1, max_length=256)
    url: str = Field(min_length=1, max_length=2048)
    active: bool = True


class TeacherSchoolAssignmentOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    user_id: uuid.UUID
    state_id: uuid.UUID
    district_id: uuid.UUID
    school_id: uuid.UUID
    active: bool
    created_at: datetime
    updated_at: datetime


class TeacherSchoolAssignmentCreate(BaseModel):
    user_id: uuid.UUID
    state_id: uuid.UUID
    district_id: uuid.UUID
    school_id: uuid.UUID
    active: bool = True


class AvailableTeacherOut(BaseModel):
    user_id: uuid.UUID
    email: str
    full_name: str | None = None


class TeacherSchoolAssignmentListOut(TeacherSchoolAssignmentOut):
    user_email: str | None = None
    user_full_name: str | None = None
    state_name: str | None = None
    district_name: str | None = None
    school_name: str | None = None


class TeacherSchoolAssignmentProvision(BaseModel):
    state_id: uuid.UUID
    district_id: uuid.UUID
    school_id: uuid.UUID
    active: bool = True
    user_id: uuid.UUID | None = None
    email: str | None = Field(default=None, max_length=320)
    full_name: str | None = Field(default=None, max_length=255)
    tenant_name: str | None = Field(default=None, max_length=255)
    catalog_grade_id: uuid.UUID | None = None


class TeacherSchoolAssignmentProvisionOut(BaseModel):
    assignment: TeacherSchoolAssignmentOut
    user_id: uuid.UUID
    email: str
    full_name: str | None = None
    created_user: bool
    temporary_password: str | None = None
    grade_setup_applied: bool = False


class TeacherMySchoolSetupOut(BaseModel):
    assignment: dict | None = None
    catalog_grade_id: str | None = None
    catalog_grade_code: str | None = None
    selected_catalog_subject_ids: list[str] = Field(default_factory=list)
    synced_subjects: list[dict[str, str]] = Field(default_factory=list)


class TeacherMySchoolSetupUpdate(BaseModel):
    state_id: uuid.UUID
    district_id: uuid.UUID
    school_id: uuid.UUID
    catalog_grade_id: uuid.UUID
    catalog_subject_ids: list[uuid.UUID] = Field(min_length=1)


class TeacherMyClassroomOut(BaseModel):
    grade_level: str | None = None
    grade_display_name: str | None = None
    homeroom_name: str
    student_count: int | None = None
    timezone: str | None = None
    class_id: str | None = None
    synced_subjects: list[dict[str, str]] = Field(default_factory=list)
    has_active_school_year: bool = False
    requires_school_setup: bool = True
    active_school_year_id: str | None = None
    active_school_year_title: str | None = None
    active_school_year_start_date: str | None = None
    active_school_year_end_date: str | None = None


class TeacherMyClassroomUpdate(BaseModel):
    homeroom_name: str = Field(min_length=1, max_length=128)
    student_count: int = Field(ge=1)
    timezone: str | None = Field(default=None, max_length=128)


class CatalogImportPreviewIn(BaseModel):
    csv_content: str = Field(min_length=1)


class CatalogImportRowErrorOut(BaseModel):
    row_number: int
    message: str
    field: str | None = None


class CatalogImportPreviewOut(BaseModel):
    total_rows: int
    valid_count: int
    invalid_count: int
    duplicate_count: int
    errors: list[CatalogImportRowErrorOut] = Field(default_factory=list)


class CatalogObjectiveImportCommitRowIn(BaseModel):
    state_abbreviation: str
    grade_level: str
    subject_code: str
    objective_type: ObjectiveTypeLiteral
    objective_id: str
    description: str
    coverage_type: CoverageTypeLiteral


class CatalogObjectiveImportCommitIn(BaseModel):
    rows: list[CatalogObjectiveImportCommitRowIn] = Field(min_length=1)


class CatalogImportCommitOut(BaseModel):
    created_count: int
    skipped_duplicate_count: int
    errors: list[CatalogImportRowErrorOut] = Field(default_factory=list)


class TeacherCatalogContextOut(BaseModel):
    assignment: dict | None = None
    grades: list[dict] = Field(default_factory=list)
    subjects: list[dict] = Field(default_factory=list)
    objectives: list[dict] = Field(default_factory=list)
    resources: list[dict] = Field(default_factory=list)
