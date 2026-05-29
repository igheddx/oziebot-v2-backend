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
    active: bool
    created_at: datetime
    updated_at: datetime


class EducationDistrictCreate(BaseModel):
    state_id: uuid.UUID
    name: str = Field(min_length=1, max_length=256)
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
