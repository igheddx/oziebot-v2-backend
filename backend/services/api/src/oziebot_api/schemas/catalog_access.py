from __future__ import annotations

import uuid

from pydantic import BaseModel, ConfigDict, Field


class CatalogPageMetaOut(BaseModel):
    page: int
    page_size: int
    total: int
    total_pages: int


class CatalogScopeFiltersOut(BaseModel):
    state_id: str | None = None
    district_id: str | None = None
    school_id: str | None = None


class CatalogScopeLabelsOut(BaseModel):
    state_name: str | None = None
    district_name: str | None = None
    school_name: str | None = None


class CatalogAssignmentOut(BaseModel):
    id: str | None = None
    state: dict
    district: dict
    school: dict


class CatalogBrowseContextOut(BaseModel):
    assignment: CatalogAssignmentOut | None = None
    missing_assignment: bool = False
    multiple_assignments_detected: bool = False
    can_browse: bool
    is_root_unscoped: bool = False
    scope_filters: CatalogScopeFiltersOut
    scope_labels: CatalogScopeLabelsOut = Field(default_factory=CatalogScopeLabelsOut)
    scope_banner: str | None = None


class CatalogLinkedResourceItemOut(BaseModel):
    id: uuid.UUID
    title: str
    resource_type: str
    reference_links: list["CatalogResourceLinkItemOut"] = Field(default_factory=list)


class CatalogGradeItemOut(BaseModel):
    id: uuid.UUID
    grade_code: str
    display_name: str
    active: bool
    subject_count: int = 0


class CatalogGradesPageOut(BaseModel):
    items: list[CatalogGradeItemOut]
    meta: CatalogPageMetaOut


class CatalogSubjectItemOut(BaseModel):
    id: uuid.UUID
    grade_id: str | None = None
    grade_code: str | None = None
    subject_code: str
    display_name: str
    active: bool
    objective_count: int = 0
    resource_count: int = 0


class CatalogSubjectsPageOut(BaseModel):
    items: list[CatalogSubjectItemOut]
    meta: CatalogPageMetaOut


class CatalogObjectiveItemOut(BaseModel):
    id: uuid.UUID
    objective_id: str
    objective_type: str
    description: str
    coverage_type: str
    grade_level: str
    subject_code: str
    active: bool
    linked_resources: list[CatalogLinkedResourceItemOut] = Field(default_factory=list)


class CatalogObjectivesPageOut(BaseModel):
    items: list[CatalogObjectiveItemOut]
    meta: CatalogPageMetaOut


class CatalogResourceLinkItemOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    link_title: str
    url: str
    active: bool


class CatalogResourceObjectiveItemOut(BaseModel):
    id: uuid.UUID
    objective_id: str
    objective_type: str
    coverage_type: str
    grade_level: str
    subject_code: str


class CatalogResourceItemOut(BaseModel):
    id: uuid.UUID
    title: str
    resource_type: str
    description: str | None = None
    grade_level: str
    subject_code: str
    storage_key: str | None = None
    active: bool
    reference_links: list[CatalogResourceLinkItemOut] = Field(default_factory=list)
    associated_objectives: list[CatalogResourceObjectiveItemOut] = Field(default_factory=list)


class CatalogResourcesPageOut(BaseModel):
    items: list[CatalogResourceItemOut]
    meta: CatalogPageMetaOut
