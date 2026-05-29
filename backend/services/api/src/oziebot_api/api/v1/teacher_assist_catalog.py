"""Read-only TeacherAssist catalog browse routes."""

from __future__ import annotations

import uuid

from fastapi import APIRouter, HTTPException, Query

from oziebot_api.deps import DbSession
from oziebot_api.deps.auth import CurrentUser
from oziebot_api.schemas.catalog_access import (
    CatalogBrowseContextOut,
    CatalogGradesPageOut,
    CatalogGradeItemOut,
    CatalogLinkedResourceItemOut,
    CatalogObjectivesPageOut,
    CatalogObjectiveItemOut,
    CatalogPageMetaOut,
    CatalogResourcesPageOut,
    CatalogResourceItemOut,
    CatalogResourceLinkItemOut,
    CatalogResourceObjectiveItemOut,
    CatalogSubjectsPageOut,
    CatalogSubjectItemOut,
    CatalogScopeFiltersOut,
    CatalogScopeLabelsOut,
    CatalogAssignmentOut,
)
from oziebot_api.services.teacher_assist.catalog_access import (
    CatalogAccessError,
    build_catalog_context,
    list_catalog_grades,
    list_catalog_objectives,
    list_catalog_resources,
    list_catalog_subjects,
    require_catalog_browse_scope,
)
from oziebot_api.services.teacher_assist.setup import teacher_assist_context_for_user

router = APIRouter(prefix="/teacher-assist/catalog", tags=["teacher_assist_catalog"])


def _require_teacher_assist(db: DbSession, user: CurrentUser):
    try:
        return teacher_assist_context_for_user(db, user)
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc


def _meta_out(meta) -> CatalogPageMetaOut:
    return CatalogPageMetaOut(
        page=meta.page,
        page_size=meta.page_size,
        total=meta.total,
        total_pages=meta.total_pages,
    )


@router.get("/context", response_model=CatalogBrowseContextOut)
def read_catalog_context(
    user: CurrentUser,
    db: DbSession,
    state_id: uuid.UUID | None = None,
    district_id: uuid.UUID | None = None,
    school_id: uuid.UUID | None = None,
) -> CatalogBrowseContextOut:
    ta_context = _require_teacher_assist(db, user)
    payload = build_catalog_context(
        db,
        user=user,
        tenant_id=ta_context.tenant_id,
        is_root_admin=user.is_root_admin,
        state_id=state_id,
        district_id=district_id,
        school_id=school_id,
    )
    assignment = payload["assignment"]
    return CatalogBrowseContextOut(
        assignment=CatalogAssignmentOut(**assignment) if assignment else None,
        missing_assignment=payload["missing_assignment"],
        multiple_assignments_detected=payload["multiple_assignments_detected"],
        can_browse=payload["can_browse"],
        is_root_unscoped=payload["is_root_unscoped"],
        scope_filters=CatalogScopeFiltersOut(**payload["scope_filters"]),
        scope_labels=CatalogScopeLabelsOut(**payload["scope_labels"]),
        scope_banner=payload["scope_banner"],
    )


@router.get("/grades", response_model=CatalogGradesPageOut)
def read_catalog_grades(
    user: CurrentUser,
    db: DbSession,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=25, ge=1, le=100),
    state_id: uuid.UUID | None = None,
    district_id: uuid.UUID | None = None,
    school_id: uuid.UUID | None = None,
) -> CatalogGradesPageOut:
    ta_context = _require_teacher_assist(db, user)
    try:
        scope = require_catalog_browse_scope(
            db,
            user=user,
            tenant_id=ta_context.tenant_id,
            is_root_admin=user.is_root_admin,
            state_id=state_id,
            district_id=district_id,
            school_id=school_id,
        )
    except CatalogAccessError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    items, meta = list_catalog_grades(db, scope=scope, page=page, page_size=page_size)
    return CatalogGradesPageOut(
        items=[CatalogGradeItemOut(**item) for item in items],
        meta=_meta_out(meta),
    )


@router.get("/subjects", response_model=CatalogSubjectsPageOut)
def read_catalog_subjects(
    user: CurrentUser,
    db: DbSession,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=25, ge=1, le=100),
    grade_id: uuid.UUID | None = None,
    state_id: uuid.UUID | None = None,
    district_id: uuid.UUID | None = None,
    school_id: uuid.UUID | None = None,
) -> CatalogSubjectsPageOut:
    ta_context = _require_teacher_assist(db, user)
    try:
        scope = require_catalog_browse_scope(
            db,
            user=user,
            tenant_id=ta_context.tenant_id,
            is_root_admin=user.is_root_admin,
            state_id=state_id,
            district_id=district_id,
            school_id=school_id,
        )
    except CatalogAccessError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    items, meta = list_catalog_subjects(
        db,
        scope=scope,
        grade_id=grade_id,
        page=page,
        page_size=page_size,
    )
    return CatalogSubjectsPageOut(
        items=[CatalogSubjectItemOut(**item) for item in items],
        meta=_meta_out(meta),
    )


@router.get("/objectives", response_model=CatalogObjectivesPageOut)
def read_catalog_objectives(
    user: CurrentUser,
    db: DbSession,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=25, ge=1, le=100),
    grade_level: str | None = None,
    subject_code: str | None = None,
    objective_type: str | None = None,
    coverage_type: str | None = None,
    q: str | None = None,
    state_id: uuid.UUID | None = None,
    district_id: uuid.UUID | None = None,
    school_id: uuid.UUID | None = None,
) -> CatalogObjectivesPageOut:
    ta_context = _require_teacher_assist(db, user)
    try:
        scope = require_catalog_browse_scope(
            db,
            user=user,
            tenant_id=ta_context.tenant_id,
            is_root_admin=user.is_root_admin,
            state_id=state_id,
            district_id=district_id,
            school_id=school_id,
        )
    except CatalogAccessError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    items, meta = list_catalog_objectives(
        db,
        scope=scope,
        grade_level=grade_level,
        subject_code=subject_code,
        objective_type=objective_type,
        coverage_type=coverage_type,
        q=q,
        page=page,
        page_size=page_size,
    )
    return CatalogObjectivesPageOut(
        items=[
            CatalogObjectiveItemOut(
                **{
                    **item,
                    "linked_resources": [
                        CatalogLinkedResourceItemOut(
                            **{
                                **resource,
                                "reference_links": [
                                    CatalogResourceLinkItemOut(**link) for link in resource["reference_links"]
                                ],
                            }
                        )
                        for resource in item["linked_resources"]
                    ],
                }
            )
            for item in items
        ],
        meta=_meta_out(meta),
    )


@router.get("/resources", response_model=CatalogResourcesPageOut)
def read_catalog_resources(
    user: CurrentUser,
    db: DbSession,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=25, ge=1, le=100),
    grade_level: str | None = None,
    subject_code: str | None = None,
    resource_type: str | None = None,
    q: str | None = None,
    state_id: uuid.UUID | None = None,
    district_id: uuid.UUID | None = None,
    school_id: uuid.UUID | None = None,
) -> CatalogResourcesPageOut:
    ta_context = _require_teacher_assist(db, user)
    try:
        scope = require_catalog_browse_scope(
            db,
            user=user,
            tenant_id=ta_context.tenant_id,
            is_root_admin=user.is_root_admin,
            state_id=state_id,
            district_id=district_id,
            school_id=school_id,
        )
    except CatalogAccessError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    items, meta = list_catalog_resources(
        db,
        scope=scope,
        grade_level=grade_level,
        subject_code=subject_code,
        resource_type=resource_type,
        q=q,
        page=page,
        page_size=page_size,
    )
    return CatalogResourcesPageOut(
        items=[
            CatalogResourceItemOut(
                **{
                    **item,
                    "reference_links": [CatalogResourceLinkItemOut(**link) for link in item["reference_links"]],
                    "associated_objectives": [
                        CatalogResourceObjectiveItemOut(**objective)
                        for objective in item["associated_objectives"]
                    ],
                }
            )
            for item in items
        ],
        meta=_meta_out(meta),
    )
