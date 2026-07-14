"""Catalog-aligned pacing guide foundation routes."""

from __future__ import annotations

import uuid

from fastapi import APIRouter, HTTPException, Query

from oziebot_api.deps import DbSession
from oziebot_api.deps.auth import CurrentUser, RootAdminUser
from oziebot_api.schemas.pacing_guide import (
    CatalogPacingGuideActiveGuideUpdate,
    CatalogPacingGuideCopyIn,
    CatalogPacingGuideCreate,
    CatalogPacingGuideDetailOut,
    CatalogPacingGuideObjectiveCreate,
    CatalogPacingGuideObjectiveOut,
    CatalogPacingGuidePeriodCreate,
    CatalogPacingGuidePeriodNoteUpdate,
    CatalogPacingGuidePeriodOut,
    CatalogPacingGuidePeriodReorderIn,
    CatalogPacingGuidePeriodUpdate,
    CatalogPacingGuideResourceCreate,
    CatalogPacingGuideResourceOut,
    CatalogPacingGuideRolloverIn,
    CatalogPacingGuideSummaryOut,
    CatalogPacingGuideUpdate,
    PacingSchoolYearOptionsOut,
    WeekArtifactGenerateIn,
)
from oziebot_api.services.teacher_assist.current_week_resolver import (
    build_current_week_payload,
    build_objective_coverage,
)
from oziebot_api.services.teacher_assist.pacing_school_year_options import (
    build_pacing_school_year_options,
)
from oziebot_api.services.teacher_assist.pacing_guide_foundation import (
    PacingGuideRolloverService,
    add_pacing_guide_objective,
    add_pacing_guide_resource,
    copy_pacing_guide,
    create_catalog_pacing_guide,
    create_pacing_guide_period,
    deactivate_catalog_pacing_guide,
    delete_pacing_guide_period,
    get_catalog_pacing_guide_detail,
    list_catalog_pacing_guides,
    reorder_pacing_guide_periods,
    update_catalog_pacing_guide,
    update_pacing_guide_period,
)
from oziebot_api.services.teacher_assist.pacing_guide_workspace import (
    build_pacing_guide_workspace,
    build_period_launch_context,
    upsert_pacing_period_note,
)
from oziebot_api.services.teacher_assist.generated_artifacts import (
    build_generation_history,
    duplicate_generated_artifact,
    serialize_generated_artifact,
)
from oziebot_api.services.teacher_assist.week_context_service import week_context_as_of
from oziebot_api.services.teacher_assist.week_generation import generate_week_artifact
from oziebot_api.services.teacher_assist.week_workspace import build_week_workspace
from oziebot_api.services.teacher_assist.user_preferences import get_user_preferences_or_create
from oziebot_api.services.teacher_assist.setup import teacher_assist_context_for_user

router = APIRouter(prefix="/teacher-assist", tags=["teacher_assist_pacing_guides"])


def _tenant_id(db: DbSession, user: CurrentUser) -> uuid.UUID:
    try:
        return teacher_assist_context_for_user(db, user).tenant_id
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc


def _handle(action):
    try:
        return action()
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc


def _require_guide_write(user: CurrentUser, guide_type: str) -> None:
    if guide_type == "DISTRICT" and not user.is_root_admin:
        raise HTTPException(
            status_code=403, detail="Only root admins can modify district pacing guides"
        )


def _summary_out(guide, *, period_count: int) -> CatalogPacingGuideSummaryOut:
    return CatalogPacingGuideSummaryOut(
        id=guide.id,
        tenant_id=guide.tenant_id,
        school_year_id=guide.school_year_id,
        school_year_label=guide.school_year_label,
        guide_type=guide.guide_type,
        title=guide.title,
        description=guide.description,
        catalog_state_id=guide.catalog_state_id,
        catalog_district_id=guide.catalog_district_id,
        catalog_school_id=guide.catalog_school_id,
        catalog_grade_id=guide.catalog_grade_id,
        catalog_subject_id=guide.catalog_subject_id,
        is_template=guide.is_template,
        is_active=guide.is_active,
        is_shared=guide.is_shared,
        created_by_user_id=guide.created_by_user_id,
        updated_by_user_id=guide.updated_by_user_id,
        period_count=period_count,
        created_at=guide.created_at,
        updated_at=guide.updated_at,
    )


def _detail_out(guide) -> CatalogPacingGuideDetailOut:
    summary = _summary_out(guide, period_count=len(guide.periods))
    periods: list[CatalogPacingGuidePeriodOut] = []
    for period in guide.periods:
        periods.append(
            CatalogPacingGuidePeriodOut(
                id=period.id,
                pacing_guide_id=period.pacing_guide_id,
                period_type=period.period_type,
                title=period.title,
                description=period.description,
                sequence_number=period.sequence_number,
                start_date=period.start_date,
                end_date=period.end_date,
                objectives=[
                    CatalogPacingGuideObjectiveOut(
                        id=row.id,
                        objective_id=row.objective_id,
                        is_required=row.is_required,
                        notes=row.notes,
                        objective_code=getattr(
                            getattr(row, "objective", None), "objective_id", None
                        ),
                        objective_description=getattr(
                            getattr(row, "objective", None), "description", None
                        ),
                    )
                    for row in period.objectives
                ],
                resources=[
                    CatalogPacingGuideResourceOut(
                        id=row.id,
                        catalog_resource_id=row.catalog_resource_id,
                        resource_library_item_id=row.resource_library_item_id,
                        is_primary=row.is_primary,
                        notes=row.notes,
                        resource_title=getattr(
                            getattr(row, "catalog_resource", None), "title", None
                        ),
                        resource_type=getattr(
                            getattr(row, "catalog_resource", None), "resource_type", None
                        ),
                    )
                    for row in period.resources
                ],
                created_at=period.created_at,
                updated_at=period.updated_at,
            )
        )
    return CatalogPacingGuideDetailOut(**summary.model_dump(), periods=periods)


@router.get("/pacing-guides/school-year-options", response_model=PacingSchoolYearOptionsOut)
def read_pacing_guide_school_year_options(
    user: CurrentUser, db: DbSession
) -> PacingSchoolYearOptionsOut:
    tenant_id = _tenant_id(db, user)
    payload = build_pacing_school_year_options(db, tenant_id=tenant_id)
    return PacingSchoolYearOptionsOut(**payload)


@router.get("/pacing-guides", response_model=list[CatalogPacingGuideSummaryOut])
def read_catalog_pacing_guides(
    user: CurrentUser,
    db: DbSession,
    guide_type: str | None = None,
    catalog_school_id: uuid.UUID | None = None,
    active_only: bool = Query(default=True),
) -> list[CatalogPacingGuideSummaryOut]:
    tenant_id = _tenant_id(db, user)
    guides = list_catalog_pacing_guides(
        db,
        tenant_id=tenant_id,
        guide_type=guide_type,
        catalog_school_id=catalog_school_id,
        active_only=active_only,
    )
    return [
        _summary_out(guide, period_count=len(guide.periods) if hasattr(guide, "periods") else 0)
        for guide in guides
    ]


@router.post("/pacing-guides", response_model=CatalogPacingGuideDetailOut, status_code=201)
def create_catalog_pacing_guide_route(
    body: CatalogPacingGuideCreate,
    user: CurrentUser,
    db: DbSession,
) -> CatalogPacingGuideDetailOut:
    tenant_id = _tenant_id(db, user)
    _require_guide_write(user, body.guide_type)
    row = _handle(
        lambda: create_catalog_pacing_guide(
            db,
            tenant_id=tenant_id,
            actor=user,
            school_year_id=body.school_year_id,
            guide_type=body.guide_type,
            title=body.title,
            description=body.description,
            catalog_state_id=body.catalog_state_id,
            catalog_district_id=body.catalog_district_id,
            catalog_school_id=body.catalog_school_id,
            catalog_grade_id=body.catalog_grade_id,
            catalog_subject_id=body.catalog_subject_id,
            is_template=body.is_template,
            is_shared=body.is_shared,
        )
    )
    detail = get_catalog_pacing_guide_detail(db, tenant_id=tenant_id, pacing_guide_id=row.id)
    return _detail_out(detail)


@router.patch("/pacing-guides/active-selection")
def update_active_pacing_guide_selection(
    body: CatalogPacingGuideActiveGuideUpdate,
    user: CurrentUser,
    db: DbSession,
) -> dict:
    tenant_id = _tenant_id(db, user)
    row = get_user_preferences_or_create(db, tenant_id=tenant_id, user_id=user.id)
    if "active_pacing_guide_id" in body.model_fields_set:
        row.active_pacing_guide_id = body.active_pacing_guide_id
    if "manual_pacing_period_id" in body.model_fields_set:
        row.manual_pacing_period_id = body.manual_pacing_period_id
    db.flush()
    from oziebot_api.services.teacher_assist.instructional_weeks import (
        ensure_instructional_week_for_current_period,
    )

    if "active_pacing_guide_id" in body.model_fields_set and row.active_pacing_guide_id is not None:
        ensure_instructional_week_for_current_period(
            db,
            tenant_id=tenant_id,
            user=user,
            guide_id=row.active_pacing_guide_id,
        )
    payload = build_current_week_payload(
        db,
        tenant_id=tenant_id,
        user_id=user.id,
        guide_id=row.active_pacing_guide_id,
    )
    return payload


@router.get("/pacing-guides/{pacing_guide_id}", response_model=CatalogPacingGuideDetailOut)
def read_catalog_pacing_guide_detail(
    pacing_guide_id: uuid.UUID,
    user: CurrentUser,
    db: DbSession,
) -> CatalogPacingGuideDetailOut:
    tenant_id = _tenant_id(db, user)
    detail = _handle(
        lambda: get_catalog_pacing_guide_detail(
            db, tenant_id=tenant_id, pacing_guide_id=pacing_guide_id
        )
    )
    return _detail_out(detail)


@router.put("/pacing-guides/{pacing_guide_id}", response_model=CatalogPacingGuideDetailOut)
def update_catalog_pacing_guide_route(
    pacing_guide_id: uuid.UUID,
    body: CatalogPacingGuideUpdate,
    user: CurrentUser,
    db: DbSession,
) -> CatalogPacingGuideDetailOut:
    tenant_id = _tenant_id(db, user)
    _require_guide_write(user, body.guide_type)
    _handle(
        lambda: update_catalog_pacing_guide(
            db,
            tenant_id=tenant_id,
            pacing_guide_id=pacing_guide_id,
            actor=user,
            school_year_id=body.school_year_id,
            guide_type=body.guide_type,
            title=body.title,
            description=body.description,
            catalog_state_id=body.catalog_state_id,
            catalog_district_id=body.catalog_district_id,
            catalog_school_id=body.catalog_school_id,
            catalog_grade_id=body.catalog_grade_id,
            catalog_subject_id=body.catalog_subject_id,
            is_template=body.is_template,
            is_active=body.is_active,
            is_shared=body.is_shared,
        )
    )
    detail = get_catalog_pacing_guide_detail(
        db, tenant_id=tenant_id, pacing_guide_id=pacing_guide_id
    )
    return _detail_out(detail)


@router.delete("/pacing-guides/{pacing_guide_id}", response_model=CatalogPacingGuideSummaryOut)
def delete_catalog_pacing_guide_route(
    pacing_guide_id: uuid.UUID,
    user: CurrentUser,
    db: DbSession,
) -> CatalogPacingGuideSummaryOut:
    tenant_id = _tenant_id(db, user)
    existing = get_catalog_pacing_guide_detail(
        db, tenant_id=tenant_id, pacing_guide_id=pacing_guide_id
    )
    _require_guide_write(user, existing.guide_type)
    row = _handle(
        lambda: deactivate_catalog_pacing_guide(
            db, tenant_id=tenant_id, pacing_guide_id=pacing_guide_id, actor=user
        )
    )
    return _summary_out(row, period_count=len(existing.periods))


@router.post(
    "/pacing-guides/{pacing_guide_id}/periods",
    response_model=CatalogPacingGuidePeriodOut,
    status_code=201,
)
def create_pacing_guide_period_route(
    pacing_guide_id: uuid.UUID,
    body: CatalogPacingGuidePeriodCreate,
    user: CurrentUser,
    db: DbSession,
) -> CatalogPacingGuidePeriodOut:
    tenant_id = _tenant_id(db, user)
    guide = get_catalog_pacing_guide_detail(
        db, tenant_id=tenant_id, pacing_guide_id=pacing_guide_id
    )
    if guide.guide_type == "DISTRICT" and not user.is_root_admin:
        raise HTTPException(
            status_code=403, detail="Only root admins can modify district pacing guides"
        )
    row = _handle(
        lambda: create_pacing_guide_period(
            db,
            tenant_id=tenant_id,
            pacing_guide_id=pacing_guide_id,
            period_type=body.period_type,
            title=body.title,
            description=body.description,
            sequence_number=body.sequence_number,
            start_date=body.start_date,
            end_date=body.end_date,
        )
    )
    return CatalogPacingGuidePeriodOut.model_validate(
        {**row.__dict__, "objectives": [], "resources": []}
    )


@router.put(
    "/pacing-guides/{pacing_guide_id}/periods/{period_id}",
    response_model=CatalogPacingGuidePeriodOut,
)
def update_pacing_guide_period_route(
    pacing_guide_id: uuid.UUID,
    period_id: uuid.UUID,
    body: CatalogPacingGuidePeriodUpdate,
    user: CurrentUser,
    db: DbSession,
) -> CatalogPacingGuidePeriodOut:
    tenant_id = _tenant_id(db, user)
    guide = get_catalog_pacing_guide_detail(
        db, tenant_id=tenant_id, pacing_guide_id=pacing_guide_id
    )
    if guide.guide_type == "DISTRICT" and not user.is_root_admin:
        raise HTTPException(
            status_code=403, detail="Only root admins can modify district pacing guides"
        )
    row = _handle(
        lambda: update_pacing_guide_period(
            db,
            tenant_id=tenant_id,
            period_id=period_id,
            period_type=body.period_type,
            title=body.title,
            description=body.description,
            sequence_number=body.sequence_number,
            start_date=body.start_date,
            end_date=body.end_date,
        )
    )
    return CatalogPacingGuidePeriodOut.model_validate(
        {**row.__dict__, "objectives": [], "resources": []}
    )


@router.delete("/pacing-guides/{pacing_guide_id}/periods/{period_id}", status_code=204)
def delete_pacing_guide_period_route(
    pacing_guide_id: uuid.UUID,
    period_id: uuid.UUID,
    user: CurrentUser,
    db: DbSession,
) -> None:
    tenant_id = _tenant_id(db, user)
    guide = get_catalog_pacing_guide_detail(
        db, tenant_id=tenant_id, pacing_guide_id=pacing_guide_id
    )
    if guide.guide_type == "DISTRICT" and not user.is_root_admin:
        raise HTTPException(
            status_code=403, detail="Only root admins can modify district pacing guides"
        )
    _handle(lambda: delete_pacing_guide_period(db, tenant_id=tenant_id, period_id=period_id))


@router.post(
    "/pacing-guides/{pacing_guide_id}/periods/reorder",
    response_model=list[CatalogPacingGuidePeriodOut],
)
def reorder_pacing_guide_periods_route(
    pacing_guide_id: uuid.UUID,
    body: CatalogPacingGuidePeriodReorderIn,
    user: CurrentUser,
    db: DbSession,
) -> list[CatalogPacingGuidePeriodOut]:
    tenant_id = _tenant_id(db, user)
    guide = get_catalog_pacing_guide_detail(
        db, tenant_id=tenant_id, pacing_guide_id=pacing_guide_id
    )
    if guide.guide_type == "DISTRICT" and not user.is_root_admin:
        raise HTTPException(
            status_code=403, detail="Only root admins can modify district pacing guides"
        )
    rows = _handle(
        lambda: reorder_pacing_guide_periods(
            db,
            tenant_id=tenant_id,
            pacing_guide_id=pacing_guide_id,
            ordered_period_ids=body.ordered_period_ids,
        )
    )
    return [
        CatalogPacingGuidePeriodOut.model_validate(
            {**row.__dict__, "objectives": [], "resources": []}
        )
        for row in rows
    ]


@router.post(
    "/pacing-guides/{pacing_guide_id}/periods/{period_id}/objectives",
    response_model=CatalogPacingGuideObjectiveOut,
    status_code=201,
)
def add_pacing_guide_objective_route(
    pacing_guide_id: uuid.UUID,
    period_id: uuid.UUID,
    body: CatalogPacingGuideObjectiveCreate,
    user: CurrentUser,
    db: DbSession,
) -> CatalogPacingGuideObjectiveOut:
    tenant_id = _tenant_id(db, user)
    guide = get_catalog_pacing_guide_detail(
        db, tenant_id=tenant_id, pacing_guide_id=pacing_guide_id
    )
    if guide.guide_type == "DISTRICT" and not user.is_root_admin:
        raise HTTPException(
            status_code=403, detail="Only root admins can modify district pacing guides"
        )
    row = _handle(
        lambda: add_pacing_guide_objective(
            db,
            tenant_id=tenant_id,
            period_id=period_id,
            objective_id=body.objective_id,
            is_required=body.is_required,
            notes=body.notes,
        )
    )
    return CatalogPacingGuideObjectiveOut.model_validate(row)


@router.post(
    "/pacing-guides/{pacing_guide_id}/periods/{period_id}/resources",
    response_model=CatalogPacingGuideResourceOut,
    status_code=201,
)
def add_pacing_guide_resource_route(
    pacing_guide_id: uuid.UUID,
    period_id: uuid.UUID,
    body: CatalogPacingGuideResourceCreate,
    user: CurrentUser,
    db: DbSession,
) -> CatalogPacingGuideResourceOut:
    tenant_id = _tenant_id(db, user)
    guide = get_catalog_pacing_guide_detail(
        db, tenant_id=tenant_id, pacing_guide_id=pacing_guide_id
    )
    if guide.guide_type == "DISTRICT" and not user.is_root_admin:
        raise HTTPException(
            status_code=403, detail="Only root admins can modify district pacing guides"
        )
    row = _handle(
        lambda: add_pacing_guide_resource(
            db,
            tenant_id=tenant_id,
            period_id=period_id,
            catalog_resource_id=body.catalog_resource_id,
            resource_library_item_id=body.resource_library_item_id,
            is_primary=body.is_primary,
            notes=body.notes,
        )
    )
    return CatalogPacingGuideResourceOut.model_validate(row)


@router.post(
    "/pacing-guides/{pacing_guide_id}/copy",
    response_model=CatalogPacingGuideDetailOut,
    status_code=201,
)
def copy_pacing_guide_route(
    pacing_guide_id: uuid.UUID,
    body: CatalogPacingGuideCopyIn,
    user: CurrentUser,
    db: DbSession,
) -> CatalogPacingGuideDetailOut:
    tenant_id = _tenant_id(db, user)
    if body.target_guide_type == "DISTRICT" and not user.is_root_admin:
        raise HTTPException(
            status_code=403, detail="Only root admins can create district pacing guides"
        )
    detail = _handle(
        lambda: copy_pacing_guide(
            db,
            tenant_id=tenant_id,
            actor=user,
            source_guide_id=pacing_guide_id,
            target_guide_type=body.target_guide_type,
            title=body.title,
            school_year_id=body.school_year_id,
        )
    )
    return _detail_out(detail)


@router.post("/pacing-guides/rollover", response_model=list[CatalogPacingGuideDetailOut])
def rollover_pacing_guides_route(
    body: CatalogPacingGuideRolloverIn,
    user: RootAdminUser,
    db: DbSession,
) -> list[CatalogPacingGuideDetailOut]:
    tenant_id = _tenant_id(db, user)
    guides = _handle(
        lambda: PacingGuideRolloverService.rollover_school_year(
            db,
            tenant_id=tenant_id,
            actor=user,
            source_school_year_id=body.source_school_year_id,
            target_school_year_id=body.target_school_year_id,
            guide_ids=body.guide_ids,
        )
    )
    return [
        _detail_out(
            get_catalog_pacing_guide_detail(db, tenant_id=tenant_id, pacing_guide_id=guide.id)
        )
        for guide in guides
    ]


@router.get("/current-week")
def read_current_week(
    user: CurrentUser,
    db: DbSession,
    guide_id: uuid.UUID | None = None,
) -> dict:
    tenant_id = _tenant_id(db, user)
    return build_current_week_payload(db, tenant_id=tenant_id, user_id=user.id, guide_id=guide_id)


@router.get("/pacing-guide-workspace")
def read_pacing_guide_workspace(
    user: CurrentUser,
    db: DbSession,
    guide_id: uuid.UUID | None = None,
    period_id: uuid.UUID | None = None,
) -> dict:
    tenant_id = _tenant_id(db, user)
    return build_pacing_guide_workspace(
        db,
        tenant_id=tenant_id,
        user_id=user.id,
        guide_id=guide_id,
        period_id=period_id,
    )


@router.get("/pacing-guides/{pacing_guide_id}/objective-coverage")
def read_pacing_guide_objective_coverage(
    pacing_guide_id: uuid.UUID,
    user: CurrentUser,
    db: DbSession,
) -> dict:
    tenant_id = _tenant_id(db, user)
    return build_objective_coverage(
        db,
        tenant_id=tenant_id,
        user_id=user.id,
        guide_id=pacing_guide_id,
    )


@router.put("/pacing-guides/{pacing_guide_id}/periods/{period_id}/notes")
def update_pacing_guide_period_notes(
    pacing_guide_id: uuid.UUID,
    period_id: uuid.UUID,
    body: CatalogPacingGuidePeriodNoteUpdate,
    user: CurrentUser,
    db: DbSession,
) -> dict:
    tenant_id = _tenant_id(db, user)
    _handle(
        lambda: get_catalog_pacing_guide_detail(
            db, tenant_id=tenant_id, pacing_guide_id=pacing_guide_id
        )
    )
    row = _handle(
        lambda: upsert_pacing_period_note(
            db,
            tenant_id=tenant_id,
            user_id=user.id,
            period_id=period_id,
            notes=body.notes,
        )
    )
    return {"period_id": row.period_id, "notes": row.notes, "updated_at": row.updated_at}


@router.get("/pacing-guide-periods/{period_id}/launch-context")
def read_pacing_guide_period_launch_context(
    period_id: uuid.UUID,
    user: CurrentUser,
    db: DbSession,
) -> dict:
    tenant_id = _tenant_id(db, user)
    return _handle(
        lambda: build_period_launch_context(
            db,
            tenant_id=tenant_id,
            user=user,
            period_id=period_id,
        )
    )


@router.get("/week-workspace")
def read_week_workspace(
    user: CurrentUser,
    db: DbSession,
    period_id: uuid.UUID = Query(...),
) -> dict:
    tenant_id = _tenant_id(db, user)
    return _handle(
        lambda: build_week_workspace(
            db,
            tenant_id=tenant_id,
            user=user,
            period_id=period_id,
        )
    )


@router.get("/pacing-guide-periods/{period_id}/week-context")
def read_pacing_guide_period_week_context(
    period_id: uuid.UUID,
    user: CurrentUser,
    db: DbSession,
) -> dict:
    tenant_id = _tenant_id(db, user)
    return _handle(
        lambda: week_context_as_of(db, tenant_id=tenant_id, user=user, period_id=period_id)
    )


@router.get("/pacing-guide-periods/{period_id}/artifacts")
def list_pacing_guide_period_artifacts(
    period_id: uuid.UUID,
    user: CurrentUser,
    db: DbSession,
) -> list[dict]:
    tenant_id = _tenant_id(db, user)
    return build_generation_history(
        db,
        tenant_id=tenant_id,
        user_id=user.id,
        pacing_guide_period_id=period_id,
    )


@router.post("/pacing-guide-periods/{period_id}/generate", status_code=201)
def generate_pacing_guide_period_artifact(
    period_id: uuid.UUID,
    body: WeekArtifactGenerateIn,
    user: CurrentUser,
    db: DbSession,
) -> dict:
    tenant_id = _tenant_id(db, user)
    return _handle(
        lambda: generate_week_artifact(
            db,
            tenant_id=tenant_id,
            user=user,
            period_id=period_id,
            artifact_type=body.artifact_type,
            class_id=body.class_id,
        )
    )


@router.post("/pacing-guide-periods/{period_id}/artifacts/{artifact_id}/duplicate", status_code=201)
def duplicate_pacing_guide_period_artifact(
    period_id: uuid.UUID,
    artifact_id: uuid.UUID,
    user: CurrentUser,
    db: DbSession,
) -> dict:
    tenant_id = _tenant_id(db, user)
    row = _handle(
        lambda: duplicate_generated_artifact(
            db,
            tenant_id=tenant_id,
            user=user,
            artifact_id=artifact_id,
        )
    )
    if row.pacing_guide_period_id != period_id:
        raise HTTPException(status_code=404, detail="Generated artifact not found for this week")
    return serialize_generated_artifact(db, row)
