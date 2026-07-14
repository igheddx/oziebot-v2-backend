"""Teacher time savings and reuse routes."""

from __future__ import annotations

import uuid

from fastapi import APIRouter, HTTPException, Query

from oziebot_api.deps import DbSession
from oziebot_api.deps.auth import CurrentUser
from oziebot_api.schemas.time_savings import (
    PlanningGroupCreateIn,
    RolloverV2In,
    WeekDuplicateIn,
    WeekTemplateApplyIn,
    WeekTemplateSaveIn,
)
from oziebot_api.services.teacher_assist.generate_next_week import generate_next_week_draft
from oziebot_api.services.teacher_assist.instructional_asset_reuse import (
    InstructionalAssetReuseService,
)
from oziebot_api.services.teacher_assist.planning_groups import (
    create_planning_group,
    join_planning_group,
    list_planning_groups,
    serialize_planning_group,
)
from oziebot_api.services.teacher_assist.recommendation_service import build_week_recommendations
from oziebot_api.services.teacher_assist.rollover_v2 import rollover_school_year_v2
from oziebot_api.services.teacher_assist.teacher_efficiency import (
    build_teacher_efficiency_dashboard,
)
from oziebot_api.services.teacher_assist.week_duplication import duplicate_week
from oziebot_api.services.teacher_assist.week_templates import (
    apply_week_template,
    list_template_library,
    save_week_as_template,
    serialize_template,
)

router = APIRouter(prefix="/teacher-assist", tags=["teacher_assist_time_savings"])


def _tenant_id(db, user) -> uuid.UUID:
    from oziebot_api.api.v1.teacher_assist_pacing_guides import _tenant_id as pacing_tenant_id

    return pacing_tenant_id(db, user)


def _handle(fn):
    try:
        return fn()
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc


@router.get("/reuse/search")
def search_reusable_assets(
    user: CurrentUser,
    db: DbSession,
    period_id: uuid.UUID = Query(...),
    limit: int = Query(default=12, ge=1, le=50),
) -> list[dict]:
    tenant_id = _tenant_id(db, user)
    return InstructionalAssetReuseService.search(
        db, tenant_id=tenant_id, user=user, period_id=period_id, limit=limit
    )


@router.post("/pacing-guide-periods/{period_id}/duplicate")
def duplicate_pacing_week(
    period_id: uuid.UUID,
    body: WeekDuplicateIn,
    user: CurrentUser,
    db: DbSession,
) -> dict:
    tenant_id = _tenant_id(db, user)
    return _handle(
        lambda: duplicate_week(
            db,
            tenant_id=tenant_id,
            user=user,
            source_period_id=period_id,
            target_period_id=body.target_period_id,
            target_guide_id=body.target_guide_id,
            target_school_year_id=body.target_school_year_id,
            copy_objectives=body.copy_objectives,
            copy_resources=body.copy_resources,
            copy_notes=body.copy_notes,
            copy_artifacts=body.copy_artifacts,
        )
    )


@router.post("/pacing-guide-periods/{period_id}/generate-next-week")
def generate_next_week(
    period_id: uuid.UUID,
    user: CurrentUser,
    db: DbSession,
) -> dict:
    tenant_id = _tenant_id(db, user)
    return _handle(
        lambda: generate_next_week_draft(db, tenant_id=tenant_id, user=user, period_id=period_id)
    )


@router.get("/pacing-guide-periods/{period_id}/recommendations")
def read_week_recommendations(
    period_id: uuid.UUID,
    user: CurrentUser,
    db: DbSession,
) -> dict:
    tenant_id = _tenant_id(db, user)
    return build_week_recommendations(db, tenant_id=tenant_id, user=user, period_id=period_id)


@router.get("/week-templates")
def list_week_templates(
    user: CurrentUser,
    db: DbSession,
    subject: str | None = None,
    grade_level: str | None = None,
    artifact_type: str | None = None,
    visibility: str | None = None,
) -> list[dict]:
    tenant_id = _tenant_id(db, user)
    rows = list_template_library(
        db,
        tenant_id=tenant_id,
        user_id=user.id,
        subject=subject,
        grade_level=grade_level,
        artifact_type=artifact_type,
        visibility=visibility,
    )
    return [serialize_template(row) for row in rows]


@router.post("/pacing-guide-periods/{period_id}/templates", status_code=201)
def save_week_template(
    period_id: uuid.UUID,
    body: WeekTemplateSaveIn,
    user: CurrentUser,
    db: DbSession,
) -> dict:
    tenant_id = _tenant_id(db, user)
    row = _handle(
        lambda: save_week_as_template(
            db,
            tenant_id=tenant_id,
            user=user,
            period_id=period_id,
            name=body.name,
            description=body.description,
            template_type=body.template_type,
            visibility=body.visibility,
            artifact_type=body.artifact_type,
        )
    )
    return serialize_template(row)


@router.post("/week-templates/{template_id}/apply")
def apply_template_to_week(
    template_id: uuid.UUID,
    body: WeekTemplateApplyIn,
    user: CurrentUser,
    db: DbSession,
) -> dict:
    tenant_id = _tenant_id(db, user)
    return _handle(
        lambda: apply_week_template(
            db,
            tenant_id=tenant_id,
            user=user,
            template_id=template_id,
            target_period_id=body.target_period_id,
        )
    )


@router.post("/rollover/v2")
def rollover_v2(
    body: RolloverV2In,
    user: CurrentUser,
    db: DbSession,
) -> dict:
    tenant_id = _tenant_id(db, user)
    return _handle(
        lambda: rollover_school_year_v2(
            db,
            tenant_id=tenant_id,
            user=user,
            source_school_year_id=body.source_school_year_id,
            target_school_year_id=body.target_school_year_id,
            pacing_guide_ids=body.pacing_guide_ids,
            period_ids=body.period_ids,
            copy_instructional_plans=body.copy_instructional_plans,
            copy_assignments=body.copy_assignments,
            copy_quizzes=body.copy_quizzes,
            copy_rubrics=body.copy_rubrics,
            copy_resources=body.copy_resources,
        )
    )


@router.get("/planning-groups")
def read_planning_groups(user: CurrentUser, db: DbSession) -> list[dict]:
    tenant_id = _tenant_id(db, user)
    return [
        serialize_planning_group(row)
        for row in list_planning_groups(db, tenant_id=tenant_id, user_id=user.id)
    ]


@router.post("/planning-groups", status_code=201)
def create_planning_group_route(
    body: PlanningGroupCreateIn, user: CurrentUser, db: DbSession
) -> dict:
    tenant_id = _tenant_id(db, user)
    row = create_planning_group(
        db,
        tenant_id=tenant_id,
        user=user,
        name=body.name,
        description=body.description,
        subject=body.subject,
        grade_level=body.grade_level,
        visibility=body.visibility,
    )
    return serialize_planning_group(row)


@router.post("/planning-groups/{group_id}/join", status_code=201)
def join_planning_group_route(group_id: uuid.UUID, user: CurrentUser, db: DbSession) -> dict:
    tenant_id = _tenant_id(db, user)
    join_planning_group(db, tenant_id=tenant_id, user_id=user.id, group_id=group_id)
    groups = list_planning_groups(db, tenant_id=tenant_id, user_id=user.id)
    match = next((row for row in groups if row.id == group_id), None)
    if match is None:
        raise HTTPException(status_code=404, detail="Planning group not found")
    return serialize_planning_group(match)


@router.get("/efficiency-dashboard")
def read_efficiency_dashboard(
    user: CurrentUser,
    db: DbSession,
    school_year_id: uuid.UUID | None = None,
) -> dict:
    tenant_id = _tenant_id(db, user)
    return build_teacher_efficiency_dashboard(
        db, tenant_id=tenant_id, user_id=user.id, school_year_id=school_year_id
    )
