"""Phase 38 — Assignment → Gradebook → Mastery → Reteach instructional loop APIs."""

from __future__ import annotations

import uuid
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from oziebot_api.deps import DbSession
from oziebot_api.deps.auth import CurrentUser
from oziebot_api.services.teacher_assist.assignment_coverage import build_assignment_coverage_view
from oziebot_api.services.teacher_assist.gradebook_v2 import build_gradebook_v2_view
from oziebot_api.services.teacher_assist.instructional_evidence import (
    confirm_instructional_evidence,
    list_instructional_evidence,
    record_instructional_evidence,
    serialize_instructional_evidence,
)
from oziebot_api.services.teacher_assist.instructional_health_report import build_instructional_health_report
from oziebot_api.services.teacher_assist.instructional_reflections import (
    list_instructional_reflections,
    serialize_instructional_reflection,
    upsert_instructional_reflection,
)
from oziebot_api.services.teacher_assist.instructional_week_closure import (
    generate_instructional_week_summary,
    get_or_create_week_closure,
    serialize_week_closure,
    serialize_week_summary,
    update_week_closure_checklist,
)
from oziebot_api.services.teacher_assist.mastery_dashboard_v2 import build_mastery_dashboard_v2
from oziebot_api.services.teacher_assist.objective_performance import ObjectivePerformanceService
from oziebot_api.services.teacher_assist.recommendation_v2 import build_instructional_loop_recommendations
from oziebot_api.services.teacher_assist.reteach_effectiveness import (
    list_reteach_effectiveness,
    record_reteach_effectiveness,
    serialize_reteach_effectiveness,
)
from oziebot_api.services.teacher_assist.reteach_workspace import build_reteach_workspace
from oziebot_api.services.teacher_assist.student_support_groups import (
    create_support_group,
    list_support_groups,
    serialize_support_group,
    update_support_group_status,
)


router = APIRouter(prefix="/teacher-assist", tags=["teacher_assist_instructional_loop"])


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


class InstructionalEvidenceIn(BaseModel):
    student_identifier: str
    source_type: str
    source_id: uuid.UUID
    objective_id: uuid.UUID | None = None
    standard_id: uuid.UUID | None = None
    class_id: uuid.UUID | None = None
    subject_id: uuid.UUID | None = None
    instructional_week_id: uuid.UUID | None = None
    score: float | None = None
    mastery_level: str | None = None
    teacher_confirmed: bool = False
    teacher_notes: str | None = None


class InstructionalEvidenceConfirmIn(BaseModel):
    mastery_level: str | None = None
    teacher_notes: str | None = None


class SupportGroupIn(BaseModel):
    class_id: uuid.UUID
    subject_id: uuid.UUID
    title: str
    student_identifiers: list[str] = Field(min_length=1)
    instructional_week_id: uuid.UUID | None = None
    objective_id: uuid.UUID | None = None
    standard_id: uuid.UUID | None = None
    notes: str | None = None
    suggested_activities: list[dict[str, Any]] | None = None
    status: str = "draft"


class SupportGroupStatusIn(BaseModel):
    status: str


class InstructionalReflectionIn(BaseModel):
    instructional_week_id: uuid.UUID | None = None
    class_id: uuid.UUID | None = None
    subject_id: uuid.UUID | None = None
    what_worked: str | None = None
    what_didnt_work: str | None = None
    student_challenges: str | None = None
    adjustments_needed: str | None = None
    future_recommendations: str | None = None
    status: str | None = None


class WeekClosureChecklistIn(BaseModel):
    checklist: dict[str, bool]


class ReteachEffectivenessIn(BaseModel):
    before_mastery_pct: float | None = None
    after_mastery_pct: float | None = None
    teacher_reflection: str | None = None


@router.get("/mastery-dashboard/v2")
def read_mastery_dashboard_v2(
    user: CurrentUser,
    db: DbSession,
    school_year_id: uuid.UUID | None = None,
    grading_period_id: uuid.UUID | None = None,
    class_id: uuid.UUID | None = None,
    subject_id: uuid.UUID | None = None,
    instructional_week_id: uuid.UUID | None = None,
) -> dict:
    tenant_id = _tenant_id(db, user)
    return build_mastery_dashboard_v2(
        db,
        tenant_id=tenant_id,
        user_id=user.id,
        school_year_id=school_year_id,
        grading_period_id=grading_period_id,
        class_id=class_id,
        subject_id=subject_id,
        instructional_week_id=instructional_week_id,
    )


@router.get("/objective-performance")
def read_objective_performance(
    user: CurrentUser,
    db: DbSession,
    objective_id: uuid.UUID | None = None,
    standard_id: uuid.UUID | None = None,
    class_id: uuid.UUID | None = None,
    instructional_week_id: uuid.UUID | None = None,
) -> dict:
    tenant_id = _tenant_id(db, user)
    if objective_id or standard_id:
        return ObjectivePerformanceService.calculate_for_objective(
            db,
            tenant_id=tenant_id,
            user_id=user.id,
            objective_id=objective_id,
            standard_id=standard_id,
            class_id=class_id,
            instructional_week_id=instructional_week_id,
        )
    return ObjectivePerformanceService.calculate_for_scope(
        db,
        tenant_id=tenant_id,
        user_id=user.id,
        class_id=class_id,
        instructional_week_id=instructional_week_id,
    )


@router.get("/assignment-coverage")
def read_assignment_coverage(
    user: CurrentUser,
    db: DbSession,
    assignment_id: uuid.UUID | None = None,
    instructional_week_id: uuid.UUID | None = None,
    class_id: uuid.UUID | None = None,
) -> dict:
    tenant_id = _tenant_id(db, user)
    return build_assignment_coverage_view(
        db,
        tenant_id=tenant_id,
        user_id=user.id,
        assignment_id=assignment_id,
        instructional_week_id=instructional_week_id,
        class_id=class_id,
    )


@router.get("/gradebook/v2")
def read_gradebook_v2(
    user: CurrentUser,
    db: DbSession,
    class_id: uuid.UUID | None = None,
    assignment_id: uuid.UUID | None = None,
) -> dict:
    tenant_id = _tenant_id(db, user)
    return build_gradebook_v2_view(
        db,
        tenant_id=tenant_id,
        user_id=user.id,
        class_id=class_id,
        assignment_id=assignment_id,
    )


@router.get("/instructional-evidence")
def read_instructional_evidence(
    user: CurrentUser,
    db: DbSession,
    class_id: uuid.UUID | None = None,
    instructional_week_id: uuid.UUID | None = None,
    source_type: str | None = None,
    teacher_confirmed: bool | None = None,
) -> list[dict]:
    tenant_id = _tenant_id(db, user)
    rows = list_instructional_evidence(
        db,
        tenant_id=tenant_id,
        user_id=user.id,
        class_id=class_id,
        instructional_week_id=instructional_week_id,
        source_type=source_type,
        teacher_confirmed=teacher_confirmed,
    )
    return [serialize_instructional_evidence(row) for row in rows]


@router.post("/instructional-evidence", status_code=201)
def create_instructional_evidence(user: CurrentUser, db: DbSession, body: InstructionalEvidenceIn) -> dict:
    tenant_id = _tenant_id(db, user)

    def _create():
        row = record_instructional_evidence(
            db,
            tenant_id=tenant_id,
            user_id=user.id,
            **body.model_dump(),
        )
        db.commit()
        return serialize_instructional_evidence(row)

    return _handle(_create)


@router.post("/instructional-evidence/{evidence_id}/confirm")
def confirm_evidence(
    evidence_id: uuid.UUID,
    user: CurrentUser,
    db: DbSession,
    body: InstructionalEvidenceConfirmIn,
) -> dict:
    tenant_id = _tenant_id(db, user)

    def _confirm():
        row = confirm_instructional_evidence(
            db,
            tenant_id=tenant_id,
            user_id=user.id,
            evidence_id=evidence_id,
            mastery_level=body.mastery_level,
            teacher_notes=body.teacher_notes,
        )
        db.commit()
        return serialize_instructional_evidence(row)

    return _handle(_confirm)


@router.get("/reteach-workspace")
def read_reteach_workspace(
    user: CurrentUser,
    db: DbSession,
    class_id: uuid.UUID | None = None,
    subject_id: uuid.UUID | None = None,
    instructional_week_id: uuid.UUID | None = None,
) -> dict:
    tenant_id = _tenant_id(db, user)
    return build_reteach_workspace(
        db,
        tenant_id=tenant_id,
        user_id=user.id,
        class_id=class_id,
        subject_id=subject_id,
        instructional_week_id=instructional_week_id,
    )


@router.get("/support-groups")
def read_support_groups(
    user: CurrentUser,
    db: DbSession,
    class_id: uuid.UUID | None = None,
    instructional_week_id: uuid.UUID | None = None,
    status: str | None = None,
) -> list[dict]:
    tenant_id = _tenant_id(db, user)
    rows = list_support_groups(
        db,
        tenant_id=tenant_id,
        user_id=user.id,
        class_id=class_id,
        instructional_week_id=instructional_week_id,
        status=status,
    )
    return [serialize_support_group(row) for row in rows]


@router.post("/support-groups", status_code=201)
def create_support_group_route(user: CurrentUser, db: DbSession, body: SupportGroupIn) -> dict:
    tenant_id = _tenant_id(db, user)

    def _create():
        row = create_support_group(db, tenant_id=tenant_id, user_id=user.id, **body.model_dump())
        db.commit()
        return serialize_support_group(row)

    return _handle(_create)


@router.patch("/support-groups/{group_id}/status")
def update_support_group_route(
    group_id: uuid.UUID,
    user: CurrentUser,
    db: DbSession,
    body: SupportGroupStatusIn,
) -> dict:
    tenant_id = _tenant_id(db, user)

    def _update():
        row = update_support_group_status(
            db,
            tenant_id=tenant_id,
            user_id=user.id,
            group_id=group_id,
            status=body.status,
        )
        db.commit()
        return serialize_support_group(row)

    return _handle(_update)


@router.get("/instructional-reflections")
def read_instructional_reflections(
    user: CurrentUser,
    db: DbSession,
    instructional_week_id: uuid.UUID | None = None,
) -> list[dict]:
    tenant_id = _tenant_id(db, user)
    rows = list_instructional_reflections(
        db,
        tenant_id=tenant_id,
        user_id=user.id,
        instructional_week_id=instructional_week_id,
    )
    return [serialize_instructional_reflection(row) for row in rows]


@router.put("/instructional-reflections")
def upsert_instructional_reflection_route(user: CurrentUser, db: DbSession, body: InstructionalReflectionIn) -> dict:
    tenant_id = _tenant_id(db, user)

    def _upsert():
        row = upsert_instructional_reflection(
            db,
            tenant_id=tenant_id,
            user_id=user.id,
            **body.model_dump(),
        )
        db.commit()
        return serialize_instructional_reflection(row)

    return _handle(_upsert)


@router.get("/instructional-weeks/{instructional_week_id}/closure")
def read_week_closure(instructional_week_id: uuid.UUID, user: CurrentUser, db: DbSession) -> dict:
    tenant_id = _tenant_id(db, user)

    def _read():
        row = get_or_create_week_closure(
            db,
            tenant_id=tenant_id,
            user_id=user.id,
            instructional_week_id=instructional_week_id,
        )
        db.commit()
        return serialize_week_closure(row)

    return _handle(_read)


@router.patch("/instructional-weeks/{instructional_week_id}/closure")
def update_week_closure(
    instructional_week_id: uuid.UUID,
    user: CurrentUser,
    db: DbSession,
    body: WeekClosureChecklistIn,
) -> dict:
    tenant_id = _tenant_id(db, user)

    def _update():
        row = update_week_closure_checklist(
            db,
            tenant_id=tenant_id,
            user_id=user.id,
            instructional_week_id=instructional_week_id,
            checklist=body.checklist,
        )
        db.commit()
        return serialize_week_closure(row)

    return _handle(_update)


@router.post("/instructional-weeks/{instructional_week_id}/summary", status_code=201)
def create_week_summary(instructional_week_id: uuid.UUID, user: CurrentUser, db: DbSession) -> dict:
    tenant_id = _tenant_id(db, user)

    def _generate():
        row = generate_instructional_week_summary(
            db,
            tenant_id=tenant_id,
            user_id=user.id,
            instructional_week_id=instructional_week_id,
        )
        db.commit()
        return serialize_week_summary(row)

    return _handle(_generate)


@router.get("/instructional-loop/recommendations")
def read_instructional_loop_recommendations(
    user: CurrentUser,
    db: DbSession,
    class_id: uuid.UUID | None = None,
    subject_id: uuid.UUID | None = None,
    instructional_week_id: uuid.UUID | None = None,
) -> dict:
    tenant_id = _tenant_id(db, user)
    return build_instructional_loop_recommendations(
        db,
        tenant_id=tenant_id,
        user_id=user.id,
        class_id=class_id,
        subject_id=subject_id,
        instructional_week_id=instructional_week_id,
    )


@router.get("/instructional-health-report")
def read_instructional_health_report(
    user: CurrentUser,
    db: DbSession,
    class_id: uuid.UUID | None = None,
    subject_id: uuid.UUID | None = None,
    instructional_week_id: uuid.UUID | None = None,
    school_year_id: uuid.UUID | None = None,
    grading_period_id: uuid.UUID | None = None,
) -> dict:
    tenant_id = _tenant_id(db, user)
    return build_instructional_health_report(
        db,
        tenant_id=tenant_id,
        user_id=user.id,
        class_id=class_id,
        subject_id=subject_id,
        instructional_week_id=instructional_week_id,
        school_year_id=school_year_id,
        grading_period_id=grading_period_id,
    )


@router.post("/reteach-plans/{reteach_plan_id}/effectiveness", status_code=201)
def create_reteach_effectiveness(
    reteach_plan_id: uuid.UUID,
    user: CurrentUser,
    db: DbSession,
    body: ReteachEffectivenessIn,
) -> dict:
    tenant_id = _tenant_id(db, user)

    def _create():
        row = record_reteach_effectiveness(
            db,
            tenant_id=tenant_id,
            user_id=user.id,
            reteach_plan_id=reteach_plan_id,
            **body.model_dump(),
        )
        db.commit()
        return serialize_reteach_effectiveness(row)

    return _handle(_create)


@router.get("/reteach-plans/{reteach_plan_id}/effectiveness")
def read_reteach_effectiveness(reteach_plan_id: uuid.UUID, user: CurrentUser, db: DbSession) -> list[dict]:
    tenant_id = _tenant_id(db, user)
    rows = list_reteach_effectiveness(
        db,
        tenant_id=tenant_id,
        user_id=user.id,
        reteach_plan_id=reteach_plan_id,
    )
    return [serialize_reteach_effectiveness(row) for row in rows]
