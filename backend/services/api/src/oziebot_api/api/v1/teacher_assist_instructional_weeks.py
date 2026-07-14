"""Instructional week routes — week-centric teacher operating system."""

from __future__ import annotations

import uuid

from fastapi import APIRouter, HTTPException

from oziebot_api.deps import DbSession
from oziebot_api.deps.auth import CurrentUser
from oziebot_api.schemas.instructional_week import (
    InstructionalWeekCreateIn,
    InstructionalWeekObjectiveIn,
    InstructionalWeekReuseIn,
    InstructionalWeekSnapshotIn,
    InstructionalWeekUpdateIn,
)
from oziebot_api.services.teacher_assist.instructional_week_reuse import (
    generate_next_instructional_week,
    reuse_prior_instructional_week,
)
from oziebot_api.services.teacher_assist.instructional_week_snapshots import (
    create_instructional_week_snapshot,
    serialize_snapshot,
)
from oziebot_api.services.teacher_assist.instructional_week_workspace import (
    build_instructional_week_workspace,
)
from oziebot_api.services.teacher_assist.instructional_weeks import (
    create_instructional_week_from_pacing_period,
    deactivate_instructional_week_objective,
    find_instructional_week_for_period,
    get_instructional_week,
    list_instructional_weeks,
    preview_instructional_week_from_pacing_period,
    serialize_instructional_week,
    serialize_instructional_week_objective,
    update_instructional_week,
    upsert_instructional_week_objective,
)
from oziebot_api.models.teacher_assist_instructional_week import (
    TeacherAssistInstructionalWeekSnapshot,
)
from sqlalchemy import select


router = APIRouter(prefix="/teacher-assist", tags=["teacher_assist_instructional_weeks"])


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


@router.get("/instructional-weeks")
def read_instructional_weeks(
    user: CurrentUser,
    db: DbSession,
    school_year_id: uuid.UUID | None = None,
    pacing_guide_id: uuid.UUID | None = None,
    status: str | None = None,
) -> list[dict]:
    tenant_id = _tenant_id(db, user)
    rows = list_instructional_weeks(
        db,
        tenant_id=tenant_id,
        user_id=user.id,
        school_year_id=school_year_id,
        pacing_guide_id=pacing_guide_id,
        status=status,
    )
    return [serialize_instructional_week(row) for row in rows]


@router.get("/instructional-weeks/by-period/{period_id}")
def read_instructional_week_by_period(
    period_id: uuid.UUID, user: CurrentUser, db: DbSession
) -> dict:
    tenant_id = _tenant_id(db, user)
    row = find_instructional_week_for_period(
        db, tenant_id=tenant_id, user_id=user.id, pacing_guide_period_id=period_id
    )
    if row is None:
        raise HTTPException(
            status_code=404, detail="Instructional week not found for pacing period"
        )
    return serialize_instructional_week(row)


@router.get("/instructional-weeks/{instructional_week_id}")
def read_instructional_week(
    instructional_week_id: uuid.UUID, user: CurrentUser, db: DbSession
) -> dict:
    tenant_id = _tenant_id(db, user)
    row = _handle(
        lambda: get_instructional_week(
            db, tenant_id=tenant_id, user_id=user.id, instructional_week_id=instructional_week_id
        )
    )
    return serialize_instructional_week(row)


@router.get("/instructional-weeks/{instructional_week_id}/workspace")
def read_instructional_week_workspace(
    instructional_week_id: uuid.UUID, user: CurrentUser, db: DbSession
) -> dict:
    tenant_id = _tenant_id(db, user)
    return _handle(
        lambda: build_instructional_week_workspace(
            db, tenant_id=tenant_id, user=user, instructional_week_id=instructional_week_id
        )
    )


@router.get("/pacing-guide-periods/{period_id}/instructional-week-preview")
def preview_instructional_week(period_id: uuid.UUID, user: CurrentUser, db: DbSession) -> dict:
    tenant_id = _tenant_id(db, user)
    return _handle(
        lambda: preview_instructional_week_from_pacing_period(
            db, tenant_id=tenant_id, user=user, pacing_guide_period_id=period_id
        )
    )


@router.post("/pacing-guide-periods/{period_id}/instructional-weeks", status_code=201)
def create_instructional_week(
    period_id: uuid.UUID,
    body: InstructionalWeekCreateIn,
    user: CurrentUser,
    db: DbSession,
) -> dict:
    tenant_id = _tenant_id(db, user)
    row = _handle(
        lambda: create_instructional_week_from_pacing_period(
            db,
            tenant_id=tenant_id,
            user=user,
            pacing_guide_period_id=period_id,
            status=body.status,
        )
    )
    return serialize_instructional_week(row)


@router.patch("/instructional-weeks/{instructional_week_id}")
def patch_instructional_week(
    instructional_week_id: uuid.UUID,
    body: InstructionalWeekUpdateIn,
    user: CurrentUser,
    db: DbSession,
) -> dict:
    tenant_id = _tenant_id(db, user)
    row = _handle(
        lambda: update_instructional_week(
            db,
            tenant_id=tenant_id,
            user_id=user.id,
            instructional_week_id=instructional_week_id,
            status=body.status,
            notes=body.notes,
            title=body.title,
            description=body.description,
        )
    )
    return serialize_instructional_week(row)


@router.post("/instructional-weeks/{instructional_week_id}/objectives", status_code=201)
def add_instructional_week_objective(
    instructional_week_id: uuid.UUID,
    body: InstructionalWeekObjectiveIn,
    user: CurrentUser,
    db: DbSession,
) -> dict:
    tenant_id = _tenant_id(db, user)
    row = _handle(
        lambda: upsert_instructional_week_objective(
            db,
            tenant_id=tenant_id,
            user_id=user.id,
            instructional_week_id=instructional_week_id,
            objective_id=body.objective_id,
            objective_code=body.objective_code,
            source_type=body.source_type,
            is_required=body.is_required,
            notes=body.notes,
        )
    )
    return serialize_instructional_week_objective(row)


@router.delete(
    "/instructional-weeks/{instructional_week_id}/objectives/{objective_row_id}", status_code=200
)
def remove_instructional_week_objective(
    instructional_week_id: uuid.UUID,
    objective_row_id: uuid.UUID,
    user: CurrentUser,
    db: DbSession,
) -> dict:
    tenant_id = _tenant_id(db, user)
    row = _handle(
        lambda: deactivate_instructional_week_objective(
            db,
            tenant_id=tenant_id,
            user_id=user.id,
            instructional_week_id=instructional_week_id,
            objective_row_id=objective_row_id,
        )
    )
    return serialize_instructional_week_objective(row)


@router.post("/instructional-weeks/{instructional_week_id}/generate-next-week")
def generate_next_week_route(
    instructional_week_id: uuid.UUID, user: CurrentUser, db: DbSession
) -> dict:
    tenant_id = _tenant_id(db, user)
    return _handle(
        lambda: generate_next_instructional_week(
            db, tenant_id=tenant_id, user=user, instructional_week_id=instructional_week_id
        )
    )


@router.post("/instructional-weeks/{instructional_week_id}/reuse", status_code=200)
def reuse_instructional_week_route(
    instructional_week_id: uuid.UUID,
    body: InstructionalWeekReuseIn,
    user: CurrentUser,
    db: DbSession,
) -> dict:
    tenant_id = _tenant_id(db, user)
    return _handle(
        lambda: reuse_prior_instructional_week(
            db,
            tenant_id=tenant_id,
            user=user,
            target_instructional_week_id=instructional_week_id,
            source_instructional_week_id=body.source_instructional_week_id,
            copy_objectives=body.copy_objectives,
            copy_assignments=body.copy_assignments,
            copy_assessments=body.copy_assessments,
            copy_lessons=body.copy_lessons,
            copy_resources=body.copy_resources,
            copy_newsletters=body.copy_newsletters,
        )
    )


@router.post("/instructional-weeks/{instructional_week_id}/snapshots", status_code=201)
def create_snapshot_route(
    instructional_week_id: uuid.UUID,
    body: InstructionalWeekSnapshotIn,
    user: CurrentUser,
    db: DbSession,
) -> dict:
    tenant_id = _tenant_id(db, user)
    row = _handle(
        lambda: create_instructional_week_snapshot(
            db,
            tenant_id=tenant_id,
            user=user,
            instructional_week_id=instructional_week_id,
            name=body.name,
        )
    )
    return serialize_snapshot(row)


@router.get("/instructional-weeks/{instructional_week_id}/snapshots")
def list_snapshots_route(
    instructional_week_id: uuid.UUID, user: CurrentUser, db: DbSession
) -> list[dict]:
    tenant_id = _tenant_id(db, user)
    _handle(
        lambda: get_instructional_week(
            db, tenant_id=tenant_id, user_id=user.id, instructional_week_id=instructional_week_id
        )
    )
    rows = list(
        db.scalars(
            select(TeacherAssistInstructionalWeekSnapshot)
            .where(
                TeacherAssistInstructionalWeekSnapshot.instructional_week_id
                == instructional_week_id
            )
            .order_by(TeacherAssistInstructionalWeekSnapshot.created_at.desc())
        ).all()
    )
    return [serialize_snapshot(row) for row in rows]
