"""TeacherAssist pilot readiness API — completion review, feedback, metrics, health."""

from __future__ import annotations

import uuid

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from oziebot_api.config import get_settings
from oziebot_api.deps import DbSession
from oziebot_api.deps.auth import CurrentUser
from oziebot_api.services.teacher_assist.pilot_feedback import (
    create_pilot_feedback,
    list_pilot_feedback,
    serialize_pilot_feedback,
    update_pilot_feedback_status,
)
from oziebot_api.services.teacher_assist.pilot_seed_validation import validate_pilot_seed_data
from oziebot_api.services.teacher_assist.product_completion_review import (
    build_product_completion_review,
)
from oziebot_api.services.teacher_assist.system_health_dashboard import (
    build_system_health_dashboard,
)
from oziebot_api.services.teacher_assist.usage_metrics import (
    build_usage_metrics_snapshot,
    record_teacher_login,
)


router = APIRouter(prefix="/teacher-assist/pilot", tags=["teacher_assist_pilot"])


def _tenant_id(db, user) -> uuid.UUID:
    from oziebot_api.api.v1.teacher_assist_pacing_guides import _tenant_id as pacing_tenant_id

    return pacing_tenant_id(db, user)


def _handle(fn):
    try:
        return fn()
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


class PilotFeedbackCreateIn(BaseModel):
    category: str
    severity: str
    feature_area: str = Field(min_length=1, max_length=128)
    description: str = Field(min_length=1)
    requested_improvement: str | None = None


class PilotFeedbackStatusIn(BaseModel):
    status: str


@router.get("/completion-review")
def read_completion_review(user: CurrentUser) -> dict:
    return build_product_completion_review()


@router.get("/seed-validation")
def read_seed_validation(user: CurrentUser, db: DbSession) -> dict:
    if not user.is_root_admin:
        raise HTTPException(
            status_code=403, detail="Seed validation is restricted to root administrators"
        )
    return validate_pilot_seed_data(db)


@router.get("/usage-metrics")
def read_usage_metrics(
    user: CurrentUser, db: DbSession, days: int = Query(default=30, ge=1, le=365)
) -> dict:
    tenant_id = _tenant_id(db, user)
    return build_usage_metrics_snapshot(db, tenant_id=tenant_id, days=days)


@router.post("/usage-metrics/login", status_code=204)
def post_login_metric(user: CurrentUser, db: DbSession) -> None:
    tenant_id = _tenant_id(db, user)

    def _record():
        record_teacher_login(db, tenant_id=tenant_id, user_id=user.id)
        db.commit()

    _handle(_record)


@router.get("/feedback")
def read_pilot_feedback(
    user: CurrentUser,
    db: DbSession,
    mine_only: bool = True,
    status: str | None = None,
) -> list[dict]:
    tenant_id = _tenant_id(db, user)
    rows = list_pilot_feedback(
        db,
        tenant_id=tenant_id,
        user_id=user.id if mine_only and not user.is_root_admin else None,
        status=status,
    )
    return [serialize_pilot_feedback(row) for row in rows]


@router.post("/feedback", status_code=201)
def create_pilot_feedback_route(
    user: CurrentUser, db: DbSession, body: PilotFeedbackCreateIn
) -> dict:
    tenant_id = _tenant_id(db, user)

    def _create():
        row = create_pilot_feedback(
            db,
            tenant_id=tenant_id,
            user_id=user.id,
            category=body.category,
            severity=body.severity,
            feature_area=body.feature_area,
            description=body.description,
            requested_improvement=body.requested_improvement,
        )
        db.commit()
        return serialize_pilot_feedback(row)

    return _handle(_create)


@router.patch("/feedback/{feedback_id}")
def patch_pilot_feedback(
    feedback_id: uuid.UUID,
    user: CurrentUser,
    db: DbSession,
    body: PilotFeedbackStatusIn,
) -> dict:
    tenant_id = _tenant_id(db, user)

    def _update():
        row = update_pilot_feedback_status(
            db,
            tenant_id=tenant_id,
            feedback_id=feedback_id,
            status=body.status,
            allow_any_user=user.is_root_admin,
            user_id=user.id,
        )
        db.commit()
        return serialize_pilot_feedback(row)

    return _handle(_update)


@router.get("/system-health")
def read_system_health(user: CurrentUser, db: DbSession) -> dict:
    if not user.is_root_admin:
        raise HTTPException(
            status_code=403, detail="System health dashboard is restricted to root administrators"
        )
    settings = get_settings()
    return build_system_health_dashboard(db, settings=settings)
