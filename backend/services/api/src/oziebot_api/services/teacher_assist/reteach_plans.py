from __future__ import annotations

from datetime import UTC, datetime
import uuid
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from oziebot_api.models.teacher_assist_reteach_plan import TeacherAssistReteachPlan
from oziebot_api.models.teacher_assist_reteach_plan_version import TeacherAssistReteachPlanVersion
from oziebot_api.services.teacher_assist.activity_events import record_activity_event
from oziebot_api.services.teacher_assist.constants import (
    validate_reteach_plan_status,
    validate_reteach_plan_version_source,
)
from oziebot_api.services.teacher_assist.mastery_matrix import (
    get_mastery_matrix_or_404,
    get_matrix_standard_or_404,
)
from oziebot_api.services.teacher_assist.reteach_insights import build_mastery_matrix_reteach_insights


def _normalize_title(value: str) -> str:
    normalized = value.strip()
    if not normalized:
        raise ValueError("Reteach plan title is required")
    return normalized


def get_reteach_plan_or_404(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    reteach_plan_id: uuid.UUID,
    load_versions: bool = False,
) -> TeacherAssistReteachPlan:
    query = select(TeacherAssistReteachPlan).where(
        TeacherAssistReteachPlan.id == reteach_plan_id,
        TeacherAssistReteachPlan.tenant_id == tenant_id,
        TeacherAssistReteachPlan.owner_user_id == user_id,
    )
    if load_versions:
        query = query.options(
            selectinload(TeacherAssistReteachPlan.versions),
            selectinload(TeacherAssistReteachPlan.standard),
            selectinload(TeacherAssistReteachPlan.current_version),
        )
    else:
        query = query.options(selectinload(TeacherAssistReteachPlan.standard))
    row = db.scalars(query).one_or_none()
    if row is None:
        raise LookupError("Reteach plan not found")
    return row


def list_reteach_plans(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    mastery_matrix_id: uuid.UUID | None = None,
    standard_id: uuid.UUID | None = None,
    status: str | None = None,
) -> list[TeacherAssistReteachPlan]:
    query = select(TeacherAssistReteachPlan).where(
        TeacherAssistReteachPlan.tenant_id == tenant_id,
        TeacherAssistReteachPlan.owner_user_id == user_id,
    )
    if mastery_matrix_id is not None:
        query = query.where(TeacherAssistReteachPlan.mastery_matrix_id == mastery_matrix_id)
    if standard_id is not None:
        query = query.where(TeacherAssistReteachPlan.standard_id == standard_id)
    if status is not None:
        query = query.where(TeacherAssistReteachPlan.status == validate_reteach_plan_status(status))
    return db.scalars(
        query.order_by(TeacherAssistReteachPlan.updated_at.desc(), TeacherAssistReteachPlan.created_at.desc())
    ).all()


def create_reteach_plan(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    mastery_matrix_id: uuid.UUID,
    standard_id: uuid.UUID,
    title: str | None = None,
) -> TeacherAssistReteachPlan:
    matrix = get_mastery_matrix_or_404(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        mastery_matrix_id=mastery_matrix_id,
        load_standards=True,
    )
    get_matrix_standard_or_404(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        mastery_matrix_id=mastery_matrix_id,
        standard_id=standard_id,
    )
    from oziebot_api.models.teacher_assist_standard import TeacherAssistStandard

    standard = db.get(TeacherAssistStandard, standard_id)
    standard_code = standard.code if standard else "Standard"
    normalized_title = _normalize_title(title or f"Reteach Plan — {standard_code}")
    now = datetime.now(UTC)
    plan = TeacherAssistReteachPlan(
        tenant_id=tenant_id,
        owner_user_id=user_id,
        mastery_matrix_id=matrix.id,
        standard_id=standard_id,
        school_year_id=matrix.school_year_id,
        grading_period_id=matrix.grading_period_id,
        class_id=matrix.class_id,
        subject_id=matrix.subject_id,
        title=normalized_title,
        status=validate_reteach_plan_status("draft"),
        created_at=now,
        updated_at=now,
    )
    db.add(plan)
    db.flush()

    record_activity_event(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        event_type="reteach_plan_created",
        event_category="planning",
        entity_type="reteach_plan",
        entity_id=plan.id,
        school_year_id=plan.school_year_id,
        grading_period_id=plan.grading_period_id,
        class_id=plan.class_id,
        subject_id=plan.subject_id,
        summary_text=f"Created reteach plan draft for {standard_code}.",
        details_json={
            "mastery_matrix_id": str(matrix.id),
            "standard_id": str(standard_id),
            "status": plan.status,
        },
    )
    db.flush()
    db.refresh(plan)
    return plan


def _next_version_number(db: Session, *, reteach_plan_id: uuid.UUID) -> int:
    latest = db.scalar(
        select(TeacherAssistReteachPlanVersion.version_number)
        .where(TeacherAssistReteachPlanVersion.reteach_plan_id == reteach_plan_id)
        .order_by(TeacherAssistReteachPlanVersion.version_number.desc())
        .limit(1)
    )
    return int(latest or 0) + 1


def create_reteach_plan_version(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    reteach_plan: TeacherAssistReteachPlan,
    content_json: dict[str, Any],
    version_source: str,
    prompt_context_json: dict[str, Any] | None = None,
    provider_name: str | None = None,
    provider_model: str | None = None,
    prompt_version: str | None = None,
    ai_usage_event_id: uuid.UUID | None = None,
    change_reason: str | None = None,
) -> TeacherAssistReteachPlanVersion:
    now = datetime.now(UTC)
    version = TeacherAssistReteachPlanVersion(
        tenant_id=tenant_id,
        owner_user_id=user_id,
        reteach_plan_id=reteach_plan.id,
        version_number=_next_version_number(db, reteach_plan_id=reteach_plan.id),
        version_source=validate_reteach_plan_version_source(version_source),
        content_json=content_json,
        prompt_context_json=prompt_context_json,
        provider_name=provider_name,
        provider_model=provider_model,
        prompt_version=prompt_version,
        ai_usage_event_id=ai_usage_event_id,
        created_by_user_id=user_id,
        change_reason=change_reason,
        created_at=now,
    )
    db.add(version)
    db.flush()
    reteach_plan.current_version_id = version.id
    reteach_plan.updated_at = now
    if version_source == "ai_draft":
        reteach_plan.status = validate_reteach_plan_status("ai_draft")
        reteach_plan.latest_ai_usage_event_id = ai_usage_event_id
    elif version_source == "teacher_edit":
        reteach_plan.status = validate_reteach_plan_status("teacher_review")
    db.flush()
    db.refresh(version)
    return version


def update_reteach_plan(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    reteach_plan_id: uuid.UUID,
    title: str | None = None,
    status: str | None = None,
) -> TeacherAssistReteachPlan:
    plan = get_reteach_plan_or_404(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        reteach_plan_id=reteach_plan_id,
    )
    if title is not None:
        plan.title = _normalize_title(title)
    if status is not None:
        normalized_status = validate_reteach_plan_status(status)
        if normalized_status not in {"teacher_review", "archived", "draft", "ai_draft"}:
            raise ValueError("Unsupported reteach plan status transition")
        plan.status = normalized_status
    plan.updated_at = datetime.now(UTC)
    db.flush()
    db.refresh(plan)
    return plan


def create_teacher_reteach_plan_version(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    reteach_plan_id: uuid.UUID,
    content_json: dict[str, Any],
    change_reason: str | None = None,
) -> TeacherAssistReteachPlanVersion:
    plan = get_reteach_plan_or_404(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        reteach_plan_id=reteach_plan_id,
    )
    version = create_reteach_plan_version(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        reteach_plan=plan,
        content_json=content_json,
        version_source="teacher_edit",
        change_reason=change_reason,
    )
    record_activity_event(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        event_type="reteach_plan_version_created",
        event_category="planning",
        entity_type="reteach_plan",
        entity_id=plan.id,
        school_year_id=plan.school_year_id,
        grading_period_id=plan.grading_period_id,
        class_id=plan.class_id,
        subject_id=plan.subject_id,
        summary_text=f"Teacher saved reteach plan version {version.version_number}.",
        details_json={
            "reteach_plan_id": str(plan.id),
            "version_id": str(version.id),
            "version_source": version.version_source,
        },
    )
    db.flush()
    return version


def list_reteach_plan_versions(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    reteach_plan_id: uuid.UUID,
) -> list[TeacherAssistReteachPlanVersion]:
    get_reteach_plan_or_404(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        reteach_plan_id=reteach_plan_id,
    )
    return db.scalars(
        select(TeacherAssistReteachPlanVersion)
        .where(
            TeacherAssistReteachPlanVersion.tenant_id == tenant_id,
            TeacherAssistReteachPlanVersion.owner_user_id == user_id,
            TeacherAssistReteachPlanVersion.reteach_plan_id == reteach_plan_id,
        )
        .order_by(TeacherAssistReteachPlanVersion.version_number.asc())
    ).all()


def _json_safe_value(value: Any) -> Any:
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: _json_safe_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe_value(item) for item in value]
    return value


def build_reteach_plan_prompt_context(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    reteach_plan: TeacherAssistReteachPlan,
    settings=None,
) -> dict[str, Any]:
    from oziebot_api.config import Settings
    from oziebot_api.services.teacher_assist.mastery_commit_service import list_mastery_evaluations

    settings = settings or Settings()
    insights = build_mastery_matrix_reteach_insights(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        mastery_matrix_id=reteach_plan.mastery_matrix_id,
        settings=settings,
    )
    standard_insight = next(
        (item for item in insights["standard_insights"] if item["standard_id"] == reteach_plan.standard_id),
        None,
    )
    evaluations = list_mastery_evaluations(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        mastery_matrix_id=reteach_plan.mastery_matrix_id,
        standard_id=reteach_plan.standard_id,
        evaluation_status="active",
    )
    student_summaries = [
        {
            "student_number": row.student_number,
            "mastery_level": row.mastery_level,
            "confirmed_at": row.confirmed_at.isoformat() if row.confirmed_at else None,
        }
        for row in evaluations
    ]
    distribution: dict[str, int] = {}
    for row in evaluations:
        distribution[row.mastery_level] = distribution.get(row.mastery_level, 0) + 1

    return _json_safe_value(
        {
            "mastery_matrix_id": str(reteach_plan.mastery_matrix_id),
            "standard_id": str(reteach_plan.standard_id),
            "class_id": str(reteach_plan.class_id),
            "subject_id": str(reteach_plan.subject_id),
            "standard_insight": standard_insight,
            "student_summaries": student_summaries,
            "mastery_distribution": distribution,
            "insight_panels": {
                "standards_needing_reteach_count": len(insights["panels"]["standards_needing_reteach"]),
            },
            "anonymous_only": True,
            "pii_policy": "STUDENT_NUMBER_ONLY",
        }
    )


def serialize_reteach_plan(plan: TeacherAssistReteachPlan) -> dict[str, Any]:
    return {
        "id": plan.id,
        "tenant_id": plan.tenant_id,
        "owner_user_id": plan.owner_user_id,
        "mastery_matrix_id": plan.mastery_matrix_id,
        "standard_id": plan.standard_id,
        "school_year_id": plan.school_year_id,
        "grading_period_id": plan.grading_period_id,
        "class_id": plan.class_id,
        "subject_id": plan.subject_id,
        "title": plan.title,
        "status": plan.status,
        "instructional_week_id": plan.instructional_week_id,
        "objective_id": plan.objective_id,
        "reason": plan.reason,
        "expected_outcome": plan.expected_outcome,
        "current_version_id": plan.current_version_id,
        "latest_ai_usage_event_id": plan.latest_ai_usage_event_id,
        "created_at": plan.created_at,
        "updated_at": plan.updated_at,
        "standard_code": plan.standard.code if plan.standard else None,
        "standard_description": plan.standard.description if plan.standard else None,
    }


def serialize_reteach_plan_version(version: TeacherAssistReteachPlanVersion) -> dict[str, Any]:
    return {
        "id": version.id,
        "reteach_plan_id": version.reteach_plan_id,
        "version_number": version.version_number,
        "version_source": version.version_source,
        "content_json": version.content_json,
        "prompt_context_json": version.prompt_context_json,
        "provider_name": version.provider_name,
        "provider_model": version.provider_model,
        "prompt_version": version.prompt_version,
        "ai_usage_event_id": version.ai_usage_event_id,
        "created_by_user_id": version.created_by_user_id,
        "change_reason": version.change_reason,
        "created_at": version.created_at,
    }
