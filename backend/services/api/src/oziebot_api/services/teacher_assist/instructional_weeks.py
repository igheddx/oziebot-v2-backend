from __future__ import annotations

from datetime import UTC, datetime
import uuid
from typing import Any

from sqlalchemy import or_, select, update
from sqlalchemy.orm import Session, selectinload

from oziebot_api.models.education_catalog import EducationObjective
from oziebot_api.models.teacher_assist_assignment import TeacherAssistAssignment
from oziebot_api.models.teacher_assist_generated_artifact import TeacherAssistGeneratedArtifact
from oziebot_api.models.teacher_assist_instructional_week import (
    TeacherAssistInstructionalWeek,
    TeacherAssistInstructionalWeekObjective,
)
from oziebot_api.models.teacher_assist_newsletter import TeacherAssistNewsletter
from oziebot_api.models.teacher_assist_pacing_guide import TeacherAssistPacingGuide
from oziebot_api.models.teacher_assist_pacing_guide_period import TeacherAssistPacingGuidePeriod
from oziebot_api.models.teacher_assist_pacing_guide_period_note import TeacherAssistPacingGuidePeriodNote
from oziebot_api.models.teacher_assist_weekly_plan import TeacherAssistWeeklyPlan
from oziebot_api.models.user import User
from oziebot_api.services.teacher_assist.current_week_resolver import CurrentWeekResolver
from oziebot_api.services.teacher_assist.instructional_week_constants import (
    INSTRUCTIONAL_WEEK_STATUSES,
    OBJECTIVE_SOURCE_TYPES,
)
from oziebot_api.services.teacher_assist.pacing_guide_foundation import get_catalog_pacing_guide_detail


def _now() -> datetime:
    return datetime.now(UTC)


def serialize_instructional_week(row: TeacherAssistInstructionalWeek) -> dict[str, Any]:
    return {
        "id": str(row.id),
        "tenant_id": str(row.tenant_id),
        "school_year_id": str(row.school_year_id),
        "grading_period_id": str(row.grading_period_id) if row.grading_period_id else None,
        "pacing_guide_id": str(row.pacing_guide_id),
        "pacing_guide_period_id": str(row.pacing_guide_period_id),
        "week_number": row.week_number,
        "title": row.title,
        "description": row.description,
        "start_date": row.start_date.isoformat() if row.start_date else None,
        "end_date": row.end_date.isoformat() if row.end_date else None,
        "status": row.status,
        "notes": row.notes,
        "created_by_user_id": str(row.created_by_user_id),
        "updated_by_user_id": str(row.updated_by_user_id),
        "created_at": row.created_at.isoformat(),
        "updated_at": row.updated_at.isoformat(),
        "navigation_href": f"/teacher-assist/week/{row.id}",
    }


def serialize_instructional_week_objective(row: TeacherAssistInstructionalWeekObjective) -> dict[str, Any]:
    return {
        "id": str(row.id),
        "instructional_week_id": str(row.instructional_week_id),
        "objective_id": str(row.objective_id) if row.objective_id else None,
        "objective_code": row.objective_code,
        "source_type": row.source_type,
        "is_required": row.is_required,
        "is_active": row.is_active,
        "notes": row.notes,
    }


def _get_period(db: Session, *, tenant_id: uuid.UUID, period_id: uuid.UUID) -> TeacherAssistPacingGuidePeriod:
    period = db.scalars(
        select(TeacherAssistPacingGuidePeriod)
        .join(TeacherAssistPacingGuide, TeacherAssistPacingGuide.id == TeacherAssistPacingGuidePeriod.pacing_guide_id)
        .where(
            TeacherAssistPacingGuidePeriod.id == period_id,
            TeacherAssistPacingGuide.tenant_id == tenant_id,
            TeacherAssistPacingGuidePeriod.period_type == "WEEK",
        )
        .options(selectinload(TeacherAssistPacingGuidePeriod.objectives))
    ).one_or_none()
    if period is None:
        raise LookupError("Pacing guide week not found")
    return period


def _inherit_objectives(
    db: Session,
    *,
    instructional_week_id: uuid.UUID,
    period: TeacherAssistPacingGuidePeriod,
) -> None:
    now = _now()
    for mapping in period.objectives:
        objective = db.get(EducationObjective, mapping.objective_id)
        code = getattr(objective, "objective_id", None) if objective else None
        db.add(
            TeacherAssistInstructionalWeekObjective(
                instructional_week_id=instructional_week_id,
                objective_id=mapping.objective_id,
                objective_code=code,
                source_type="INHERITED",
                is_required=mapping.is_required,
                is_active=True,
                notes=mapping.notes,
                created_at=now,
                updated_at=now,
            )
        )


def _attach_existing_artifacts(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    instructional_week_id: uuid.UUID,
    pacing_guide_period_id: uuid.UUID,
) -> None:
    db.execute(
        update(TeacherAssistWeeklyPlan)
        .where(
            TeacherAssistWeeklyPlan.tenant_id == tenant_id,
            TeacherAssistWeeklyPlan.user_id == user_id,
            TeacherAssistWeeklyPlan.pacing_guide_period_id == pacing_guide_period_id,
            TeacherAssistWeeklyPlan.instructional_week_id.is_(None),
        )
        .values(instructional_week_id=instructional_week_id)
    )
    db.execute(
        update(TeacherAssistAssignment)
        .where(
            TeacherAssistAssignment.tenant_id == tenant_id,
            TeacherAssistAssignment.teacher_user_id == user_id,
            TeacherAssistAssignment.pacing_guide_period_id == pacing_guide_period_id,
            TeacherAssistAssignment.instructional_week_id.is_(None),
        )
        .values(instructional_week_id=instructional_week_id)
    )
    db.execute(
        update(TeacherAssistNewsletter)
        .where(
            TeacherAssistNewsletter.tenant_id == tenant_id,
            TeacherAssistNewsletter.owner_user_id == user_id,
            TeacherAssistNewsletter.pacing_guide_period_id == pacing_guide_period_id,
            TeacherAssistNewsletter.instructional_week_id.is_(None),
        )
        .values(instructional_week_id=instructional_week_id)
    )
    db.execute(
        update(TeacherAssistGeneratedArtifact)
        .where(
            TeacherAssistGeneratedArtifact.tenant_id == tenant_id,
            TeacherAssistGeneratedArtifact.created_by_user_id == user_id,
            TeacherAssistGeneratedArtifact.pacing_guide_period_id == pacing_guide_period_id,
            TeacherAssistGeneratedArtifact.instructional_week_id.is_(None),
        )
        .values(instructional_week_id=instructional_week_id)
    )


def get_instructional_week(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    instructional_week_id: uuid.UUID,
) -> TeacherAssistInstructionalWeek:
    row = db.scalars(
        select(TeacherAssistInstructionalWeek).where(
            TeacherAssistInstructionalWeek.id == instructional_week_id,
            TeacherAssistInstructionalWeek.tenant_id == tenant_id,
            TeacherAssistInstructionalWeek.created_by_user_id == user_id,
        )
        .options(selectinload(TeacherAssistInstructionalWeek.objectives))
    ).one_or_none()
    if row is None:
        raise LookupError("Instructional week not found")
    return row


def find_instructional_week_for_period(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    pacing_guide_period_id: uuid.UUID,
) -> TeacherAssistInstructionalWeek | None:
    return db.scalars(
        select(TeacherAssistInstructionalWeek).where(
            TeacherAssistInstructionalWeek.tenant_id == tenant_id,
            TeacherAssistInstructionalWeek.created_by_user_id == user_id,
            TeacherAssistInstructionalWeek.pacing_guide_period_id == pacing_guide_period_id,
        )
    ).one_or_none()


def resolve_instructional_week_id_for_period(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    pacing_guide_period_id: uuid.UUID,
) -> uuid.UUID | None:
    week = find_instructional_week_for_period(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        pacing_guide_period_id=pacing_guide_period_id,
    )
    return week.id if week else None


def link_entities_to_instructional_week(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    pacing_guide_period_id: uuid.UUID,
    assignment_id: uuid.UUID | None = None,
    newsletter_id: uuid.UUID | None = None,
    weekly_plan_id: uuid.UUID | None = None,
) -> uuid.UUID | None:
    week_id = resolve_instructional_week_id_for_period(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        pacing_guide_period_id=pacing_guide_period_id,
    )
    if week_id is None:
        return None
    if assignment_id is not None:
        assignment = db.get(TeacherAssistAssignment, assignment_id)
        if assignment is not None and assignment.instructional_week_id is None:
            assignment.instructional_week_id = week_id
    if newsletter_id is not None:
        newsletter = db.get(TeacherAssistNewsletter, newsletter_id)
        if newsletter is not None and newsletter.instructional_week_id is None:
            newsletter.instructional_week_id = week_id
    if weekly_plan_id is not None:
        plan = db.get(TeacherAssistWeeklyPlan, weekly_plan_id)
        if plan is not None and plan.instructional_week_id is None:
            plan.instructional_week_id = week_id
    db.flush()
    return week_id


def ensure_instructional_week_for_current_period(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user: User,
    guide_id: uuid.UUID | None,
) -> TeacherAssistInstructionalWeek | None:
    if guide_id is None:
        return None
    from oziebot_api.services.teacher_assist.current_week_resolver import build_current_week_payload

    current = build_current_week_payload(db, tenant_id=tenant_id, user_id=user.id, guide_id=guide_id)
    period_id = (current.get("current_week") or {}).get("id")
    if period_id is None:
        return None
    return create_instructional_week_from_pacing_period(
        db,
        tenant_id=tenant_id,
        user=user,
        pacing_guide_period_id=uuid.UUID(str(period_id)),
        status="DRAFT",
    )


def create_instructional_week_from_pacing_period(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user: User,
    pacing_guide_period_id: uuid.UUID,
    status: str = "DRAFT",
) -> TeacherAssistInstructionalWeek:
    if status not in INSTRUCTIONAL_WEEK_STATUSES:
        raise ValueError("Unsupported instructional week status")
    existing = find_instructional_week_for_period(
        db,
        tenant_id=tenant_id,
        user_id=user.id,
        pacing_guide_period_id=pacing_guide_period_id,
    )
    if existing is not None:
        return existing

    period = _get_period(db, tenant_id=tenant_id, period_id=pacing_guide_period_id)
    guide = db.get(TeacherAssistPacingGuide, period.pacing_guide_id)
    if guide is None:
        raise LookupError("Pacing guide not found")

    resolved = CurrentWeekResolver.resolve(db, tenant_id=tenant_id, user_id=user.id)
    grading_period_id = resolved.grading_period.id if resolved.grading_period else None
    school_year_id = guide.school_year_id
    if resolved.school_year is not None:
        school_year_id = resolved.school_year.id

    note = db.scalars(
        select(TeacherAssistPacingGuidePeriodNote).where(
            TeacherAssistPacingGuidePeriodNote.tenant_id == tenant_id,
            TeacherAssistPacingGuidePeriodNote.user_id == user.id,
            TeacherAssistPacingGuidePeriodNote.period_id == period.id,
        )
    ).one_or_none()

    now = _now()
    row = TeacherAssistInstructionalWeek(
        tenant_id=tenant_id,
        school_year_id=school_year_id,
        grading_period_id=grading_period_id,
        pacing_guide_id=guide.id,
        pacing_guide_period_id=period.id,
        week_number=period.sequence_number,
        title=period.title,
        description=period.description,
        start_date=period.start_date,
        end_date=period.end_date,
        status=status,
        notes=note.notes if note else None,
        created_by_user_id=user.id,
        updated_by_user_id=user.id,
        created_at=now,
        updated_at=now,
    )
    db.add(row)
    db.flush()
    _inherit_objectives(db, instructional_week_id=row.id, period=period)
    _attach_existing_artifacts(
        db,
        tenant_id=tenant_id,
        user_id=user.id,
        instructional_week_id=row.id,
        pacing_guide_period_id=period.id,
    )
    db.refresh(row)
    return row


def list_instructional_weeks(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    school_year_id: uuid.UUID | None = None,
    pacing_guide_id: uuid.UUID | None = None,
    status: str | None = None,
) -> list[TeacherAssistInstructionalWeek]:
    query = select(TeacherAssistInstructionalWeek).where(
        TeacherAssistInstructionalWeek.tenant_id == tenant_id,
        TeacherAssistInstructionalWeek.created_by_user_id == user_id,
    )
    if school_year_id is not None:
        query = query.where(TeacherAssistInstructionalWeek.school_year_id == school_year_id)
    if pacing_guide_id is not None:
        query = query.where(TeacherAssistInstructionalWeek.pacing_guide_id == pacing_guide_id)
    if status is not None:
        query = query.where(TeacherAssistInstructionalWeek.status == status.upper())
    return list(db.scalars(query.order_by(TeacherAssistInstructionalWeek.week_number.asc())).all())


def update_instructional_week(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    instructional_week_id: uuid.UUID,
    status: str | None = None,
    notes: str | None = None,
    title: str | None = None,
    description: str | None = None,
) -> TeacherAssistInstructionalWeek:
    row = get_instructional_week(db, tenant_id=tenant_id, user_id=user_id, instructional_week_id=instructional_week_id)
    if status is not None:
        if status.upper() not in INSTRUCTIONAL_WEEK_STATUSES:
            raise ValueError("Unsupported instructional week status")
        row.status = status.upper()
    if notes is not None:
        row.notes = notes
    if title is not None:
        row.title = title.strip()
    if description is not None:
        row.description = description
    row.updated_by_user_id = user_id
    row.updated_at = _now()
    db.flush()
    return row


def upsert_instructional_week_objective(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    instructional_week_id: uuid.UUID,
    objective_id: uuid.UUID | None = None,
    objective_code: str | None = None,
    source_type: str = "ADDED",
    is_required: bool = True,
    notes: str | None = None,
) -> TeacherAssistInstructionalWeekObjective:
    if source_type not in OBJECTIVE_SOURCE_TYPES:
        raise ValueError("Unsupported objective source type")
    get_instructional_week(db, tenant_id=tenant_id, user_id=user_id, instructional_week_id=instructional_week_id)
    code = objective_code
    if objective_id is not None and not code:
        objective = db.get(EducationObjective, objective_id)
        code = getattr(objective, "objective_id", None) if objective else None
    now = _now()
    row = TeacherAssistInstructionalWeekObjective(
        instructional_week_id=instructional_week_id,
        objective_id=objective_id,
        objective_code=code,
        source_type=source_type.upper(),
        is_required=is_required,
        is_active=True,
        notes=notes,
        created_at=now,
        updated_at=now,
    )
    db.add(row)
    db.flush()
    return row


def deactivate_instructional_week_objective(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    instructional_week_id: uuid.UUID,
    objective_row_id: uuid.UUID,
) -> TeacherAssistInstructionalWeekObjective:
    get_instructional_week(db, tenant_id=tenant_id, user_id=user_id, instructional_week_id=instructional_week_id)
    row = db.scalars(
        select(TeacherAssistInstructionalWeekObjective).where(
            TeacherAssistInstructionalWeekObjective.id == objective_row_id,
            TeacherAssistInstructionalWeekObjective.instructional_week_id == instructional_week_id,
        )
    ).one_or_none()
    if row is None:
        raise LookupError("Instructional week objective not found")
    row.is_active = False
    row.updated_at = _now()
    db.flush()
    return row


def preview_instructional_week_from_pacing_period(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user: User,
    pacing_guide_period_id: uuid.UUID,
) -> dict[str, Any]:
    period = _get_period(db, tenant_id=tenant_id, period_id=pacing_guide_period_id)
    guide = db.get(TeacherAssistPacingGuide, period.pacing_guide_id)
    detail = get_catalog_pacing_guide_detail(db, tenant_id=tenant_id, pacing_guide_id=period.pacing_guide_id)
    from oziebot_api.services.teacher_assist.recommendation_service import build_week_recommendations

    detail_period = next((row for row in detail.periods if row.id == period.id), None)
    resources = []
    if detail_period is not None:
        resources = [
            {
                "catalog_resource_id": str(row.catalog_resource_id) if row.catalog_resource_id else None,
                "resource_library_item_id": str(row.resource_library_item_id) if row.resource_library_item_id else None,
            }
            for row in detail_period.resources
        ]
    recommendations = build_week_recommendations(
        db, tenant_id=tenant_id, user=user, period_id=pacing_guide_period_id
    )
    return {
        "pacing_guide_period_id": str(period.id),
        "pacing_guide_id": str(period.pacing_guide_id),
        "pacing_guide_title": guide.title if guide else None,
        "week_number": period.sequence_number,
        "title": period.title,
        "description": period.description,
        "start_date": period.start_date.isoformat() if period.start_date else None,
        "end_date": period.end_date.isoformat() if period.end_date else None,
        "objectives": [
            {
                "objective_id": str(row.objective_id),
                "objective_code": getattr(getattr(row, "objective", None), "objective_id", None),
                "source_type": "INHERITED",
            }
            for row in period.objectives
        ],
        "resources": resources,
        "recommended_reuse": recommendations.get("recommended_for_this_week", {}).get("top_reusable", []),
        "requires_review": True,
    }
