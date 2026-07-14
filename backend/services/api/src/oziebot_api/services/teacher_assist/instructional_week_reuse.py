from __future__ import annotations

import uuid
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from oziebot_api.models.teacher_assist_instructional_week import TeacherAssistInstructionalWeek
from oziebot_api.models.user import User
from oziebot_api.services.teacher_assist.constants import instructional_week_href
from oziebot_api.services.teacher_assist.generate_next_week import generate_next_week_draft
from oziebot_api.services.teacher_assist.instructional_weeks import (
    create_instructional_week_from_pacing_period,
    find_instructional_week_for_period,
    get_instructional_week,
)


def generate_next_instructional_week(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user: User,
    instructional_week_id: uuid.UUID,
) -> dict[str, Any]:
    current = get_instructional_week(
        db, tenant_id=tenant_id, user_id=user.id, instructional_week_id=instructional_week_id
    )
    draft = generate_next_week_draft(
        db, tenant_id=tenant_id, user=user, period_id=current.pacing_guide_period_id
    )
    next_period_id = uuid.UUID(str(draft["next_period_id"]))
    next_week = create_instructional_week_from_pacing_period(
        db,
        tenant_id=tenant_id,
        user=user,
        pacing_guide_period_id=next_period_id,
        status="DRAFT",
    )
    return {
        **draft,
        "instructional_week_id": str(next_week.id),
        "navigation_href": instructional_week_href(str(next_week.id)),
    }


def reuse_prior_instructional_week(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user: User,
    target_instructional_week_id: uuid.UUID,
    source_instructional_week_id: uuid.UUID,
    copy_objectives: bool = True,
    copy_assignments: bool = False,
    copy_assessments: bool = False,
    copy_lessons: bool = False,
    copy_resources: bool = True,
    copy_newsletters: bool = False,
) -> dict[str, Any]:
    target = get_instructional_week(
        db, tenant_id=tenant_id, user_id=user.id, instructional_week_id=target_instructional_week_id
    )
    source = db.scalars(
        select(TeacherAssistInstructionalWeek)
        .where(
            TeacherAssistInstructionalWeek.id == source_instructional_week_id,
            TeacherAssistInstructionalWeek.tenant_id == tenant_id,
        )
        .options(selectinload(TeacherAssistInstructionalWeek.objectives))
    ).one_or_none()
    if source is None:
        raise LookupError("Source instructional week not found")

    copied: dict[str, Any] = {"objectives": 0, "notes_applied": False}
    if copy_objectives:
        for row in source.objectives:
            if not row.is_active:
                continue
            from oziebot_api.services.teacher_assist.instructional_weeks import (
                upsert_instructional_week_objective,
            )

            upsert_instructional_week_objective(
                db,
                tenant_id=tenant_id,
                user_id=user.id,
                instructional_week_id=target.id,
                objective_id=row.objective_id,
                objective_code=row.objective_code,
                source_type="INHERITED",
                is_required=row.is_required,
                notes=row.notes,
            )
            copied["objectives"] += 1
    if copy_resources and source.notes:
        target.notes = source.notes
        copied["notes_applied"] = True

    if copy_assignments or copy_assessments or copy_lessons or copy_newsletters:
        from oziebot_api.services.teacher_assist.week_duplication import duplicate_week

        if source.pacing_guide_period_id != target.pacing_guide_period_id:
            duplicate_week(
                db,
                tenant_id=tenant_id,
                user=user,
                source_period_id=source.pacing_guide_period_id,
                target_period_id=target.pacing_guide_period_id,
                copy_objectives=False,
                copy_resources=copy_resources,
                copy_notes=True,
                copy_artifacts=copy_assignments
                or copy_assessments
                or copy_lessons
                or copy_newsletters,
            )
            copied["artifacts_duplicated"] = True

    return {
        "target_instructional_week_id": str(target.id),
        "source_instructional_week_id": str(source.id),
        "copied": copied,
        "navigation_href": instructional_week_href(str(target.id)),
    }


def resolve_or_create_for_period(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user: User,
    pacing_guide_period_id: uuid.UUID,
) -> TeacherAssistInstructionalWeek | None:
    existing = find_instructional_week_for_period(
        db,
        tenant_id=tenant_id,
        user_id=user.id,
        pacing_guide_period_id=pacing_guide_period_id,
    )
    if existing is not None:
        return existing
    return None
