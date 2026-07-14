from __future__ import annotations

from datetime import UTC, datetime
import uuid
from typing import Any

from sqlalchemy import or_, select
from sqlalchemy.orm import Session

from oziebot_api.models.teacher_assist_time_savings import TeacherAssistWeekTemplate
from oziebot_api.models.user import User
from oziebot_api.services.teacher_assist.reuse_events import record_reuse_event
from oziebot_api.services.teacher_assist.time_savings_constants import (
    TEMPLATE_TYPES,
    TEMPLATE_VISIBILITY,
    TIME_SAVINGS_MINUTES,
)
from oziebot_api.services.teacher_assist.week_context_service import WeekContextService
from oziebot_api.services.teacher_assist.week_duplication import duplicate_week


def _now() -> datetime:
    return datetime.now(UTC)


def serialize_template(row: TeacherAssistWeekTemplate) -> dict[str, Any]:
    return {
        "id": str(row.id),
        "name": row.name,
        "description": row.description,
        "subject": row.subject,
        "grade_level": row.grade_level,
        "artifact_type": row.artifact_type,
        "template_type": row.template_type,
        "visibility": row.visibility,
        "school_year_id": str(row.school_year_id) if row.school_year_id else None,
        "source_period_id": str(row.source_period_id) if row.source_period_id else None,
        "template_data": row.template_data,
        "created_at": row.created_at.isoformat(),
        "updated_at": row.updated_at.isoformat(),
    }


def save_week_as_template(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user: User,
    period_id: uuid.UUID,
    name: str,
    description: str | None = None,
    template_type: str = "TEACHER",
    visibility: str = "PRIVATE",
    artifact_type: str = "WEEK",
) -> TeacherAssistWeekTemplate:
    if template_type not in TEMPLATE_TYPES:
        raise ValueError("Unsupported template type")
    if visibility not in TEMPLATE_VISIBILITY:
        raise ValueError("Unsupported template visibility")
    context = WeekContextService.build(db, tenant_id=tenant_id, user=user, period_id=period_id)
    payload = WeekContextService.serialize(context)
    now = _now()
    row = TeacherAssistWeekTemplate(
        tenant_id=tenant_id,
        created_by_user_id=user.id,
        name=name.strip(),
        description=description,
        subject=context.subject_name,
        grade_level=context.grade_level,
        artifact_type=artifact_type.upper(),
        template_type=template_type.upper(),
        visibility=visibility.upper(),
        school_year_id=context.school_year_id,
        source_period_id=period_id,
        template_data=payload,
        created_at=now,
        updated_at=now,
    )
    db.add(row)
    db.flush()
    record_reuse_event(
        db,
        tenant_id=tenant_id,
        user_id=user.id,
        event_type="save_template",
        artifact_type=artifact_type.upper(),
        source_entity_type="pacing_guide_period",
        source_entity_id=period_id,
        target_entity_id=row.id,
        estimated_minutes_saved=0,
    )
    return row


def list_template_library(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    subject: str | None = None,
    grade_level: str | None = None,
    artifact_type: str | None = None,
    visibility: str | None = None,
) -> list[TeacherAssistWeekTemplate]:
    query = select(TeacherAssistWeekTemplate).where(
        TeacherAssistWeekTemplate.tenant_id == tenant_id,
        or_(
            TeacherAssistWeekTemplate.created_by_user_id == user_id,
            TeacherAssistWeekTemplate.visibility.in_(("TEAM", "SCHOOL", "DISTRICT")),
        ),
    )
    if subject:
        query = query.where(TeacherAssistWeekTemplate.subject.ilike(f"%{subject}%"))
    if grade_level:
        query = query.where(TeacherAssistWeekTemplate.grade_level == grade_level)
    if artifact_type:
        query = query.where(TeacherAssistWeekTemplate.artifact_type == artifact_type.upper())
    if visibility:
        query = query.where(TeacherAssistWeekTemplate.visibility == visibility.upper())
    return list(db.scalars(query.order_by(TeacherAssistWeekTemplate.updated_at.desc())).all())


def apply_week_template(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user: User,
    template_id: uuid.UUID,
    target_period_id: uuid.UUID,
) -> dict[str, Any]:
    template = db.scalars(
        select(TeacherAssistWeekTemplate).where(
            TeacherAssistWeekTemplate.id == template_id,
            TeacherAssistWeekTemplate.tenant_id == tenant_id,
        )
    ).one_or_none()
    if template is None:
        raise LookupError("Template not found")
    source_period_id = template.source_period_id
    if source_period_id is None:
        raise ValueError("Template is missing a source week")
    result = duplicate_week(
        db,
        tenant_id=tenant_id,
        user=user,
        source_period_id=source_period_id,
        target_period_id=target_period_id,
        copy_objectives=True,
        copy_resources=True,
        copy_notes=True,
        copy_artifacts=False,
    )
    record_reuse_event(
        db,
        tenant_id=tenant_id,
        user_id=user.id,
        event_type="apply_template",
        artifact_type=template.artifact_type,
        source_entity_type="week_template",
        source_entity_id=template.id,
        target_entity_id=target_period_id,
        estimated_minutes_saved=TIME_SAVINGS_MINUTES["TEMPLATE"],
    )
    result["template_id"] = str(template.id)
    return result
