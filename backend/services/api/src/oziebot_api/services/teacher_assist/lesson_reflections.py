from __future__ import annotations

from datetime import UTC, date, datetime
import uuid
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from oziebot_api.models.teacher_assist_lesson_reflection import TeacherAssistLessonReflection
from oziebot_api.models.teacher_assist_lesson_reflection_version import (
    TeacherAssistLessonReflectionVersion,
)
from oziebot_api.services.teacher_assist.activity_events import record_activity_event
from oziebot_api.services.teacher_assist.constants import (
    validate_lesson_reflection_status,
    validate_lesson_reflection_version_source,
)
from oziebot_api.services.teacher_assist.lesson_effectiveness import (
    build_weekly_plan_lesson_effectiveness_by_id,
)
from oziebot_api.services.teacher_assist.setup import (
    get_class_or_404,
    get_grading_period_or_404,
    get_school_year_or_404,
    get_subject_or_404,
)
from oziebot_api.services.teacher_assist.workflow_service import get_visible_weekly_plan_or_404


def _normalize_title(value: str) -> str:
    normalized = value.strip()
    if not normalized:
        raise ValueError("Reflection title is required")
    return normalized


def _json_safe_value(value: Any) -> Any:
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: _json_safe_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe_value(item) for item in value]
    return value


def _empty_reflection_content() -> dict[str, Any]:
    return {
        "what_worked": [],
        "what_failed": [],
        "notes_for_next_year": [],
        "strengths": [],
        "weaknesses": [],
        "improvements": [],
        "teacher_review_required": True,
        "is_ai_draft": False,
    }


def get_lesson_reflection_or_404(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    lesson_reflection_id: uuid.UUID,
    load_versions: bool = False,
) -> TeacherAssistLessonReflection:
    query = select(TeacherAssistLessonReflection).where(
        TeacherAssistLessonReflection.id == lesson_reflection_id,
        TeacherAssistLessonReflection.tenant_id == tenant_id,
        TeacherAssistLessonReflection.owner_user_id == user_id,
    )
    if load_versions:
        query = query.options(
            selectinload(TeacherAssistLessonReflection.versions),
            selectinload(TeacherAssistLessonReflection.current_version),
            selectinload(TeacherAssistLessonReflection.subject),
            selectinload(TeacherAssistLessonReflection.teacher_class),
            selectinload(TeacherAssistLessonReflection.weekly_plan),
        )
    else:
        query = query.options(
            selectinload(TeacherAssistLessonReflection.subject),
            selectinload(TeacherAssistLessonReflection.teacher_class),
            selectinload(TeacherAssistLessonReflection.weekly_plan),
        )
    row = db.scalars(query).one_or_none()
    if row is None:
        raise LookupError("Lesson reflection not found")
    return row


def list_lesson_reflections(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    school_year_id: uuid.UUID | None = None,
    grading_period_id: uuid.UUID | None = None,
    class_id: uuid.UUID | None = None,
    subject_id: uuid.UUID | None = None,
    weekly_plan_id: uuid.UUID | None = None,
    status: str | None = None,
) -> list[TeacherAssistLessonReflection]:
    query = select(TeacherAssistLessonReflection).where(
        TeacherAssistLessonReflection.tenant_id == tenant_id,
        TeacherAssistLessonReflection.owner_user_id == user_id,
    )
    if school_year_id is not None:
        query = query.where(TeacherAssistLessonReflection.school_year_id == school_year_id)
    if grading_period_id is not None:
        query = query.where(TeacherAssistLessonReflection.grading_period_id == grading_period_id)
    if class_id is not None:
        query = query.where(TeacherAssistLessonReflection.class_id == class_id)
    if subject_id is not None:
        query = query.where(TeacherAssistLessonReflection.subject_id == subject_id)
    if weekly_plan_id is not None:
        query = query.where(TeacherAssistLessonReflection.weekly_plan_id == weekly_plan_id)
    if status is not None:
        query = query.where(
            TeacherAssistLessonReflection.status == validate_lesson_reflection_status(status)
        )
    return db.scalars(
        query.order_by(
            TeacherAssistLessonReflection.updated_at.desc(),
            TeacherAssistLessonReflection.created_at.desc(),
        )
    ).all()


def _validate_reflection_context(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    school_year_id: uuid.UUID,
    grading_period_id: uuid.UUID | None,
    class_id: uuid.UUID,
    subject_id: uuid.UUID,
    weekly_plan_id: uuid.UUID | None,
) -> None:
    get_school_year_or_404(db, tenant_id=tenant_id, school_year_id=school_year_id)
    if grading_period_id is not None:
        period = get_grading_period_or_404(
            db, tenant_id=tenant_id, grading_period_id=grading_period_id
        )
        if period.school_year_id != school_year_id:
            raise ValueError("Grading period does not belong to the selected school year")
    teacher_class = get_class_or_404(db, tenant_id=tenant_id, class_id=class_id)
    if teacher_class.school_year_id != school_year_id:
        raise ValueError("Class does not belong to the selected school year")
    get_subject_or_404(db, tenant_id=tenant_id, subject_id=subject_id)
    if weekly_plan_id is not None:
        get_visible_weekly_plan_or_404(
            db,
            tenant_id=tenant_id,
            user_id=user_id,
            weekly_plan_id=weekly_plan_id,
        )


def create_lesson_reflection(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    school_year_id: uuid.UUID,
    grading_period_id: uuid.UUID | None,
    class_id: uuid.UUID,
    subject_id: uuid.UUID,
    weekly_plan_id: uuid.UUID | None = None,
    title: str | None = None,
    lesson_date: date | None = None,
) -> TeacherAssistLessonReflection:
    _validate_reflection_context(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        school_year_id=school_year_id,
        grading_period_id=grading_period_id,
        class_id=class_id,
        subject_id=subject_id,
        weekly_plan_id=weekly_plan_id,
    )
    subject = get_subject_or_404(db, tenant_id=tenant_id, subject_id=subject_id)
    normalized_title = _normalize_title(title or f"Lesson Reflection — {subject.name}")
    now = datetime.now(UTC)
    reflection = TeacherAssistLessonReflection(
        tenant_id=tenant_id,
        owner_user_id=user_id,
        school_year_id=school_year_id,
        grading_period_id=grading_period_id,
        class_id=class_id,
        subject_id=subject_id,
        weekly_plan_id=weekly_plan_id,
        title=normalized_title,
        status=validate_lesson_reflection_status("draft"),
        lesson_date=lesson_date,
        created_at=now,
        updated_at=now,
    )
    db.add(reflection)
    db.flush()
    create_lesson_reflection_version(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        lesson_reflection=reflection,
        content_json=_empty_reflection_content(),
        version_source="initial",
        change_reason="Initial reflection workspace",
    )
    record_activity_event(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        event_type="lesson_reflection_created",
        event_category="insights",
        entity_type="lesson_reflection",
        entity_id=reflection.id,
        school_year_id=reflection.school_year_id,
        grading_period_id=reflection.grading_period_id,
        class_id=reflection.class_id,
        subject_id=reflection.subject_id,
        summary_text=f"Created lesson reflection '{reflection.title}'.",
        details_json={
            "lesson_reflection_id": str(reflection.id),
            "weekly_plan_id": str(reflection.weekly_plan_id) if reflection.weekly_plan_id else None,
            "status": reflection.status,
        },
    )
    db.flush()
    return reflection


def create_lesson_reflection_version(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    lesson_reflection: TeacherAssistLessonReflection,
    content_json: dict[str, Any],
    version_source: str,
    change_reason: str | None = None,
    prompt_context_json: dict[str, Any] | None = None,
    provider_name: str | None = None,
    provider_model: str | None = None,
    prompt_version: str | None = None,
    ai_usage_event_id: uuid.UUID | None = None,
) -> TeacherAssistLessonReflectionVersion:
    normalized_source = validate_lesson_reflection_version_source(version_source)
    next_version = (
        db.scalar(
            select(TeacherAssistLessonReflectionVersion.version_number)
            .where(
                TeacherAssistLessonReflectionVersion.lesson_reflection_id == lesson_reflection.id
            )
            .order_by(TeacherAssistLessonReflectionVersion.version_number.desc())
        )
        or 0
    ) + 1
    now = datetime.now(UTC)
    version = TeacherAssistLessonReflectionVersion(
        tenant_id=tenant_id,
        owner_user_id=user_id,
        lesson_reflection_id=lesson_reflection.id,
        version_number=next_version,
        version_source=normalized_source,
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
    lesson_reflection.current_version_id = version.id
    lesson_reflection.updated_at = now
    if ai_usage_event_id is not None:
        lesson_reflection.latest_ai_usage_event_id = ai_usage_event_id
    if normalized_source == "ai_draft":
        lesson_reflection.status = validate_lesson_reflection_status("review")
    elif normalized_source == "teacher_edit":
        lesson_reflection.status = validate_lesson_reflection_status("review")
    db.flush()
    db.refresh(version)
    return version


def update_lesson_reflection(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    lesson_reflection_id: uuid.UUID,
    title: str | None = None,
    status: str | None = None,
    lesson_date: date | None = None,
) -> TeacherAssistLessonReflection:
    reflection = get_lesson_reflection_or_404(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        lesson_reflection_id=lesson_reflection_id,
    )
    if title is not None:
        reflection.title = _normalize_title(title)
    if status is not None:
        reflection.status = validate_lesson_reflection_status(status)
    if lesson_date is not None:
        reflection.lesson_date = lesson_date
    reflection.updated_at = datetime.now(UTC)
    db.flush()
    db.refresh(reflection)
    return reflection


def create_teacher_lesson_reflection_version(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    lesson_reflection_id: uuid.UUID,
    content_json: dict[str, Any],
    change_reason: str | None = None,
) -> TeacherAssistLessonReflectionVersion:
    reflection = get_lesson_reflection_or_404(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        lesson_reflection_id=lesson_reflection_id,
    )
    normalized_content = dict(content_json)
    normalized_content["teacher_review_required"] = True
    normalized_content["is_ai_draft"] = False
    version = create_lesson_reflection_version(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        lesson_reflection=reflection,
        content_json=normalized_content,
        version_source="teacher_edit",
        change_reason=change_reason,
    )
    record_activity_event(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        event_type="lesson_reflection_version_created",
        event_category="insights",
        entity_type="lesson_reflection",
        entity_id=reflection.id,
        school_year_id=reflection.school_year_id,
        grading_period_id=reflection.grading_period_id,
        class_id=reflection.class_id,
        subject_id=reflection.subject_id,
        summary_text=f"Teacher saved reflection version {version.version_number}.",
        details_json={
            "lesson_reflection_id": str(reflection.id),
            "version_id": str(version.id),
            "version_source": version.version_source,
        },
    )
    db.flush()
    return version


def list_lesson_reflection_versions(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    lesson_reflection_id: uuid.UUID,
) -> list[TeacherAssistLessonReflectionVersion]:
    get_lesson_reflection_or_404(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        lesson_reflection_id=lesson_reflection_id,
    )
    return db.scalars(
        select(TeacherAssistLessonReflectionVersion)
        .where(
            TeacherAssistLessonReflectionVersion.tenant_id == tenant_id,
            TeacherAssistLessonReflectionVersion.owner_user_id == user_id,
            TeacherAssistLessonReflectionVersion.lesson_reflection_id == lesson_reflection_id,
        )
        .order_by(TeacherAssistLessonReflectionVersion.version_number.asc())
    ).all()


def build_lesson_reflection_prompt_context(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    lesson_reflection: TeacherAssistLessonReflection,
    settings=None,
) -> dict[str, Any]:
    from oziebot_api.config import Settings

    settings = settings or Settings()
    effectiveness = None
    if lesson_reflection.weekly_plan_id is not None:
        try:
            effectiveness = build_weekly_plan_lesson_effectiveness_by_id(
                db,
                tenant_id=tenant_id,
                user_id=user_id,
                weekly_plan_id=lesson_reflection.weekly_plan_id,
                settings=settings,
            )
        except LookupError:
            effectiveness = None

    current_content = (
        lesson_reflection.current_version.content_json
        if lesson_reflection.current_version is not None
        else _empty_reflection_content()
    )
    return _json_safe_value(
        {
            "lesson_reflection_id": str(lesson_reflection.id),
            "weekly_plan_id": str(lesson_reflection.weekly_plan_id)
            if lesson_reflection.weekly_plan_id
            else None,
            "class_id": str(lesson_reflection.class_id),
            "subject_id": str(lesson_reflection.subject_id),
            "subject_name": lesson_reflection.subject.name if lesson_reflection.subject else None,
            "lesson_effectiveness": effectiveness,
            "teacher_notes": {
                "what_worked": current_content.get("what_worked") or [],
                "what_failed": current_content.get("what_failed") or [],
                "notes_for_next_year": current_content.get("notes_for_next_year") or [],
            },
            "anonymous_only": True,
            "pii_policy": "NO_STUDENT_NAMES_GRADES_BEHAVIOR",
        }
    )


def serialize_lesson_reflection(reflection: TeacherAssistLessonReflection) -> dict[str, Any]:
    return {
        "id": reflection.id,
        "tenant_id": reflection.tenant_id,
        "owner_user_id": reflection.owner_user_id,
        "school_year_id": reflection.school_year_id,
        "grading_period_id": reflection.grading_period_id,
        "class_id": reflection.class_id,
        "subject_id": reflection.subject_id,
        "weekly_plan_id": reflection.weekly_plan_id,
        "title": reflection.title,
        "status": reflection.status,
        "lesson_date": reflection.lesson_date,
        "current_version_id": reflection.current_version_id,
        "latest_ai_usage_event_id": reflection.latest_ai_usage_event_id,
        "created_at": reflection.created_at,
        "updated_at": reflection.updated_at,
        "subject_name": reflection.subject.name if reflection.subject else None,
        "class_name": reflection.teacher_class.name if reflection.teacher_class else None,
        "weekly_plan_title": reflection.weekly_plan.title if reflection.weekly_plan else None,
    }


def serialize_lesson_reflection_version(
    version: TeacherAssistLessonReflectionVersion,
) -> dict[str, Any]:
    return {
        "id": version.id,
        "lesson_reflection_id": version.lesson_reflection_id,
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
