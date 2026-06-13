from __future__ import annotations

from datetime import UTC, datetime
import uuid
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, object_session

from oziebot_api.models.teacher_assist_class import TeacherAssistClass
from oziebot_api.models.teacher_assist_subject import TeacherAssistSubject
from oziebot_api.models.teacher_assist_user_preference import TeacherAssistUserPreference
from oziebot_api.services.teacher_assist.constants import (
    TEACHER_ASSIST_ONBOARDING_STEP_KEYS,
    TEACHER_ASSIST_PREFERRED_LANDINGS,
)
from oziebot_api.services.teacher_assist.education_catalog import get_active_teacher_assignment
from oziebot_api.services.teacher_assist.setup import get_teacher_profile


def _now() -> datetime:
    return datetime.now(UTC)


def validate_preferred_landing(value: str | None) -> str:
    normalized = (value or "home").strip().lower()
    if normalized not in TEACHER_ASSIST_PREFERRED_LANDINGS:
        raise ValueError("Unsupported preferred landing")
    return normalized


def get_user_preferences_or_create(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
) -> TeacherAssistUserPreference:
    row = _get_user_preferences(db, tenant_id=tenant_id, user_id=user_id)
    if row is not None:
        return row
    now = _now()
    row = TeacherAssistUserPreference(
        tenant_id=tenant_id,
        user_id=user_id,
        preferred_landing="home",
        recently_viewed_json=[],
        onboarding_progress_json={},
        created_at=now,
        updated_at=now,
    )
    db.add(row)
    try:
        with db.begin_nested():
            db.flush()
    except IntegrityError:
        # Nested rollback may already detach the failed insert from the session.
        if object_session(row) is db:
            db.expunge(row)
        existing = _get_user_preferences(db, tenant_id=tenant_id, user_id=user_id)
        if existing is None:
            raise
        return existing
    return row


def _get_user_preferences(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
) -> TeacherAssistUserPreference | None:
    return db.scalars(
        select(TeacherAssistUserPreference).where(
            TeacherAssistUserPreference.tenant_id == tenant_id,
            TeacherAssistUserPreference.user_id == user_id,
        )
    ).one_or_none()


def mark_onboarding_step_complete(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    step_key: str,
) -> TeacherAssistUserPreference:
    prefs = get_user_preferences_or_create(db, tenant_id=tenant_id, user_id=user_id)
    progress = dict(prefs.onboarding_progress_json or {})
    steps_completed = dict(progress.get("steps_completed") or {})
    steps_completed[step_key] = True
    progress["steps_completed"] = steps_completed
    prefs.onboarding_progress_json = progress
    prefs.updated_at = _now()
    db.flush()
    return prefs


def _onboarding_step_complete(
    preferences: TeacherAssistUserPreference | None,
    step_key: str,
) -> bool:
    if preferences is None:
        return False
    steps_completed = (preferences.onboarding_progress_json or {}).get("steps_completed") or {}
    return bool(steps_completed.get(step_key))


def build_onboarding_progress(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    preferences: TeacherAssistUserPreference | None = None,
) -> dict[str, Any]:
    if preferences is None:
        preferences = _get_user_preferences(db, tenant_id=tenant_id, user_id=user_id)
    profile = get_teacher_profile(db, user_id=user_id)
    school_assignment = get_active_teacher_assignment(db, user_id=user_id)
    class_count = int(
        db.scalar(
            select(func.count()).select_from(TeacherAssistClass).where(
                TeacherAssistClass.tenant_id == tenant_id
            )
        )
        or 0
    )
    subject_count = int(
        db.scalar(
            select(func.count()).select_from(TeacherAssistSubject).where(
                TeacherAssistSubject.tenant_id == tenant_id
            )
        )
        or 0
    )
    step_definitions: list[dict[str, Any]] = [
        {
            "key": "school_placement",
            "title": "School & district",
            "description": "Associate with your state, district, school, grade, and teaching subjects.",
            "complete": school_assignment is not None
            and subject_count > 0
            and profile is not None
            and bool(profile.preferred_grade_level),
            "navigation_href": "/teacher-assist/settings#school-setup",
            "navigation_label": "Open school setup",
        },
        {
            "key": "school_year",
            "title": "School year",
            "description": "Define the active instructional year for your homeroom.",
            "complete": _onboarding_step_complete(preferences, "school_year"),
            "navigation_href": "/teacher-assist/settings#school-year",
            "navigation_label": "Add school year",
        },
        {
            "key": "classroom",
            "title": "My classroom",
            "description": "Set your homeroom name and student count.",
            "complete": class_count > 0
            and profile is not None
            and profile.default_student_count is not None
            and profile.default_student_count > 0,
            "navigation_href": "/teacher-assist/settings#my-classroom",
            "navigation_label": "Configure classroom",
        },
    ]
    step_definitions = [
        step for step in step_definitions if step["key"] in TEACHER_ASSIST_ONBOARDING_STEP_KEYS
    ]
    completed_count = sum(1 for step in step_definitions if step["complete"])
    progress_percent = round((completed_count / len(step_definitions)) * 100) if step_definitions else 0
    is_complete = completed_count == len(step_definitions)
    if preferences is not None and is_complete and preferences.onboarding_completed_at is None:
        preferences.onboarding_completed_at = _now()
        preferences.updated_at = _now()
        db.flush()
    return {
        "steps": step_definitions,
        "completed_count": completed_count,
        "total_count": len(step_definitions),
        "progress_percent": progress_percent,
        "is_complete": is_complete,
        "completed_at": (
            preferences.onboarding_completed_at.isoformat()
            if preferences is not None and preferences.onboarding_completed_at is not None
            else None
        ),
    }


def update_user_preferences(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    last_class_id: uuid.UUID | None = None,
    last_grading_period_id: uuid.UUID | None = None,
    last_subject_id: uuid.UUID | None = None,
    active_pacing_guide_id: uuid.UUID | None = None,
    manual_pacing_period_id: uuid.UUID | None = None,
    preferred_landing: str | None = None,
    recently_viewed: list[dict[str, Any]] | None = None,
    mark_onboarding_complete: bool = False,
) -> TeacherAssistUserPreference:
    row = get_user_preferences_or_create(db, tenant_id=tenant_id, user_id=user_id)
    if last_class_id is not None:
        row.last_class_id = last_class_id
    if last_grading_period_id is not None:
        row.last_grading_period_id = last_grading_period_id
    if last_subject_id is not None:
        row.last_subject_id = last_subject_id
    if active_pacing_guide_id is not None:
        row.active_pacing_guide_id = active_pacing_guide_id
    if manual_pacing_period_id is not None:
        row.manual_pacing_period_id = manual_pacing_period_id
    if preferred_landing is not None:
        row.preferred_landing = validate_preferred_landing(preferred_landing)
    if recently_viewed is not None:
        row.recently_viewed_json = recently_viewed[:20]
    if mark_onboarding_complete:
        row.onboarding_completed_at = _now()
    row.updated_at = _now()
    db.flush()
    db.refresh(row)
    return row


def record_recently_viewed(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    item_type: str,
    item_id: uuid.UUID,
    title: str,
    href: str,
) -> TeacherAssistUserPreference:
    row = get_user_preferences_or_create(db, tenant_id=tenant_id, user_id=user_id)
    entry = {
        "item_type": item_type,
        "item_id": str(item_id),
        "title": title,
        "href": href,
        "viewed_at": _now().isoformat(),
    }
    existing = [item for item in row.recently_viewed_json if item.get("item_id") != str(item_id)]
    row.recently_viewed_json = [entry, *existing][:20]
    row.updated_at = _now()
    db.flush()
    return row


def serialize_user_preferences(row: TeacherAssistUserPreference) -> dict[str, Any]:
    return {
        "id": row.id,
        "tenant_id": row.tenant_id,
        "user_id": row.user_id,
        "last_class_id": row.last_class_id,
        "last_grading_period_id": row.last_grading_period_id,
        "last_subject_id": row.last_subject_id,
        "active_pacing_guide_id": row.active_pacing_guide_id,
        "manual_pacing_period_id": row.manual_pacing_period_id,
        "preferred_landing": row.preferred_landing,
        "recently_viewed": row.recently_viewed_json or [],
        "onboarding_completed_at": row.onboarding_completed_at,
        "created_at": row.created_at,
        "updated_at": row.updated_at,
    }
