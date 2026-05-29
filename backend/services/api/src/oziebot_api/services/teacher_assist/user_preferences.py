from __future__ import annotations

from datetime import UTC, datetime
import uuid
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from oziebot_api.models.teacher_assist_assignment import TeacherAssistAssignment
from oziebot_api.models.teacher_assist_class import TeacherAssistClass
from oziebot_api.models.teacher_assist_grading_period import TeacherAssistGradingPeriod
from oziebot_api.models.teacher_assist_mastery_matrix import TeacherAssistMasteryMatrix
from oziebot_api.models.teacher_assist_resource_library_item import TeacherAssistResourceLibraryItem
from oziebot_api.models.teacher_assist_school_year import TeacherAssistSchoolYear
from oziebot_api.models.teacher_assist_standard import TeacherAssistStandard
from oziebot_api.models.teacher_assist_subject import TeacherAssistSubject
from oziebot_api.models.teacher_assist_user_preference import TeacherAssistUserPreference
from oziebot_api.models.teacher_assist_weekly_plan import TeacherAssistWeeklyPlan
from oziebot_api.services.teacher_assist.constants import (
    TEACHER_ASSIST_ONBOARDING_STEP_KEYS,
    TEACHER_ASSIST_PREFERRED_LANDINGS,
)
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
        row = _get_user_preferences(db, tenant_id=tenant_id, user_id=user_id)
        if row is None:
            raise
        return row
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


def build_onboarding_progress(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    preferences: TeacherAssistUserPreference | None = None,
) -> dict[str, Any]:
    profile = get_teacher_profile(db, user_id=user_id)
    school_year_count = int(
        db.scalar(
            select(func.count()).select_from(TeacherAssistSchoolYear).where(
                TeacherAssistSchoolYear.tenant_id == tenant_id
            )
        )
        or 0
    )
    grading_period_count = int(
        db.scalar(
            select(func.count())
            .select_from(TeacherAssistGradingPeriod)
            .join(
                TeacherAssistSchoolYear,
                TeacherAssistGradingPeriod.school_year_id == TeacherAssistSchoolYear.id,
            )
            .where(TeacherAssistSchoolYear.tenant_id == tenant_id)
        )
        or 0
    )
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
    standard_count = int(
        db.scalar(
            select(func.count()).select_from(TeacherAssistStandard).where(
                TeacherAssistStandard.tenant_id == tenant_id
            )
        )
        or 0
    )
    resource_count = int(
        db.scalar(
            select(func.count()).select_from(TeacherAssistResourceLibraryItem).where(
                TeacherAssistResourceLibraryItem.tenant_id == tenant_id
            )
        )
        or 0
    )
    plan_count = int(
        db.scalar(
            select(func.count())
            .select_from(TeacherAssistWeeklyPlan)
            .where(
                TeacherAssistWeeklyPlan.tenant_id == tenant_id,
                TeacherAssistWeeklyPlan.user_id == user_id,
            )
        )
        or 0
    )
    assignment_count = int(
        db.scalar(
            select(func.count())
            .select_from(TeacherAssistAssignment)
            .where(
                TeacherAssistAssignment.tenant_id == tenant_id,
                TeacherAssistAssignment.teacher_user_id == user_id,
            )
        )
        or 0
    )
    mastery_matrix_count = int(
        db.scalar(
            select(func.count())
            .select_from(TeacherAssistMasteryMatrix)
            .where(
                TeacherAssistMasteryMatrix.tenant_id == tenant_id,
                TeacherAssistMasteryMatrix.owner_user_id == user_id,
            )
        )
        or 0
    )

    step_definitions: list[dict[str, Any]] = [
        {
            "key": "profile",
            "title": "Teacher profile",
            "description": "Set grade level, student count, and timezone.",
            "complete": profile is not None
            and any(
                [
                    profile.preferred_grade_level,
                    profile.default_student_count,
                    profile.preferred_grading_period_type,
                    profile.timezone,
                ]
            ),
            "navigation_href": "/teacher-assist/settings",
            "navigation_label": "Open settings",
        },
        {
            "key": "school_year",
            "title": "School year",
            "description": "Define the active instructional year.",
            "complete": school_year_count > 0,
            "navigation_href": "/teacher-assist/settings",
            "navigation_label": "Add school year",
        },
        {
            "key": "grading_periods",
            "title": "Grading periods",
            "description": "Configure reporting periods for pacing and gradebook.",
            "complete": grading_period_count > 0,
            "navigation_href": "/teacher-assist/settings",
            "navigation_label": "Add grading period",
        },
        {
            "key": "classes",
            "title": "Classes",
            "description": "Create the classes you teach.",
            "complete": class_count > 0,
            "navigation_href": "/teacher-assist/settings",
            "navigation_label": "Add class",
        },
        {
            "key": "subjects",
            "title": "Subjects",
            "description": "Add subjects linked to your classes.",
            "complete": subject_count > 0,
            "navigation_href": "/teacher-assist/settings",
            "navigation_label": "Add subject",
        },
        {
            "key": "standards",
            "title": "Standards / TEKS",
            "description": "Import or create standards for mastery tracking.",
            "complete": standard_count > 0,
            "navigation_href": "/teacher-assist/settings",
            "navigation_label": "Add standards",
        },
        {
            "key": "resources",
            "title": "Resources",
            "description": "Upload worksheets, links, and reference materials.",
            "complete": resource_count > 0,
            "navigation_href": "/teacher-assist/resources",
            "navigation_label": "Add resources",
        },
        {
            "key": "first_lesson_plan",
            "title": "First lesson plan",
            "description": "Generate or create your first instructional plan.",
            "complete": plan_count > 0,
            "navigation_href": "/teacher-assist/weekly-planning",
            "navigation_label": "Create plan",
        },
        {
            "key": "first_assignment",
            "title": "First assignment",
            "description": "Create an assignment from a plan or scratch.",
            "complete": assignment_count > 0,
            "navigation_href": "/teacher-assist/assignments",
            "navigation_label": "Create assignment",
        },
        {
            "key": "first_mastery_matrix",
            "title": "First mastery matrix",
            "description": "Start standards-based mastery tracking.",
            "complete": mastery_matrix_count > 0,
            "navigation_href": "/teacher-assist/mastery",
            "navigation_label": "Create matrix",
        },
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
        "preferred_landing": row.preferred_landing,
        "recently_viewed": row.recently_viewed_json or [],
        "onboarding_completed_at": row.onboarding_completed_at,
        "created_at": row.created_at,
        "updated_at": row.updated_at,
    }
