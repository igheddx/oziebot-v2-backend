from __future__ import annotations

import uuid
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from oziebot_api.models.teacher_assist_assignment import TeacherAssistAssignment
from oziebot_api.models.teacher_assist_class import TeacherAssistClass
from oziebot_api.models.teacher_assist_reteach_plan import TeacherAssistReteachPlan
from oziebot_api.models.teacher_assist_subject import TeacherAssistSubject
from oziebot_api.models.teacher_assist_weekly_plan import TeacherAssistWeeklyPlan
from oziebot_api.services.teacher_assist.user_preferences import get_user_preferences_or_create


def build_teacher_shortcuts(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    preferences: TeacherAssistUserPreference | None = None,
) -> dict[str, Any]:
    prefs = preferences or get_user_preferences_or_create(db, tenant_id=tenant_id, user_id=user_id)

    recent_assignments = db.scalars(
        select(TeacherAssistAssignment)
        .where(
            TeacherAssistAssignment.tenant_id == tenant_id,
            TeacherAssistAssignment.teacher_user_id == user_id,
        )
        .order_by(TeacherAssistAssignment.updated_at.desc())
        .limit(5)
    ).all()
    recent_plans = db.scalars(
        select(TeacherAssistWeeklyPlan)
        .where(
            TeacherAssistWeeklyPlan.tenant_id == tenant_id,
            TeacherAssistWeeklyPlan.user_id == user_id,
        )
        .order_by(TeacherAssistWeeklyPlan.updated_at.desc())
        .limit(5)
    ).all()
    recent_reteach = db.scalars(
        select(TeacherAssistReteachPlan)
        .where(
            TeacherAssistReteachPlan.tenant_id == tenant_id,
            TeacherAssistReteachPlan.owner_user_id == user_id,
        )
        .order_by(TeacherAssistReteachPlan.updated_at.desc())
        .limit(5)
    ).all()

    class_usage: dict[uuid.UUID, int] = {}
    subject_usage: dict[uuid.UUID, int] = {}
    grading_period_usage: dict[uuid.UUID, int] = {}
    for assignment in recent_assignments:
        class_usage[assignment.class_id] = class_usage.get(assignment.class_id, 0) + 1
        subject_usage[assignment.subject_id] = subject_usage.get(assignment.subject_id, 0) + 1
        if assignment.grading_period_id is not None:
            grading_period_usage[assignment.grading_period_id] = (
                grading_period_usage.get(assignment.grading_period_id, 0) + 1
            )

    def _most_used(mapping: dict[uuid.UUID, int]) -> uuid.UUID | None:
        if not mapping:
            return None
        return max(mapping.items(), key=lambda item: item[1])[0]

    most_used_class_id = _most_used(class_usage) or prefs.last_class_id
    most_used_subject_id = _most_used(subject_usage) or prefs.last_subject_id
    most_used_grading_period_id = _most_used(grading_period_usage) or prefs.last_grading_period_id

    class_name = None
    subject_name = None
    if most_used_class_id is not None:
        teacher_class = db.get(TeacherAssistClass, most_used_class_id)
        class_name = teacher_class.name if teacher_class else None
    if most_used_subject_id is not None:
        subject = db.get(TeacherAssistSubject, most_used_subject_id)
        subject_name = subject.name if subject else None

    return {
        "most_used_class": (
            {
                "class_id": most_used_class_id,
                "class_name": class_name,
                "navigation_href": f"/teacher-assist/classes/{most_used_class_id}",
            }
            if most_used_class_id is not None
            else None
        ),
        "most_used_subject": (
            {
                "subject_id": most_used_subject_id,
                "subject_name": subject_name,
            }
            if most_used_subject_id is not None
            else None
        ),
        "most_used_grading_period_id": most_used_grading_period_id,
        "recent_assignments": [
            {
                "assignment_id": row.id,
                "title": row.title,
                "navigation_href": f"/teacher-assist/assignments?assignment_id={row.id}",
            }
            for row in recent_assignments
        ],
        "recent_plans": [
            {
                "weekly_plan_id": row.id,
                "title": row.title,
                "navigation_href": f"/teacher-assist/weekly-planning/plans?id={row.id}",
            }
            for row in recent_plans
        ],
        "recent_reteach_plans": [
            {
                "reteach_plan_id": row.id,
                "title": row.title,
                "navigation_href": f"/teacher-assist/reteach-plans?id={row.id}",
            }
            for row in recent_reteach
        ],
        "recently_viewed": prefs.recently_viewed_json or [],
    }
