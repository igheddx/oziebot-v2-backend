from __future__ import annotations

import uuid
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from oziebot_api.config import Settings
from oziebot_api.models.teacher_assist_assignment import TeacherAssistAssignment
from oziebot_api.models.teacher_assist_lesson_reflection import TeacherAssistLessonReflection
from oziebot_api.models.teacher_assist_reteach_plan import TeacherAssistReteachPlan
from oziebot_api.services.teacher_assist.action_workspace import get_teacher_assist_action_workspace
from oziebot_api.services.teacher_assist.lesson_effectiveness import list_lesson_effectiveness
from oziebot_api.services.teacher_assist.mastery_dashboard import build_mastery_dashboard
from oziebot_api.services.teacher_assist.setup import get_class_or_404


def get_teacher_assist_class_workspace(
    db: Session,
    *,
    settings: Settings,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    class_id: uuid.UUID,
) -> dict[str, Any]:
    teacher_class = get_class_or_404(db, tenant_id=tenant_id, class_id=class_id)
    action_payload = get_teacher_assist_action_workspace(
        db,
        settings=settings,
        tenant_id=tenant_id,
        user_id=user_id,
    )
    class_items = [
        item
        for section in action_payload.get("sections", [])
        for item in section.get("items", [])
        if item.get("class_id") == class_id and item.get("severity") != "info"
    ]
    assignments = db.scalars(
        select(TeacherAssistAssignment)
        .where(
            TeacherAssistAssignment.tenant_id == tenant_id,
            TeacherAssistAssignment.teacher_user_id == user_id,
            TeacherAssistAssignment.class_id == class_id,
        )
        .order_by(TeacherAssistAssignment.updated_at.desc())
        .limit(10)
    ).all()
    reteach_plans = db.scalars(
        select(TeacherAssistReteachPlan)
        .where(
            TeacherAssistReteachPlan.tenant_id == tenant_id,
            TeacherAssistReteachPlan.owner_user_id == user_id,
            TeacherAssistReteachPlan.class_id == class_id,
            TeacherAssistReteachPlan.status != "archived",
        )
        .order_by(TeacherAssistReteachPlan.updated_at.desc())
        .limit(10)
    ).all()
    reflections = db.scalars(
        select(TeacherAssistLessonReflection)
        .where(
            TeacherAssistLessonReflection.tenant_id == tenant_id,
            TeacherAssistLessonReflection.owner_user_id == user_id,
            TeacherAssistLessonReflection.class_id == class_id,
        )
        .order_by(TeacherAssistLessonReflection.updated_at.desc())
        .limit(10)
    ).all()
    mastery_dashboard = build_mastery_dashboard(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        class_id=class_id,
        school_year_id=teacher_class.school_year_id,
        settings=settings,
    )
    effectiveness = list_lesson_effectiveness(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        class_id=class_id,
        school_year_id=teacher_class.school_year_id,
        settings=settings,
    )[:5]

    return {
        "class_id": teacher_class.id,
        "class_name": teacher_class.name,
        "grade_level": teacher_class.grade_level,
        "student_count": teacher_class.student_count,
        "school_year_id": teacher_class.school_year_id,
        "summary": {
            "pending_actions_count": len(class_items),
            "assignment_count": len(assignments),
            "reteach_plan_count": len(reteach_plans),
            "reflection_count": len(reflections),
            "mastery_matrix_count": len(mastery_dashboard.get("matrices") or []),
        },
        "tabs": {
            "overview": {
                "pending_reviews": sum(
                    1
                    for item in class_items
                    if item.get("section_key") in {"extractions", "grading"}
                ),
                "recent_assignments": [
                    {
                        "assignment_id": row.id,
                        "title": row.title,
                        "status": row.status,
                        "due_date": row.due_date.isoformat() if row.due_date else None,
                        "navigation_href": f"/teacher-assist/assignments?assignment_id={row.id}",
                    }
                    for row in assignments[:5]
                ],
                "lesson_effectiveness": effectiveness,
                "mastery_snapshot": {
                    "reteach_recommended_count": len(
                        mastery_dashboard.get("reteach_recommended_standards") or []
                    ),
                    "matrices": mastery_dashboard.get("matrices") or [],
                },
            },
            "assignments": {
                "items": [
                    {
                        "assignment_id": row.id,
                        "title": row.title,
                        "status": row.status,
                        "navigation_href": f"/teacher-assist/assignments?assignment_id={row.id}",
                    }
                    for row in assignments
                ],
                "navigation_href": f"/teacher-assist/assignments?class_id={class_id}",
            },
            "student_work": {
                "navigation_href": f"/teacher-assist/extractions?class_id={class_id}",
            },
            "reviews": {
                "open_items": class_items,
                "navigation_href": f"/teacher-assist/work-queue?class_id={class_id}",
            },
            "gradebook": {
                "navigation_href": f"/teacher-assist/gradebook?class_id={class_id}",
            },
            "mastery": {
                "navigation_href": f"/teacher-assist/mastery?class_id={class_id}",
                "dashboard": mastery_dashboard,
            },
            "reteach": {
                "items": [
                    {
                        "reteach_plan_id": row.id,
                        "title": row.title,
                        "status": row.status,
                        "navigation_href": f"/teacher-assist/reteach-plans?id={row.id}",
                    }
                    for row in reteach_plans
                ],
                "navigation_href": f"/teacher-assist/reteach-plans?class_id={class_id}",
            },
            "reflections": {
                "items": [
                    {
                        "reflection_id": row.id,
                        "title": row.title,
                        "status": row.status,
                        "navigation_href": f"/teacher-assist/reflections?id={row.id}",
                    }
                    for row in reflections
                ],
                "navigation_href": f"/teacher-assist/reflections?class_id={class_id}",
            },
        },
        "recent_activity": action_payload.get("recent_activity", [])[:10],
        "read_only": True,
    }
