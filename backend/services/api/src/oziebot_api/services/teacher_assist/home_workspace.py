from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
import uuid
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from oziebot_api.config import Settings
from oziebot_api.models.teacher_assist_assignment import TeacherAssistAssignment
from oziebot_api.models.teacher_assist_newsletter import TeacherAssistNewsletter
from oziebot_api.models.teacher_assist_reteach_plan import TeacherAssistReteachPlan
from oziebot_api.models.teacher_assist_weekly_plan import TeacherAssistWeeklyPlan
from oziebot_api.models.teacher_assist_workflow import TeacherAssistWorkflow
from oziebot_api.services.teacher_assist.action_workspace import get_teacher_assist_action_workspace
from oziebot_api.services.teacher_assist.constants import TEACHER_ASSIST_QUICK_CREATE_ACTIONS
from oziebot_api.services.teacher_assist.mastery_dashboard import build_mastery_dashboard
from oziebot_api.services.teacher_assist.teacher_shortcuts import build_teacher_shortcuts
from oziebot_api.services.teacher_assist.today_workspace import get_teacher_assist_today_workspace
from oziebot_api.services.teacher_assist.user_preferences import (
    build_onboarding_progress,
    get_user_preferences_or_create,
)
from oziebot_api.services.teacher_assist.work_queue import (
    PRIORITY_LEVEL_BY_SEVERITY,
    build_teacher_assist_work_queue,
)
from oziebot_api.services.teacher_assist.workspace_service import get_teacher_assist_workspace


def _priority_items_with_levels(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    enriched: list[dict[str, Any]] = []
    for item in items:
        severity = str(item.get("severity") or "info")
        enriched.append(
            {
                **item,
                "priority_level": PRIORITY_LEVEL_BY_SEVERITY.get(severity, "informational"),
            }
        )
    return enriched


def build_home_priorities(
    db: Session,
    *,
    settings: Settings,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
) -> dict[str, Any]:
    today = get_teacher_assist_today_workspace(
        db,
        settings=settings,
        tenant_id=tenant_id,
        user_id=user_id,
    )
    items = _priority_items_with_levels(today.get("priority_items", []))
    grouped: dict[str, list[dict[str, Any]]] = {
        "critical": [],
        "high": [],
        "medium": [],
        "informational": [],
    }
    for item in items:
        level = str(item.get("priority_level") or "informational")
        grouped.setdefault(level, []).append(item)
    return {
        "items": items,
        "grouped": grouped,
        "read_only": True,
    }


def build_home_classes(
    db: Session,
    *,
    settings: Settings,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
) -> list[dict[str, Any]]:
    action_payload = get_teacher_assist_action_workspace(
        db,
        settings=settings,
        tenant_id=tenant_id,
        user_id=user_id,
    )
    workspace_payload = get_teacher_assist_workspace(
        db,
        settings=settings,
        tenant_id=tenant_id,
        user_id=user_id,
    )
    cards: list[dict[str, Any]] = []
    for class_workspace in workspace_payload.get("class_workspaces", []):
        class_id = class_workspace.get("class_id")
        rollup = next(
            (row for row in action_payload.get("class_rollups", []) if row.get("class_id") == class_id),
            None,
        )
        due_assignments = db.scalars(
            select(TeacherAssistAssignment)
            .where(
                TeacherAssistAssignment.tenant_id == tenant_id,
                TeacherAssistAssignment.teacher_user_id == user_id,
                TeacherAssistAssignment.class_id == class_id,
                TeacherAssistAssignment.due_date.is_not(None),
                TeacherAssistAssignment.due_date >= date.today(),
            )
            .order_by(TeacherAssistAssignment.due_date.asc())
            .limit(3)
        ).all()
        cards.append(
            {
                "class_id": class_id,
                "class_name": class_workspace.get("class_name"),
                "subject_names": class_workspace.get("subject_names") or [],
                "student_count": class_workspace.get("student_count"),
                "pending_reviews": (rollup or {}).get("grading_count", 0) + (rollup or {}).get("extraction_count", 0),
                "pending_grades": (rollup or {}).get("gradebook_count", 0),
                "mastery_alerts": (rollup or {}).get("planning_assignment_count", 0),
                "reteach_alerts": 0,
                "open_action_count": (rollup or {}).get("open_action_count", 0),
                "assignments_due": [
                    {
                        "assignment_id": row.id,
                        "title": row.title,
                        "due_date": row.due_date.isoformat() if row.due_date else None,
                    }
                    for row in due_assignments
                ],
                "navigation_href": f"/teacher-assist/classes/{class_id}",
                "actions": [
                    {"label": "Open class", "href": f"/teacher-assist/classes/{class_id}"},
                    {"label": "Assignments", "href": f"/teacher-assist/assignments?class_id={class_id}"},
                    {"label": "Mastery", "href": f"/teacher-assist/mastery?class_id={class_id}"},
                    {"label": "Reteach", "href": f"/teacher-assist/reteach-plans?class_id={class_id}"},
                ],
            }
        )
    return cards


def build_home_timeline(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    days_ahead: int = 14,
) -> list[dict[str, Any]]:
    end_date = date.today() + timedelta(days=days_ahead)
    events: list[dict[str, Any]] = []

    assignments = db.scalars(
        select(TeacherAssistAssignment)
        .where(
            TeacherAssistAssignment.tenant_id == tenant_id,
            TeacherAssistAssignment.teacher_user_id == user_id,
            TeacherAssistAssignment.due_date.is_not(None),
            TeacherAssistAssignment.due_date <= end_date,
        )
        .order_by(TeacherAssistAssignment.due_date.asc())
        .limit(20)
    ).all()
    for row in assignments:
        events.append(
            {
                "event_type": "assignment",
                "title": row.title,
                "event_date": row.due_date.isoformat() if row.due_date else None,
                "navigation_href": f"/teacher-assist/assignments?assignment_id={row.id}",
            }
        )

    plans = db.scalars(
        select(TeacherAssistWeeklyPlan)
        .where(
            TeacherAssistWeeklyPlan.tenant_id == tenant_id,
            TeacherAssistWeeklyPlan.user_id == user_id,
            TeacherAssistWeeklyPlan.end_date.is_not(None),
            TeacherAssistWeeklyPlan.end_date <= end_date,
        )
        .order_by(TeacherAssistWeeklyPlan.end_date.asc())
        .limit(10)
    ).all()
    for row in plans:
        events.append(
            {
                "event_type": "lesson",
                "title": row.title,
                "event_date": row.end_date.isoformat() if row.end_date else None,
                "navigation_href": f"/teacher-assist/weekly-planning/plans?id={row.id}",
            }
        )

    newsletters = db.scalars(
        select(TeacherAssistNewsletter)
        .where(
            TeacherAssistNewsletter.tenant_id == tenant_id,
            TeacherAssistNewsletter.owner_user_id == user_id,
            TeacherAssistNewsletter.week_end_date.is_not(None),
            TeacherAssistNewsletter.week_end_date <= end_date,
        )
        .order_by(TeacherAssistNewsletter.week_end_date.asc())
        .limit(10)
    ).all()
    for row in newsletters:
        events.append(
            {
                "event_type": "newsletter",
                "title": row.title,
                "event_date": row.week_end_date.isoformat() if row.week_end_date else None,
                "navigation_href": f"/teacher-assist/newsletters?id={row.id}",
            }
        )

    reteach_plans = db.scalars(
        select(TeacherAssistReteachPlan)
        .where(
            TeacherAssistReteachPlan.tenant_id == tenant_id,
            TeacherAssistReteachPlan.owner_user_id == user_id,
        )
        .order_by(TeacherAssistReteachPlan.updated_at.desc())
        .limit(5)
    ).all()
    for row in reteach_plans:
        events.append(
            {
                "event_type": "reteach",
                "title": row.title,
                "event_date": row.updated_at.date().isoformat(),
                "navigation_href": f"/teacher-assist/reteach-plans?id={row.id}",
            }
        )

    workflows = db.scalars(
        select(TeacherAssistWorkflow)
        .where(
            TeacherAssistWorkflow.tenant_id == tenant_id,
            TeacherAssistWorkflow.user_id == user_id,
            TeacherAssistWorkflow.status.in_(("queued", "running", "failed")),
        )
        .order_by(TeacherAssistWorkflow.updated_at.desc())
        .limit(5)
    ).all()
    for row in workflows:
        events.append(
            {
                "event_type": "workflow",
                "title": f"{row.workflow_type} ({row.status})",
                "event_date": row.updated_at.date().isoformat(),
                "navigation_href": "/teacher-assist/exports",
            }
        )

    events.sort(key=lambda row: row.get("event_date") or "")
    return events


def build_home_mastery_alerts(
    db: Session,
    *,
    settings: Settings,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
) -> list[dict[str, Any]]:
    workspace_payload = get_teacher_assist_workspace(
        db,
        settings=settings,
        tenant_id=tenant_id,
        user_id=user_id,
    )
    dashboard = build_mastery_dashboard(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        school_year_id=(
            workspace_payload["current_school_year"].id
            if workspace_payload.get("current_school_year") is not None
            else None
        ),
        grading_period_id=(
            workspace_payload["active_grading_period"].id
            if workspace_payload.get("active_grading_period") is not None
            else None
        ),
        settings=settings,
    )
    alerts: list[dict[str, Any]] = []
    for item in dashboard.get("reteach_recommended_standards", [])[:8]:
        alerts.append(
            {
                "alert_type": "reteach_recommended",
                "title": item.get("standard_code") or "Standard needs reteach",
                "description": item.get("matrix_title"),
                "navigation_href": f"/teacher-assist/mastery?id={item.get('mastery_matrix_id')}",
            }
        )
    for item in dashboard.get("unassessed_standards", [])[:5]:
        alerts.append(
            {
                "alert_type": "unassessed_standard",
                "title": item.get("standard_code") or "Unassessed standard",
                "description": item.get("matrix_title"),
                "navigation_href": f"/teacher-assist/mastery?id={item.get('mastery_matrix_id')}",
            }
        )
    return alerts


def build_home_quick_actions() -> list[dict[str, Any]]:
    mapping = {
        "lesson": ("Create lesson", "/teacher-assist/weekly-planning"),
        "assignment": ("Create assignment", "/teacher-assist/assignments"),
        "quiz": ("Create quiz", "/teacher-assist/exports"),
        "reteach_plan": ("Create reteach plan", "/teacher-assist/reteach-plans"),
        "newsletter": ("Create newsletter", "/teacher-assist/newsletters"),
    }
    return [
        {
            "action_key": key,
            "label": mapping[key][0],
            "navigation_href": mapping[key][1],
        }
        for key in TEACHER_ASSIST_QUICK_CREATE_ACTIONS
        if key in mapping
    ]


def build_home_this_week(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
) -> dict[str, Any]:
    week_end = date.today() + timedelta(days=7)
    assignment_count = len(
        db.scalars(
            select(TeacherAssistAssignment).where(
                TeacherAssistAssignment.tenant_id == tenant_id,
                TeacherAssistAssignment.teacher_user_id == user_id,
                TeacherAssistAssignment.due_date.is_not(None),
                TeacherAssistAssignment.due_date <= week_end,
            )
        ).all()
    )
    plan_count = len(
        db.scalars(
            select(TeacherAssistWeeklyPlan).where(
                TeacherAssistWeeklyPlan.tenant_id == tenant_id,
                TeacherAssistWeeklyPlan.user_id == user_id,
                TeacherAssistWeeklyPlan.status == "completed",
            )
        ).all()
    )
    return {
        "assignments_due_count": assignment_count,
        "completed_plans_count": plan_count,
        "week_end_date": week_end.isoformat(),
    }


def get_teacher_assist_home_workspace(
    db: Session,
    *,
    settings: Settings,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
) -> dict[str, Any]:
    preferences = get_user_preferences_or_create(db, tenant_id=tenant_id, user_id=user_id)
    onboarding = build_onboarding_progress(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        preferences=preferences,
    )
    today = get_teacher_assist_today_workspace(
        db,
        settings=settings,
        tenant_id=tenant_id,
        user_id=user_id,
    )
    priorities = build_home_priorities(
        db,
        settings=settings,
        tenant_id=tenant_id,
        user_id=user_id,
    )
    return {
        "summary": today.get("summary") or {},
        "priorities": priorities,
        "classes": build_home_classes(db, settings=settings, tenant_id=tenant_id, user_id=user_id),
        "this_week": build_home_this_week(db, tenant_id=tenant_id, user_id=user_id),
        "mastery_alerts": build_home_mastery_alerts(
            db,
            settings=settings,
            tenant_id=tenant_id,
            user_id=user_id,
        ),
        "quick_actions": build_home_quick_actions(),
        "shortcuts": build_teacher_shortcuts(
            db,
            tenant_id=tenant_id,
            user_id=user_id,
            preferences=preferences,
        ),
        "timeline": build_home_timeline(db, tenant_id=tenant_id, user_id=user_id),
        "recent_activity": today.get("recent_activity") or [],
        "onboarding": onboarding,
        "preferences": {
            "preferred_landing": preferences.preferred_landing,
            "last_class_id": preferences.last_class_id,
            "last_subject_id": preferences.last_subject_id,
            "last_grading_period_id": preferences.last_grading_period_id,
        },
        "read_only": True,
    }
