from __future__ import annotations

from datetime import date, timedelta
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
from oziebot_api.services.teacher_assist.constants import (
    class_workspace_href,
    instructional_week_href,
    weekly_planning_href,
)
from oziebot_api.services.teacher_assist.mastery_dashboard import build_mastery_dashboard
from oziebot_api.services.teacher_assist.teacher_shortcuts import build_teacher_shortcuts
from oziebot_api.services.teacher_assist.today_workspace import get_teacher_assist_today_workspace
from oziebot_api.services.teacher_assist.current_week_resolver import build_current_week_payload
from oziebot_api.services.teacher_assist.user_preferences import (
    build_onboarding_progress,
    get_user_preferences_or_create,
)
from oziebot_api.services.teacher_assist.work_queue import (
    PRIORITY_LEVEL_BY_SEVERITY,
)
from oziebot_api.services.teacher_assist.instructional_weeks import (
    find_instructional_week_for_period,
)
from oziebot_api.services.teacher_assist.week_context_service import WeekContextService
from oziebot_api.services.teacher_assist.recommendation_v2 import (
    build_instructional_loop_recommendations,
)
from oziebot_api.services.teacher_assist.objective_performance import ObjectivePerformanceService
from oziebot_api.services.teacher_assist.reteach_plans import list_reteach_plans
from oziebot_api.services.teacher_assist.instructional_week_closure import (
    get_or_create_week_closure,
    serialize_week_closure,
)
from oziebot_api.services.teacher_assist.teacher_copilot_service import get_suggested_questions
from oziebot_api.services.teacher_assist.recommendation_service import build_week_recommendations
from oziebot_api.services.teacher_assist.teacher_efficiency import (
    build_home_time_savings_summary,
    build_teacher_efficiency_dashboard,
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
            (
                row
                for row in action_payload.get("class_rollups", [])
                if row.get("class_id") == class_id
            ),
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
                "pending_reviews": (rollup or {}).get("grading_count", 0)
                + (rollup or {}).get("extraction_count", 0),
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
                "navigation_href": class_workspace_href(str(class_id)),
                "actions": [
                    {"label": "Open class", "href": class_workspace_href(str(class_id))},
                    {
                        "label": "Assignments",
                        "href": f"/teacher-assist/assignments?class_id={class_id}",
                    },
                    {"label": "Mastery", "href": f"/teacher-assist/mastery?class_id={class_id}"},
                    {
                        "label": "Reteach",
                        "href": f"/teacher-assist/reteach-plans?class_id={class_id}",
                    },
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


def build_home_quick_actions(current_week: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    """Pacing-first workflow actions only — no standalone generate shortcuts on Home."""
    has_active_guide = bool(current_week and current_week.get("has_active_guide"))
    period_id = (current_week.get("current_week") or {}).get("id") if current_week else None

    if not has_active_guide or period_id is None:
        return [
            {
                "action_key": "browse_pacing_guides",
                "label": "Browse pacing guides",
                "navigation_href": "/teacher-assist/pacing-guides",
            },
            {
                "action_key": "open_pacing_workspace",
                "label": "Open pacing workspace",
                "navigation_href": "/teacher-assist/planning/pacing-guides/workspace",
            },
        ]

    week_base = weekly_planning_href(str(period_id))
    return [
        {
            "action_key": "weekly_planning",
            "label": "Plan this week",
            "navigation_href": f"{week_base}&action=instructional_plan",
        },
        {
            "action_key": "upload_resources",
            "label": "Upload supporting materials",
            "navigation_href": f"{week_base}&tab=resources",
        },
        {
            "action_key": "open_pacing_workspace",
            "label": "Open pacing workspace",
            "navigation_href": "/teacher-assist/planning/pacing-guides/workspace",
        },
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


def _build_recently_used_resources(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    period_id: uuid.UUID | None,
    instructional_week_id: str | None,
) -> list[dict[str, Any]]:
    if period_id is None:
        return []
    from oziebot_api.models.user import User

    user = db.get(User, user_id)
    if user is None:
        return []
    week_context = WeekContextService.serialize(
        WeekContextService.build(db, tenant_id=tenant_id, user=user, period_id=period_id)
    )
    navigation_href = (
        instructional_week_href(str(instructional_week_id), tab="resources")
        if instructional_week_id
        else weekly_planning_href(str(period_id), focus="resources")
    )
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in (week_context.get("resources") or []) + (
        week_context.get("teacher_resources") or []
    ):
        title = row.get("title")
        if not title:
            continue
        key = f"{title}:{row.get('resource_type')}"
        if key in seen:
            continue
        seen.add(key)
        rows.append(
            {
                "title": title,
                "resource_type": row.get("resource_type"),
                "navigation_href": navigation_href,
            }
        )
    return rows[:6]


def build_home_instructional_loop(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    instructional_week_id: str | None,
) -> dict[str, Any]:
    week_uuid = uuid.UUID(instructional_week_id) if instructional_week_id else None
    performance = ObjectivePerformanceService.calculate_for_scope(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        instructional_week_id=week_uuid,
    )
    recommendations = build_instructional_loop_recommendations(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        instructional_week_id=week_uuid,
    )
    open_reteach = [
        row
        for row in list_reteach_plans(db, tenant_id=tenant_id, user_id=user_id)
        if row.status in {"draft", "teacher_review", "active"}
    ]
    closure = None
    if week_uuid is not None:
        closure = serialize_week_closure(
            get_or_create_week_closure(
                db,
                tenant_id=tenant_id,
                user_id=user_id,
                instructional_week_id=week_uuid,
            )
        )
    objectives_attention = [
        row
        for row in performance.get("objectives") or []
        if (row.get("mastery_pct") or 0) < 50 and (row.get("students_assessed") or 0) > 0
    ]
    return {
        "students_needing_support": performance.get("students_needing_support") or [],
        "objectives_requiring_attention": objectives_attention[:5],
        "open_reteach_plans": [
            {
                "id": str(row.id),
                "title": row.title,
                "status": row.status,
                "navigation_href": "/teacher-assist/reteach-plans",
            }
            for row in open_reteach[:5]
        ],
        "recent_mastery_changes": [
            {
                "objective_code": row.get("objective_code"),
                "trend_direction": row.get("trend_direction"),
                "mastery_pct": row.get("mastery_pct"),
            }
            for row in performance.get("objectives") or []
            if row.get("trend_direction") in {"improving", "declining"}
        ][:5],
        "week_closure_status": closure,
        "instructional_health": {
            "objectives_assessed": len(performance.get("objectives") or []),
            "students_needing_support_count": len(
                performance.get("students_needing_support") or []
            ),
            "open_reteach_plan_count": len(open_reteach),
        },
        "loop_recommendations": recommendations.get("recommended_actions") or [],
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
    current_week = build_current_week_payload(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        require_explicit_guide_selection=True,
    )
    period_id = (
        (current_week.get("current_week") or {}).get("id")
        if current_week.get("has_active_guide")
        else None
    )
    upcoming_period_id = (
        (current_week.get("upcoming_week") or {}).get("id")
        if current_week.get("has_active_guide")
        else None
    )
    instructional_week_id = None
    upcoming_instructional_week_id = None
    if period_id is not None:
        instructional_week = find_instructional_week_for_period(
            db,
            tenant_id=tenant_id,
            user_id=user_id,
            pacing_guide_period_id=uuid.UUID(str(period_id)),
        )
        if instructional_week is not None:
            instructional_week_id = str(instructional_week.id)
    if upcoming_period_id is not None:
        upcoming_instructional_week = find_instructional_week_for_period(
            db,
            tenant_id=tenant_id,
            user_id=user_id,
            pacing_guide_period_id=uuid.UUID(str(upcoming_period_id)),
        )
        if upcoming_instructional_week is not None:
            upcoming_instructional_week_id = str(upcoming_instructional_week.id)
    recommended_reuse = []
    if onboarding["is_complete"] and current_week.get("has_active_guide") and period_id is not None:
        from oziebot_api.models.user import User

        user = db.get(User, user_id)
        if user is not None:
            recommended_reuse = (
                build_week_recommendations(db, tenant_id=tenant_id, user=user, period_id=period_id)
                .get("recommended_for_this_week", {})
                .get("top_reusable", [])
            )
    if onboarding["is_complete"] and current_week.get("has_active_guide"):
        efficiency = build_teacher_efficiency_dashboard(db, tenant_id=tenant_id, user_id=user_id)
        time_savings = build_home_time_savings_summary(db, tenant_id=tenant_id, user_id=user_id)
        instructional_loop = build_home_instructional_loop(
            db,
            tenant_id=tenant_id,
            user_id=user_id,
            instructional_week_id=instructional_week_id,
        )
        recently_used_resources = _build_recently_used_resources(
            db,
            tenant_id=tenant_id,
            user_id=user_id,
            period_id=uuid.UUID(str(period_id)) if period_id else None,
            instructional_week_id=instructional_week_id,
        )
    else:
        efficiency = {
            "estimated_hours_saved": 0,
            "reuse_rate_percent": 0,
            "recent_templates": [],
        }
        time_savings = {"time_saved_this_year_hours": 0.0, "time_saved_this_year_minutes": 0}
        instructional_loop = {
            "objectives_requiring_attention": [],
            "students_needing_support": [],
            "loop_recommendations": [],
            "instructional_health": {},
            "week_closure_status": None,
        }
        recently_used_resources = []
    return {
        "summary": today.get("summary") or {},
        "priorities": priorities,
        "classes": build_home_classes(db, settings=settings, tenant_id=tenant_id, user_id=user_id),
        "this_week": build_home_this_week(db, tenant_id=tenant_id, user_id=user_id),
        "current_week": current_week,
        "mastery_alerts": build_home_mastery_alerts(
            db,
            settings=settings,
            tenant_id=tenant_id,
            user_id=user_id,
        ),
        "quick_actions": build_home_quick_actions(current_week),
        "continue_planning": {
            "current_week_href": (
                weekly_planning_href(str(period_id))
                if period_id
                else "/teacher-assist/planning/pacing-guides/workspace"
            ),
            "instructional_week_href": instructional_week_href(str(instructional_week_id))
            if instructional_week_id
            else None,
            "create_instructional_week_href": None,
            "generate_next_week_href": (
                weekly_planning_href(str(upcoming_period_id)) if upcoming_period_id else None
            ),
            "template_library_href": "/teacher-assist/planning/templates",
            "upcoming_instructional_week_href": (
                weekly_planning_href(str(upcoming_period_id)) if upcoming_period_id else None
            ),
        },
        "instructional_week_id": instructional_week_id,
        "upcoming_instructional_week_id": upcoming_instructional_week_id,
        "recently_used_resources": recently_used_resources,
        "instructional_loop": instructional_loop,
        "copilot": {
            "href": "/teacher-assist/copilot",
            "suggested_questions": get_suggested_questions(is_root_admin=False)[:6],
            "weekly_summary_href": (
                "/teacher-assist/copilot?prompt=Summarize+this+week"
                if instructional_week_id
                else "/teacher-assist/copilot"
            ),
            "objectives_requiring_attention": instructional_loop.get(
                "objectives_requiring_attention"
            )
            or [],
            "students_needing_support": (instructional_loop.get("students_needing_support") or [])[
                :5
            ],
            "suggested_actions": (instructional_loop.get("loop_recommendations") or [])[:4],
            "instructional_health": instructional_loop.get("instructional_health") or {},
        },
        "recommended_reuse": recommended_reuse,
        "time_savings": time_savings,
        "efficiency_summary": {
            "estimated_hours_saved": efficiency.get("estimated_hours_saved"),
            "reuse_rate_percent": efficiency.get("reuse_rate_percent"),
            "recent_templates": efficiency.get("recent_templates", []),
        },
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
            "active_pacing_guide_id": preferences.active_pacing_guide_id,
            "manual_pacing_period_id": preferences.manual_pacing_period_id,
        },
        "read_only": True,
    }
