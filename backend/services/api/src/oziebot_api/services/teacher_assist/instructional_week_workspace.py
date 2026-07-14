from __future__ import annotations

import uuid
from typing import Any

from sqlalchemy import or_, select
from sqlalchemy.orm import Session

from oziebot_api.models.teacher_assist_activity_event import TeacherAssistActivityEvent
from oziebot_api.models.teacher_assist_assignment import TeacherAssistAssignment
from oziebot_api.models.teacher_assist_newsletter import TeacherAssistNewsletter
from oziebot_api.models.teacher_assist_weekly_plan import TeacherAssistWeeklyPlan
from oziebot_api.models.user import User
from oziebot_api.services.teacher_assist.constants import instructional_week_href
from oziebot_api.services.teacher_assist.current_week_resolver import build_objective_coverage
from oziebot_api.services.teacher_assist.generated_artifacts import (
    build_generation_history,
    list_generated_artifacts_for_period,
    serialize_generated_artifact,
)
from oziebot_api.services.teacher_assist.instructional_weeks import (
    get_instructional_week,
    serialize_instructional_week,
    serialize_instructional_week_objective,
)
from oziebot_api.services.teacher_assist.recommendation_service import build_week_recommendations
from oziebot_api.services.teacher_assist.week_context_service import WeekContextService


def _lesson_href(plan_id: uuid.UUID) -> str:
    return f"/teacher-assist/weekly-planning/plans?id={plan_id}"


def _assignment_href(assignment_id: uuid.UUID) -> str:
    return f"/teacher-assist/assignments?id={assignment_id}"


def _newsletter_href(newsletter_id: uuid.UUID) -> str:
    return f"/teacher-assist/newsletters?id={newsletter_id}"


def _build_health_indicators(
    *,
    objectives_count: int,
    lessons_count: int,
    assignments_count: int,
    assessments_count: int,
    resources_count: int,
    newsletter_count: int,
    mastery_covered: int,
) -> dict[str, Any]:
    return {
        "objectives_covered": objectives_count > 0,
        "assignments_created": assignments_count > 0,
        "assessments_created": assessments_count > 0,
        "mastery_coverage": mastery_covered > 0,
        "resources_attached": resources_count > 0,
        "newsletter_ready": newsletter_count > 0,
        "lessons_created": lessons_count > 0,
        "summary": {
            "objectives_count": objectives_count,
            "lessons_count": lessons_count,
            "assignments_count": assignments_count,
            "assessments_count": assessments_count,
            "resources_count": resources_count,
            "newsletter_count": newsletter_count,
            "mastery_covered_count": mastery_covered,
        },
    }


def _build_action_center(
    *,
    health: dict[str, Any],
    instructional_week_id: uuid.UUID,
    pacing_guide_period_id: uuid.UUID,
    recommendations: dict[str, Any],
) -> list[dict[str, Any]]:
    actions: list[dict[str, Any]] = []
    if not health["lessons_created"]:
        actions.append(
            {
                "action_key": "generate_lesson_plan",
                "label": "Generate lesson plan",
                "navigation_href": f"/teacher-assist/planning/weeks?period_id={pacing_guide_period_id}&action=instructional_plan",
            }
        )
    if not health["assignments_created"]:
        actions.append(
            {
                "action_key": "create_assignment",
                "label": "Create assignment",
                "navigation_href": f"/teacher-assist/planning/weeks?period_id={pacing_guide_period_id}&action=assignment",
            }
        )
    if not health["assessments_created"]:
        actions.append(
            {
                "action_key": "create_assessment",
                "label": "Create assessment",
                "navigation_href": f"/teacher-assist/planning/weeks?period_id={pacing_guide_period_id}&action=quiz",
            }
        )
    if not health["newsletter_ready"]:
        actions.append(
            {
                "action_key": "prepare_newsletter",
                "label": "Prepare newsletter",
                "navigation_href": f"/teacher-assist/planning/weeks?period_id={pacing_guide_period_id}&action=newsletter",
            }
        )
    if not health["mastery_coverage"]:
        actions.append(
            {
                "action_key": "review_mastery",
                "label": "Review mastery",
                "navigation_href": "/teacher-assist/mastery",
            }
        )
    top_reuse = recommendations.get("recommended_for_this_week", {}).get("top_reusable", [])
    if top_reuse:
        first = top_reuse[0]
        actions.append(
            {
                "action_key": "reuse_prior_work",
                "label": "Reuse prior year assignment",
                "navigation_href": first.get("navigation_href")
                or instructional_week_href(str(instructional_week_id), tab="recommendations"),
            }
        )
    actions.append(
        {
            "action_key": "generate_next_week",
            "label": "Generate next week",
            "navigation_href": instructional_week_href(
                str(instructional_week_id), action="generate_next_week"
            ),
        }
    )
    return actions


def build_instructional_week_workspace(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user: User,
    instructional_week_id: uuid.UUID,
) -> dict[str, Any]:
    week = get_instructional_week(
        db, tenant_id=tenant_id, user_id=user.id, instructional_week_id=instructional_week_id
    )
    week_context = WeekContextService.build(
        db, tenant_id=tenant_id, user=user, period_id=week.pacing_guide_period_id
    )
    week_context_payload = WeekContextService.serialize(week_context)
    objectives = [
        serialize_instructional_week_objective(row) for row in week.objectives if row.is_active
    ]
    if not objectives:
        objectives = week_context_payload.get("objectives", [])

    plans = list(
        db.scalars(
            select(TeacherAssistWeeklyPlan).where(
                TeacherAssistWeeklyPlan.tenant_id == tenant_id,
                or_(
                    TeacherAssistWeeklyPlan.instructional_week_id == week.id,
                    TeacherAssistWeeklyPlan.pacing_guide_period_id == week.pacing_guide_period_id,
                ),
            )
        ).all()
    )
    assignments = list(
        db.scalars(
            select(TeacherAssistAssignment).where(
                TeacherAssistAssignment.tenant_id == tenant_id,
                TeacherAssistAssignment.teacher_user_id == user.id,
                or_(
                    TeacherAssistAssignment.instructional_week_id == week.id,
                    TeacherAssistAssignment.pacing_guide_period_id == week.pacing_guide_period_id,
                ),
            )
        ).all()
    )
    newsletters = list(
        db.scalars(
            select(TeacherAssistNewsletter).where(
                TeacherAssistNewsletter.tenant_id == tenant_id,
                TeacherAssistNewsletter.owner_user_id == user.id,
                or_(
                    TeacherAssistNewsletter.instructional_week_id == week.id,
                    TeacherAssistNewsletter.pacing_guide_period_id == week.pacing_guide_period_id,
                ),
            )
        ).all()
    )
    artifacts = list_generated_artifacts_for_period(
        db,
        tenant_id=tenant_id,
        user_id=user.id,
        pacing_guide_period_id=week.pacing_guide_period_id,
    )
    serialized_artifacts = [serialize_generated_artifact(db, row) for row in artifacts]
    lessons = [
        {
            "id": str(plan.id),
            "title": plan.title,
            "status": plan.status,
            "artifact_type": "LESSON_PLAN",
            "navigation_href": _lesson_href(plan.id),
        }
        for plan in plans
    ] + [row for row in serialized_artifacts if row["artifact_type"] == "LESSON_PLAN"]
    assignment_rows = [
        {
            "id": str(row.id),
            "title": row.title,
            "status": row.status,
            "artifact_type": "ASSIGNMENT",
            "navigation_href": _assignment_href(row.id),
        }
        for row in assignments
    ] + [row for row in serialized_artifacts if row["artifact_type"] == "ASSIGNMENT"]
    assessment_rows = [
        row for row in serialized_artifacts if row["artifact_type"] in {"QUIZ", "RUBRIC"}
    ] + [
        {
            "id": str(row.id),
            "title": row.title,
            "status": row.status,
            "artifact_type": row.assignment_type.upper(),
            "navigation_href": _assignment_href(row.id),
        }
        for row in assignments
        if row.assignment_type.lower() in {"quiz", "test", "exit_ticket"}
    ]
    newsletter_rows = [
        {
            "id": str(row.id),
            "title": row.title,
            "status": row.status,
            "artifact_type": "NEWSLETTER",
            "navigation_href": _newsletter_href(row.id),
        }
        for row in newsletters
    ] + [row for row in serialized_artifacts if row["artifact_type"] == "NEWSLETTER"]

    coverage = build_objective_coverage(
        db,
        tenant_id=tenant_id,
        user_id=user.id,
        guide_id=week.pacing_guide_id,
    )
    recommendations = build_week_recommendations(
        db, tenant_id=tenant_id, user=user, period_id=week.pacing_guide_period_id
    )
    resources = week_context_payload.get("resources", [])
    health = _build_health_indicators(
        objectives_count=len(objectives),
        lessons_count=len(lessons),
        assignments_count=len(assignment_rows),
        assessments_count=len(assessment_rows),
        resources_count=len(resources),
        newsletter_count=len(newsletter_rows),
        mastery_covered=len(
            [
                row
                for row in (coverage.get("objectives") or [])
                if row.get("coverage_status") == "covered"
            ]
        ),
    )
    history = build_generation_history(
        db,
        tenant_id=tenant_id,
        user_id=user.id,
        pacing_guide_period_id=week.pacing_guide_period_id,
    )
    activity = list(
        db.scalars(
            select(TeacherAssistActivityEvent)
            .where(
                TeacherAssistActivityEvent.tenant_id == tenant_id,
                TeacherAssistActivityEvent.user_id == user.id,
            )
            .order_by(TeacherAssistActivityEvent.created_at.desc())
            .limit(30)
        ).all()
    )
    timeline = [
        {
            "id": str(row.id),
            "event_type": row.event_type,
            "title": row.summary_text,
            "created_at": row.created_at.isoformat(),
            "navigation_href": instructional_week_href(str(week.id), tab="timeline"),
        }
        for row in history
    ] + [
        {
            "id": str(row.id),
            "event_type": row.event_type,
            "title": row.summary_text,
            "created_at": row.created_at.isoformat(),
            "navigation_href": instructional_week_href(str(week.id), tab="timeline"),
        }
        for row in activity
        if (row.metadata_json or {}).get("pacing_guide_period_id")
        == str(week.pacing_guide_period_id)
        or (row.metadata_json or {}).get("instructional_week_id") == str(week.id)
    ]
    timeline.sort(key=lambda row: row["created_at"], reverse=True)

    from oziebot_api.services.teacher_assist.assignment_coverage import (
        build_assignment_coverage_view,
    )
    from oziebot_api.services.teacher_assist.instructional_week_closure import (
        get_or_create_week_closure,
        serialize_week_closure,
    )
    from oziebot_api.services.teacher_assist.objective_performance import (
        ObjectivePerformanceService,
    )
    from oziebot_api.services.teacher_assist.recommendation_v2 import (
        build_instructional_loop_recommendations,
    )
    from oziebot_api.services.teacher_assist.reteach_workspace import build_reteach_workspace
    from oziebot_api.services.teacher_assist.student_support_groups import (
        list_support_groups,
        serialize_support_group,
    )

    loop_performance = ObjectivePerformanceService.calculate_for_scope(
        db,
        tenant_id=tenant_id,
        user_id=user.id,
        instructional_week_id=week.id,
    )
    reteach_workspace = build_reteach_workspace(
        db,
        tenant_id=tenant_id,
        user_id=user.id,
        instructional_week_id=week.id,
    )
    assignment_coverage = build_assignment_coverage_view(
        db,
        tenant_id=tenant_id,
        user_id=user.id,
        instructional_week_id=week.id,
    )
    support_groups = list_support_groups(
        db,
        tenant_id=tenant_id,
        user_id=user.id,
        instructional_week_id=week.id,
    )
    week_closure = serialize_week_closure(
        get_or_create_week_closure(
            db,
            tenant_id=tenant_id,
            user_id=user.id,
            instructional_week_id=week.id,
        )
    )
    loop_recommendations = build_instructional_loop_recommendations(
        db,
        tenant_id=tenant_id,
        user_id=user.id,
        instructional_week_id=week.id,
    )

    return {
        "instructional_week": serialize_instructional_week(week),
        "week_context": week_context_payload,
        "tabs": {
            "overview": {
                "title": week.title,
                "dates": {
                    "start_date": week.start_date.isoformat() if week.start_date else None,
                    "end_date": week.end_date.isoformat() if week.end_date else None,
                },
                "subject": week_context_payload.get("subject_name"),
                "grade": week_context_payload.get("grade_display_name")
                or week_context_payload.get("grade_level"),
                "objectives": objectives,
                "resources": resources,
                "mastery_goals": coverage,
                "notes": week.notes,
                "progress": health["summary"],
            },
            "lessons": {"items": lessons},
            "assignments": {"items": assignment_rows},
            "assessments": {"items": assessment_rows},
            "resources": {
                "items": resources,
                "curriculum_references": week_context_payload.get("curriculum_references", []),
                "teacher_resources": week_context_payload.get("teacher_resources", []),
                "external_links": week_context_payload.get("external_links", []),
                "recommendations": recommendations.get("recommended_for_this_week", {}).get(
                    "top_reusable", []
                ),
            },
            "newsletter": {"items": newsletter_rows},
            "mastery": {
                "objective_coverage": coverage,
                "expected_mastery": objectives,
                "assessment_coverage": assessment_rows,
                "performance": loop_performance,
                "reteach_needs": reteach_workspace.get("objectives_requiring_reteach") or [],
            },
            "timeline": {"items": timeline[:20]},
            "instructional_loop": {
                "mastery_results": loop_performance.get("objectives") or [],
                "reteach_needs": reteach_workspace.get("objectives_requiring_reteach") or [],
                "student_groups": [serialize_support_group(row) for row in support_groups],
                "assignment_coverage": assignment_coverage.get("assignments") or [],
                "week_closure": week_closure,
                "recommendations": loop_recommendations.get("recommended_actions") or [],
            },
        },
        "action_center": _build_action_center(
            health=health,
            instructional_week_id=week.id,
            pacing_guide_period_id=week.pacing_guide_period_id,
            recommendations=recommendations,
        ),
        "health_indicators": health,
        "recommendations": recommendations,
        "legacy_pacing_workspace_href": f"/teacher-assist/planning/weeks?period_id={week.pacing_guide_period_id}",
    }
