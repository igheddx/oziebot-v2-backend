"""TeacherContextEngine — assembles grounded context packets for copilot."""

from __future__ import annotations

import uuid
from typing import Any

from sqlalchemy.orm import Session

from oziebot_api.config import Settings
from oziebot_api.models.user import User
from oziebot_api.services.teacher_assist.assignment_coverage import build_assignment_coverage_view
from oziebot_api.services.teacher_assist.current_week_resolver import build_current_week_payload
from oziebot_api.services.teacher_assist.instructional_reflections import (
    list_instructional_reflections,
    serialize_instructional_reflection,
)
from oziebot_api.services.teacher_assist.instructional_weeks import (
    find_instructional_week_for_period,
)
from oziebot_api.services.teacher_assist.mastery_dashboard_v2 import build_mastery_dashboard_v2
from oziebot_api.services.teacher_assist.objective_performance import ObjectivePerformanceService
from oziebot_api.services.teacher_assist.recommendation_v2 import (
    build_instructional_loop_recommendations,
)
from oziebot_api.services.teacher_assist.reteach_plans import (
    list_reteach_plans,
    serialize_reteach_plan,
)
from oziebot_api.services.teacher_assist.reteach_workspace import build_reteach_workspace
from oziebot_api.services.teacher_assist.student_support_groups import (
    list_support_groups,
    serialize_support_group,
)
from oziebot_api.services.teacher_assist.user_preferences import get_user_preferences_or_create
from oziebot_api.services.teacher_assist.recommendation_service import build_week_recommendations


def build_teacher_context(
    db: Session,
    *,
    settings: Settings | None,
    tenant_id: uuid.UUID,
    user: User,
    instructional_week_id: uuid.UUID | None = None,
) -> dict[str, Any]:
    preferences = get_user_preferences_or_create(db, tenant_id=tenant_id, user_id=user.id)
    current_week = build_current_week_payload(
        db,
        tenant_id=tenant_id,
        user_id=user.id,
        guide_id=preferences.active_pacing_guide_id,
    )
    period_id = (current_week.get("current_week") or {}).get("id")
    week_id = instructional_week_id
    if week_id is None and period_id is not None:
        week = find_instructional_week_for_period(
            db,
            tenant_id=tenant_id,
            user_id=user.id,
            pacing_guide_period_id=uuid.UUID(str(period_id)),
        )
        if week is not None:
            week_id = week.id

    performance = ObjectivePerformanceService.calculate_for_scope(
        db,
        tenant_id=tenant_id,
        user_id=user.id,
        instructional_week_id=week_id,
    )
    mastery_v2 = build_mastery_dashboard_v2(
        db,
        tenant_id=tenant_id,
        user_id=user.id,
        instructional_week_id=week_id,
        settings=settings,
    )
    reteach = build_reteach_workspace(
        db,
        tenant_id=tenant_id,
        user_id=user.id,
        instructional_week_id=week_id,
    )
    coverage = build_assignment_coverage_view(
        db,
        tenant_id=tenant_id,
        user_id=user.id,
        instructional_week_id=week_id,
    )
    reflections = [
        serialize_instructional_reflection(row)
        for row in list_instructional_reflections(
            db,
            tenant_id=tenant_id,
            user_id=user.id,
            instructional_week_id=week_id,
        )
    ]
    support_groups = [
        serialize_support_group(row)
        for row in list_support_groups(
            db,
            tenant_id=tenant_id,
            user_id=user.id,
            instructional_week_id=week_id,
        )
    ]
    reteach_plans = [
        serialize_reteach_plan(row)
        for row in list_reteach_plans(db, tenant_id=tenant_id, user_id=user.id)[:10]
    ]
    loop_recs = build_instructional_loop_recommendations(
        db,
        tenant_id=tenant_id,
        user_id=user.id,
        instructional_week_id=week_id,
    )
    resource_reuse = []
    if period_id is not None:
        resource_reuse = (
            build_week_recommendations(db, tenant_id=tenant_id, user=user, period_id=period_id)
            .get("recommended_for_this_week", {})
            .get("top_reusable", [])
        )

    packets = {
        "current_week": {
            "packet_type": "current_week_context",
            "has_active_guide": current_week.get("has_active_guide"),
            "current_week": current_week.get("current_week"),
            "upcoming_week": current_week.get("upcoming_week"),
            "instructional_week_id": str(week_id) if week_id else None,
        },
        "pacing_guide": {
            "packet_type": "current_pacing_guide_context",
            "pacing_guide": current_week.get("pacing_guide"),
            "school_year": current_week.get("school_year"),
            "grading_period": current_week.get("grading_period"),
        },
        "objectives": {
            "packet_type": "current_objective_context",
            "objectives": performance.get("objectives") or [],
            "weakest": performance.get("weakest_objectives") or [],
            "strongest": performance.get("strongest_objectives") or [],
        },
        "mastery": {
            "packet_type": "current_mastery_context",
            "performance": performance,
            "dashboard_v2": mastery_v2.get("v2"),
        },
        "reteach": {
            "packet_type": "current_reteach_context",
            "workspace": reteach,
            "plans": reteach_plans,
            "support_groups": support_groups,
        },
        "assessments": {
            "packet_type": "current_assessment_context",
            "assignment_coverage": coverage,
        },
        "resources": {
            "packet_type": "current_resource_context",
            "recommended_reuse": resource_reuse,
            "week_resources": (current_week.get("current_week") or {}).get("resources") or [],
        },
        "reflections": {
            "packet_type": "current_reflection_context",
            "items": reflections,
        },
        "recommendations": {
            "packet_type": "current_recommendation_context",
            "loop_recommendations": loop_recs.get("recommended_actions") or [],
        },
    }
    return {
        "teacher_id": str(user.id),
        "instructional_week_id": str(week_id) if week_id else None,
        "context_packets": packets,
        "read_only": True,
    }
