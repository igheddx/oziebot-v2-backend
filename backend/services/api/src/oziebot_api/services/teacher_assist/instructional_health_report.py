"""Instructional health reporting — exportable instructional loop snapshot."""

from __future__ import annotations

import uuid
from typing import Any

from sqlalchemy.orm import Session

from oziebot_api.services.teacher_assist.assignment_coverage import build_assignment_coverage_view
from oziebot_api.services.teacher_assist.instructional_reflections import list_instructional_reflections, serialize_instructional_reflection
from oziebot_api.services.teacher_assist.instructional_week_closure import get_or_create_week_closure, serialize_week_closure
from oziebot_api.services.teacher_assist.mastery_dashboard_v2 import build_mastery_dashboard_v2
from oziebot_api.services.teacher_assist.objective_performance import ObjectivePerformanceService
from oziebot_api.services.teacher_assist.reteach_workspace import build_reteach_workspace
from oziebot_api.services.teacher_assist.student_support_groups import list_support_groups, serialize_support_group


def build_instructional_health_report(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    class_id: uuid.UUID | None = None,
    subject_id: uuid.UUID | None = None,
    instructional_week_id: uuid.UUID | None = None,
    school_year_id: uuid.UUID | None = None,
    grading_period_id: uuid.UUID | None = None,
) -> dict[str, Any]:
    performance = ObjectivePerformanceService.calculate_for_scope(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        class_id=class_id,
        subject_id=subject_id,
        instructional_week_id=instructional_week_id,
        school_year_id=school_year_id,
        grading_period_id=grading_period_id,
    )
    dashboard = build_mastery_dashboard_v2(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        class_id=class_id,
        subject_id=subject_id,
        instructional_week_id=instructional_week_id,
        school_year_id=school_year_id,
        grading_period_id=grading_period_id,
    )
    coverage = build_assignment_coverage_view(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        instructional_week_id=instructional_week_id,
        class_id=class_id,
    )
    reteach = build_reteach_workspace(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        class_id=class_id,
        subject_id=subject_id,
        instructional_week_id=instructional_week_id,
    )
    groups = list_support_groups(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        class_id=class_id,
        instructional_week_id=instructional_week_id,
    )
    reflections = list_instructional_reflections(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        instructional_week_id=instructional_week_id,
    )
    closure = None
    if instructional_week_id is not None:
        closure = serialize_week_closure(
            get_or_create_week_closure(
                db,
                tenant_id=tenant_id,
                user_id=user_id,
                instructional_week_id=instructional_week_id,
            )
        )

    return {
        "scope": performance.get("scope"),
        "objective_coverage": dashboard.get("v2", {}).get("objective_coverage"),
        "mastery_levels": performance.get("objectives") or [],
        "support_groups": [serialize_support_group(row) for row in groups],
        "reteach_status": {
            "objectives_requiring_reteach": reteach.get("objectives_requiring_reteach") or [],
            "open_reteach_plans": reteach.get("reteach_plans") or [],
        },
        "week_progress": coverage.get("summary"),
        "assignment_coverage": coverage.get("assignments") or [],
        "teacher_reflections": [serialize_instructional_reflection(row) for row in reflections],
        "week_closure": closure,
        "exportable": True,
        "read_only": True,
    }
