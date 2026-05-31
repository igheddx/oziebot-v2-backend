"""Recommendation engine v2 — teacher-reviewed instructional loop suggestions."""

from __future__ import annotations

import uuid
from typing import Any

from sqlalchemy.orm import Session

from oziebot_api.services.teacher_assist.constants import instructional_week_href
from oziebot_api.services.teacher_assist.objective_performance import ObjectivePerformanceService
from oziebot_api.services.teacher_assist.reteach_plans import list_reteach_plans
from oziebot_api.services.teacher_assist.student_support_groups import list_support_groups


def build_instructional_loop_recommendations(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    class_id: uuid.UUID | None = None,
    subject_id: uuid.UUID | None = None,
    instructional_week_id: uuid.UUID | None = None,
) -> dict[str, Any]:
    performance = ObjectivePerformanceService.calculate_for_scope(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        class_id=class_id,
        subject_id=subject_id,
        instructional_week_id=instructional_week_id,
    )
    recommendations: list[dict[str, Any]] = []

    for row in performance.get("weakest_objectives") or []:
        if (row.get("mastery_pct") or 0) >= 50:
            continue
        recommendations.append(
            {
                "recommendation_key": "objective_below_threshold",
                "title": f"Review {row.get('objective_code') or 'objective'} mastery",
                "description": f"Mastery is {row.get('mastery_pct')}% — below the 50% review threshold.",
                "action_key": "create_reteach_plan",
                "navigation_href": "/teacher-assist/reteach",
                "requires_teacher_review": True,
            }
        )
        recommendations.append(
            {
                "recommendation_key": "create_reteach_plan",
                "title": f"Create reteach plan for {row.get('objective_code') or 'objective'}",
                "description": "Draft an intervention plan for students who need additional support.",
                "action_key": "create_reteach_plan",
                "navigation_href": "/teacher-assist/reteach-plans",
                "requires_teacher_review": True,
            }
        )

    open_plans = [
        row
        for row in list_reteach_plans(db, tenant_id=tenant_id, user_id=user_id)
        if row.status in {"draft", "teacher_review", "active"}
    ]
    if open_plans:
        recommendations.append(
            {
                "recommendation_key": "review_open_reteach_plans",
                "title": f"Review {len(open_plans)} open reteach plan(s)",
                "description": "Continue intervention planning for objectives needing support.",
                "action_key": "review_reteach_plans",
                "navigation_href": "/teacher-assist/reteach-plans",
                "requires_teacher_review": True,
            }
        )

    draft_groups = list_support_groups(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        class_id=class_id,
        instructional_week_id=instructional_week_id,
        status="draft",
    )
    if draft_groups:
        recommendations.append(
            {
                "recommendation_key": "review_support_group",
                "title": f"Review {len(draft_groups)} suggested support group(s)",
                "description": "Confirm student groupings before activating reteach support.",
                "action_key": "review_support_groups",
                "navigation_href": "/teacher-assist/reteach",
                "requires_teacher_review": True,
            }
        )

    if instructional_week_id is not None:
        recommendations.append(
            {
                "recommendation_key": "prepare_reassessment",
                "title": "Prepare reassessment checkpoint",
                "description": "Schedule a short reassessment after reteach to measure improvement.",
                "action_key": "prepare_reassessment",
                "navigation_href": instructional_week_href(str(instructional_week_id), tab="assessments"),
                "requires_teacher_review": True,
            }
        )

    return {
        "recommended_actions": recommendations[:12],
        "requires_teacher_review": True,
        "read_only": True,
    }
