from __future__ import annotations

import uuid
from typing import Any

from sqlalchemy.orm import Session

from oziebot_api.models.user import User
from oziebot_api.services.teacher_assist.current_week_resolver import (
    _serialize_period,
    build_objective_coverage,
    build_pacing_guide_timeline,
)
from oziebot_api.services.teacher_assist.generated_artifacts import (
    build_generation_history,
    list_generated_artifacts_for_period,
    serialize_generated_artifact,
)
from oziebot_api.services.teacher_assist.pacing_guide_foundation import get_catalog_pacing_guide_detail
from oziebot_api.services.teacher_assist.instructional_weeks import find_instructional_week_for_period
from oziebot_api.services.teacher_assist.recommendation_service import build_week_recommendations
from oziebot_api.services.teacher_assist.week_context_service import WeekContextService


WEEK_GENERATION_ACTIONS = (
    {"action_key": "instructional_plan", "artifact_type": "LESSON_PLAN", "label": "Generate Instructional Plan"},
    {"action_key": "lesson_plan", "artifact_type": "LESSON_PLAN", "label": "Generate Lesson Plan"},
    {"action_key": "assignment", "artifact_type": "ASSIGNMENT", "label": "Generate Assignment"},
    {"action_key": "quiz", "artifact_type": "QUIZ", "label": "Generate Quiz"},
    {"action_key": "rubric", "artifact_type": "RUBRIC", "label": "Generate Rubric"},
    {"action_key": "newsletter", "artifact_type": "NEWSLETTER", "label": "Generate Newsletter"},
    {"action_key": "parent_communication", "artifact_type": "PARENT_COMMUNICATION", "label": "Draft Parent Communication"},
)


def _week_action_href(period_id: uuid.UUID, action_key: str) -> str:
    return f"/teacher-assist/planning/weeks?period_id={period_id}&action={action_key}"


def build_week_workspace(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user: User,
    period_id: uuid.UUID,
) -> dict[str, Any]:
    week_context_dto = WeekContextService.build(db, tenant_id=tenant_id, user=user, period_id=period_id)
    week_context = WeekContextService.serialize(week_context_dto)
    detail = get_catalog_pacing_guide_detail(
        db,
        tenant_id=tenant_id,
        pacing_guide_id=week_context_dto.pacing_guide_id,
    )
    selected_period = next((row for row in detail.periods if row.id == period_id), None)
    timeline = build_pacing_guide_timeline(detail.periods, current_period_id=period_id)
    coverage = build_objective_coverage(
        db,
        tenant_id=tenant_id,
        user_id=user.id,
        guide_id=week_context_dto.pacing_guide_id,
    )
    artifacts = list_generated_artifacts_for_period(
        db,
        tenant_id=tenant_id,
        user_id=user.id,
        pacing_guide_period_id=period_id,
    )
    serialized_artifacts = [serialize_generated_artifact(db, row) for row in artifacts]
    history = build_generation_history(
        db,
        tenant_id=tenant_id,
        user_id=user.id,
        pacing_guide_period_id=period_id,
    )
    period_payload = _serialize_period(
        db,
        tenant_id=tenant_id,
        user_id=user.id,
        period=selected_period,
    )
    recommendations = build_week_recommendations(db, tenant_id=tenant_id, user=user, period_id=period_id)
    instructional_week = find_instructional_week_for_period(
        db, tenant_id=tenant_id, user_id=user.id, pacing_guide_period_id=period_id
    )
    instructional_week_payload = (
        {
            "id": str(instructional_week.id),
            "navigation_href": f"/teacher-assist/week/{instructional_week.id}",
            "status": instructional_week.status,
        }
        if instructional_week is not None
        else {
            "create_href": f"/teacher-assist/planning/weeks?period_id={period_id}&action=create_instructional_week",
        }
    )
    return {
        "week_context": week_context,
        "period": period_payload,
        "timeline": timeline,
        "objective_coverage": coverage,
        "generated_artifacts": serialized_artifacts,
        "generation_history": history,
        "week_actions": [
            {
                **action,
                "navigation_href": _week_action_href(period_id, action["action_key"]),
            }
            for action in WEEK_GENERATION_ACTIONS
        ]
        + [
            {
                "action_key": "generate_next_week",
                "artifact_type": "NEXT_WEEK",
                "label": "Generate Next Week",
                "navigation_href": f"/teacher-assist/planning/weeks?period_id={period_id}&action=generate_next_week",
            },
            {
                "action_key": "duplicate_week",
                "artifact_type": "WEEK",
                "label": "Duplicate Week",
                "navigation_href": f"/teacher-assist/planning/weeks?period_id={period_id}&action=duplicate_week",
            },
            {
                "action_key": "save_template",
                "artifact_type": "TEMPLATE",
                "label": "Save Week as Template",
                "navigation_href": f"/teacher-assist/planning/weeks?period_id={period_id}&action=save_template",
            },
            {
                "action_key": "create_instructional_week",
                "artifact_type": "INSTRUCTIONAL_WEEK",
                "label": "Create Instructional Week",
                "navigation_href": f"/teacher-assist/planning/weeks?period_id={period_id}&action=create_instructional_week",
            },
        ],
        "artifact_library": {
            "lesson_plans": [row for row in serialized_artifacts if row["artifact_type"] == "LESSON_PLAN"],
            "assignments": [row for row in serialized_artifacts if row["artifact_type"] == "ASSIGNMENT"],
            "quizzes": [row for row in serialized_artifacts if row["artifact_type"] == "QUIZ"],
            "rubrics": [row for row in serialized_artifacts if row["artifact_type"] == "RUBRIC"],
            "newsletters": [row for row in serialized_artifacts if row["artifact_type"] == "NEWSLETTER"],
            "parent_communications": [
                row for row in serialized_artifacts if row["artifact_type"] == "PARENT_COMMUNICATION"
            ],
        },
        "recommendations": recommendations,
        "instructional_week": instructional_week_payload,
    }
