from __future__ import annotations

import uuid
from typing import Any

from sqlalchemy.orm import Session

from oziebot_api.models.user import User
from oziebot_api.services.teacher_assist.pacing_guide_foundation import PacingGuideRolloverService
from oziebot_api.services.teacher_assist.reuse_events import record_reuse_event
from oziebot_api.services.teacher_assist.time_savings_constants import TIME_SAVINGS_MINUTES
from oziebot_api.services.teacher_assist.week_duplication import duplicate_week
from oziebot_api.services.teacher_assist.workflow_service import curriculum_rollover_copy


def rollover_school_year_v2(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user: User,
    source_school_year_id: uuid.UUID,
    target_school_year_id: uuid.UUID,
    pacing_guide_ids: list[uuid.UUID] | None = None,
    period_ids: list[uuid.UUID] | None = None,
    copy_instructional_plans: bool = True,
    copy_assignments: bool = False,
    copy_quizzes: bool = False,
    copy_rubrics: bool = False,
    copy_resources: bool = True,
) -> dict[str, Any]:
    results: dict[str, Any] = {
        "pacing_guides": [],
        "weeks": [],
        "instructional_plans": [],
        "warnings": [],
    }
    if pacing_guide_ids:
        rolled = PacingGuideRolloverService.rollover_school_year(
            db,
            tenant_id=tenant_id,
            actor=user,
            source_school_year_id=source_school_year_id,
            target_school_year_id=target_school_year_id,
            guide_ids=pacing_guide_ids,
        )
        results["pacing_guides"] = [{"id": str(row.id), "title": row.title} for row in rolled]

    if period_ids:
        for index, period_id in enumerate(period_ids):
            if index + 1 >= len(period_ids):
                break
            target_id = period_ids[index + 1]
            try:
                payload = duplicate_week(
                    db,
                    tenant_id=tenant_id,
                    user=user,
                    source_period_id=period_id,
                    target_period_id=target_id,
                    copy_objectives=True,
                    copy_resources=copy_resources,
                    copy_notes=True,
                    copy_artifacts=copy_assignments or copy_quizzes or copy_rubrics,
                )
                results["weeks"].append(payload)
            except (LookupError, ValueError) as exc:
                results["warnings"].append(str(exc))

    if copy_instructional_plans:
        copied, warnings = curriculum_rollover_copy(
            db,
            tenant_id=tenant_id,
            user_id=user.id,
            source_school_year_id=source_school_year_id,
            target_school_year_id=target_school_year_id,
            plan_ids=[],
            preserve_titles=True,
            title_suffix=None,
            target_grading_period_mapping=None,
        )
        results["instructional_plans"] = [{"id": str(row.id), "title": row.title} for row in copied]
        results["warnings"].extend(warnings)

    record_reuse_event(
        db,
        tenant_id=tenant_id,
        user_id=user.id,
        event_type="rollover_v2",
        artifact_type="WEEK",
        source_entity_type="school_year",
        source_entity_id=source_school_year_id,
        target_entity_id=target_school_year_id,
        estimated_minutes_saved=TIME_SAVINGS_MINUTES["ROLLOVER"],
        metadata={
            "copy_instructional_plans": copy_instructional_plans,
            "copy_assignments": copy_assignments,
            "copy_quizzes": copy_quizzes,
            "copy_rubrics": copy_rubrics,
        },
    )
    return results
