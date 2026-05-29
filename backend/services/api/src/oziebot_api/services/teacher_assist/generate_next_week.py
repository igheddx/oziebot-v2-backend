from __future__ import annotations

from datetime import UTC, datetime, timedelta
import uuid
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from oziebot_api.models.teacher_assist_pacing_guide import TeacherAssistPacingGuide
from oziebot_api.models.teacher_assist_pacing_guide_period import TeacherAssistPacingGuidePeriod
from oziebot_api.models.teacher_assist_pacing_guide_period_note import TeacherAssistPacingGuidePeriodNote
from oziebot_api.models.user import User
from oziebot_api.services.teacher_assist.instructional_asset_reuse import InstructionalAssetReuseService
from oziebot_api.services.teacher_assist.pacing_guide_foundation import get_catalog_pacing_guide_detail
from oziebot_api.services.teacher_assist.reuse_events import record_reuse_event
from oziebot_api.services.teacher_assist.time_savings_constants import TIME_SAVINGS_MINUTES
from oziebot_api.services.teacher_assist.week_context_service import WeekContextService


def _now() -> datetime:
    return datetime.now(UTC)


def generate_next_week_draft(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user: User,
    period_id: uuid.UUID,
) -> dict[str, Any]:
    period = db.scalars(
        select(TeacherAssistPacingGuidePeriod)
        .join(TeacherAssistPacingGuide, TeacherAssistPacingGuide.id == TeacherAssistPacingGuidePeriod.pacing_guide_id)
        .where(
            TeacherAssistPacingGuidePeriod.id == period_id,
            TeacherAssistPacingGuide.tenant_id == tenant_id,
        )
    ).one_or_none()
    if period is None:
        raise LookupError("Pacing guide period not found")

    detail = get_catalog_pacing_guide_detail(db, tenant_id=tenant_id, pacing_guide_id=period.pacing_guide_id)
    weeks = [row for row in detail.periods if row.period_type == "WEEK"]
    weeks.sort(key=lambda row: row.sequence_number)
    current_index = next((index for index, row in enumerate(weeks) if row.id == period.id), None)
    if current_index is None:
        raise ValueError("Selected period is not a week")
    if current_index + 1 >= len(weeks):
        raise ValueError("No upcoming week exists in this pacing guide")

    next_week = weeks[current_index + 1]
    current_context = WeekContextService.build(db, tenant_id=tenant_id, user=user, period_id=period_id)
    recommendations = InstructionalAssetReuseService.search(
        db, tenant_id=tenant_id, user=user, period_id=next_week.id, limit=5
    )

    suggested_objectives = [row.get("objective_code") for row in current_context.objectives if row.get("objective_code")]
    suggested_resources = current_context.resources[:]
    if next_week.objectives:
        suggested_objectives = [
            getattr(getattr(row, "objective", None), "objective_id", None) for row in next_week.objectives
        ]
        suggested_objectives = [code for code in suggested_objectives if code]
    if next_week.resources:
        suggested_resources = [
            {
                "catalog_resource_id": str(row.catalog_resource_id) if row.catalog_resource_id else None,
                "resource_library_item_id": str(row.resource_library_item_id) if row.resource_library_item_id else None,
            }
            for row in next_week.resources
        ]

    draft_notes = (
        f"Suggested draft for {next_week.title}.\n"
        f"Objectives: {', '.join(suggested_objectives) if suggested_objectives else 'Review pacing guide objectives.'}\n"
        f"Based on prior week: {current_context.period_title}."
    )
    note = db.scalars(
        select(TeacherAssistPacingGuidePeriodNote).where(
            TeacherAssistPacingGuidePeriodNote.tenant_id == tenant_id,
            TeacherAssistPacingGuidePeriodNote.user_id == user.id,
            TeacherAssistPacingGuidePeriodNote.period_id == next_week.id,
        )
    ).one_or_none()
    now = _now()
    if note is None:
        note = TeacherAssistPacingGuidePeriodNote(
            tenant_id=tenant_id,
            user_id=user.id,
            period_id=next_week.id,
            notes=draft_notes,
            created_at=now,
            updated_at=now,
        )
        db.add(note)
    else:
        note.notes = draft_notes
        note.updated_at = now
    db.flush()

    record_reuse_event(
        db,
        tenant_id=tenant_id,
        user_id=user.id,
        event_type="generate_next_week",
        artifact_type="WEEK",
        source_entity_type="pacing_guide_period",
        source_entity_id=period.id,
        target_entity_id=next_week.id,
        estimated_minutes_saved=TIME_SAVINGS_MINUTES["WEEK"] // 2,
    )
    return {
        "source_period_id": str(period.id),
        "next_period_id": str(next_week.id),
        "next_period_title": next_week.title,
        "suggested_objectives": suggested_objectives,
        "suggested_resources": suggested_resources,
        "recommended_reuse": recommendations,
        "draft_notes": draft_notes,
        "requires_review": True,
        "navigation_href": f"/teacher-assist/planning/weeks?period_id={next_week.id}",
    }
