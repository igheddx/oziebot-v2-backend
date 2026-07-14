from __future__ import annotations

from datetime import UTC, datetime, timedelta
import uuid
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from oziebot_api.models.teacher_assist_generated_artifact import TeacherAssistGeneratedArtifact
from oziebot_api.models.teacher_assist_pacing_guide import TeacherAssistPacingGuide
from oziebot_api.models.teacher_assist_pacing_guide_period import TeacherAssistPacingGuidePeriod
from oziebot_api.models.teacher_assist_pacing_guide_period_note import (
    TeacherAssistPacingGuidePeriodNote,
)
from oziebot_api.models.user import User
from oziebot_api.services.teacher_assist.generated_artifacts import register_generated_artifact
from oziebot_api.services.teacher_assist.pacing_guide_foundation import (
    add_pacing_guide_objective,
    add_pacing_guide_resource,
    create_pacing_guide_period,
    get_catalog_pacing_guide_detail,
)
from oziebot_api.services.teacher_assist.reuse_events import record_reuse_event
from oziebot_api.services.teacher_assist.time_savings_constants import TIME_SAVINGS_MINUTES


def _now() -> datetime:
    return datetime.now(UTC)


def _get_period(
    db: Session, *, tenant_id: uuid.UUID, period_id: uuid.UUID
) -> TeacherAssistPacingGuidePeriod:
    period = db.scalars(
        select(TeacherAssistPacingGuidePeriod)
        .join(
            TeacherAssistPacingGuide,
            TeacherAssistPacingGuide.id == TeacherAssistPacingGuidePeriod.pacing_guide_id,
        )
        .where(
            TeacherAssistPacingGuidePeriod.id == period_id,
            TeacherAssistPacingGuide.tenant_id == tenant_id,
        )
        .options(
            selectinload(TeacherAssistPacingGuidePeriod.objectives),
            selectinload(TeacherAssistPacingGuidePeriod.resources),
        )
    ).one_or_none()
    if period is None:
        raise LookupError("Pacing guide period not found")
    return period


def duplicate_week(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user: User,
    source_period_id: uuid.UUID,
    target_period_id: uuid.UUID | None = None,
    target_guide_id: uuid.UUID | None = None,
    target_school_year_id: uuid.UUID | None = None,
    copy_objectives: bool = True,
    copy_resources: bool = True,
    copy_notes: bool = True,
    copy_artifacts: bool = False,
) -> dict[str, Any]:
    source = _get_period(db, tenant_id=tenant_id, period_id=source_period_id)
    source_guide = db.get(TeacherAssistPacingGuide, source.pacing_guide_id)
    if source_guide is None:
        raise LookupError("Pacing guide not found")

    guide_id = target_guide_id or source.pacing_guide_id
    if target_school_year_id and target_school_year_id != source_guide.school_year_id:
        guide_id = source.pacing_guide_id

    if target_period_id is None:
        detail = get_catalog_pacing_guide_detail(db, tenant_id=tenant_id, pacing_guide_id=guide_id)
        week_periods = [row for row in detail.periods if row.period_type == "WEEK"]
        next_sequence = max((row.sequence_number for row in week_periods), default=0) + 1
        start = source.end_date + timedelta(days=3) if source.end_date else None
        end = start + timedelta(days=4) if start else None
        target = create_pacing_guide_period(
            db,
            tenant_id=tenant_id,
            pacing_guide_id=guide_id,
            period_type="WEEK",
            title=f"{source.title} (Copy)",
            description=source.description,
            sequence_number=next_sequence,
            start_date=start,
            end_date=end,
        )
        target_period_id = target.id
    else:
        target = _get_period(db, tenant_id=tenant_id, period_id=target_period_id)

    if copy_objectives:
        for mapping in source.objectives:
            add_pacing_guide_objective(
                db,
                tenant_id=tenant_id,
                period_id=target.id,
                objective_id=mapping.objective_id,
                is_required=mapping.is_required,
                notes=mapping.notes,
            )
    if copy_resources:
        for mapping in source.resources:
            add_pacing_guide_resource(
                db,
                tenant_id=tenant_id,
                period_id=target.id,
                catalog_resource_id=mapping.catalog_resource_id,
                resource_library_item_id=mapping.resource_library_item_id,
                is_primary=mapping.is_primary,
                notes=mapping.notes,
            )
    if copy_notes:
        source_note = db.scalars(
            select(TeacherAssistPacingGuidePeriodNote).where(
                TeacherAssistPacingGuidePeriodNote.tenant_id == tenant_id,
                TeacherAssistPacingGuidePeriodNote.user_id == user.id,
                TeacherAssistPacingGuidePeriodNote.period_id == source.id,
            )
        ).one_or_none()
        combined = "\n\n".join(
            [
                part
                for part in [source.description, source_note.notes if source_note else None]
                if part
            ]
        )
        if combined.strip():
            now = _now()
            row = db.scalars(
                select(TeacherAssistPacingGuidePeriodNote).where(
                    TeacherAssistPacingGuidePeriodNote.tenant_id == tenant_id,
                    TeacherAssistPacingGuidePeriodNote.user_id == user.id,
                    TeacherAssistPacingGuidePeriodNote.period_id == target.id,
                )
            ).one_or_none()
            if row is None:
                row = TeacherAssistPacingGuidePeriodNote(
                    tenant_id=tenant_id,
                    user_id=user.id,
                    period_id=target.id,
                    notes=combined,
                    created_at=now,
                    updated_at=now,
                )
                db.add(row)
            else:
                row.notes = combined
                row.updated_at = now

    copied_artifacts: list[str] = []
    if copy_artifacts:
        artifacts = db.scalars(
            select(TeacherAssistGeneratedArtifact).where(
                TeacherAssistGeneratedArtifact.tenant_id == tenant_id,
                TeacherAssistGeneratedArtifact.pacing_guide_period_id == source.id,
            )
        ).all()
        for artifact in artifacts:
            clone = register_generated_artifact(
                db,
                tenant_id=tenant_id,
                user=user,
                pacing_guide_id=guide_id,
                pacing_guide_period_id=target.id,
                artifact_type=artifact.artifact_type,
                title=f"{artifact.title} (Copy)",
                status="draft",
                instructional_plan_id=artifact.instructional_plan_id,
                planning_draft_id=artifact.planning_draft_id,
                assignment_id=artifact.assignment_id,
                export_artifact_id=artifact.export_artifact_id,
                newsletter_id=artifact.newsletter_id,
                resource_links=artifact.resource_links_json,
                metadata={
                    **(artifact.metadata_json or {}),
                    "duplicated_from_period_id": str(source.id),
                },
            )
            copied_artifacts.append(str(clone.id))

    record_reuse_event(
        db,
        tenant_id=tenant_id,
        user_id=user.id,
        event_type="duplicate_week",
        artifact_type="WEEK",
        source_entity_type="pacing_guide_period",
        source_entity_id=source.id,
        target_entity_id=target.id,
        estimated_minutes_saved=TIME_SAVINGS_MINUTES["WEEK"],
    )
    return {
        "source_period_id": str(source.id),
        "target_period_id": str(target.id),
        "target_guide_id": str(guide_id),
        "copied_artifact_ids": copied_artifacts,
        "navigation_href": f"/teacher-assist/planning/weeks?period_id={target.id}",
    }
