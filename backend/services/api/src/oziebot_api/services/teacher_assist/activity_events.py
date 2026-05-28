from __future__ import annotations

from datetime import UTC, datetime
import uuid
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from oziebot_api.models.teacher_assist_activity_event import TeacherAssistActivityEvent
from oziebot_api.services.teacher_assist.instructional_plan_validator import contains_pii_like_content

ACTIVITY_EVENT_CATEGORIES = (
    "workflow",
    "planning",
    "assignment",
    "grading",
    "submission",
    "packet",
    "review",
    "system",
)

ACTIVITY_EVENT_TYPES = (
    "workflow_started",
    "workflow_completed",
    "workflow_failed",
    "workflow_cancelled",
    "plan_created",
    "plan_updated",
    "plan_regenerated",
    "plan_completed",
    "assignment_created",
    "assignment_updated",
    "assignment_status_changed",
    "packet_generated",
    "student_work_uploaded",
    "student_work_status_changed",
    "grading_review_created",
    "grading_review_confirmed",
    "grading_review_updated",
    "section_regenerated",
    "extraction_started",
    "extraction_completed",
    "extraction_failed",
    "extraction_cancelled",
    "extraction_retry_requested",
    "extraction_review_started",
    "extraction_review_approved",
    "extraction_review_rejected",
    "extraction_text_corrected",
    "extraction_issue_flagged",
)


def _validate_event_category(value: str) -> str:
    normalized = value.strip().lower()
    if normalized not in ACTIVITY_EVENT_CATEGORIES:
        raise ValueError(f"Unsupported TeacherAssist activity event category '{value}'")
    return normalized


def _validate_event_type(value: str) -> str:
    normalized = value.strip().lower()
    if normalized not in ACTIVITY_EVENT_TYPES:
        raise ValueError(f"Unsupported TeacherAssist activity event type '{value}'")
    return normalized


def _normalize_summary_text(value: str) -> str:
    normalized = value.strip()
    if not normalized:
        raise ValueError("TeacherAssist activity events require summary_text")
    if contains_pii_like_content({"summary_text": normalized}):
        raise ValueError("TeacherAssist activity event summary cannot include PII-like content")
    return normalized


def _normalize_details_json(value: dict[str, Any] | None) -> dict[str, Any] | None:
    if value is None:
        return None
    normalized = dict(value)
    if contains_pii_like_content({"details_json": normalized}):
        raise ValueError("TeacherAssist activity event details cannot include PII-like content")
    return normalized


def record_activity_event(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    event_type: str,
    event_category: str,
    entity_type: str,
    entity_id: uuid.UUID,
    summary_text: str,
    school_year_id: uuid.UUID | None = None,
    grading_period_id: uuid.UUID | None = None,
    class_id: uuid.UUID | None = None,
    subject_id: uuid.UUID | None = None,
    workflow_id: uuid.UUID | None = None,
    details_json: dict[str, Any] | None = None,
    event_timestamp: datetime | None = None,
) -> TeacherAssistActivityEvent:
    normalized_entity_type = entity_type.strip().lower()
    if not normalized_entity_type:
        raise ValueError("TeacherAssist activity events require entity_type")
    timestamp = event_timestamp or datetime.now(UTC)
    row = TeacherAssistActivityEvent(
        tenant_id=tenant_id,
        user_id=user_id,
        event_type=_validate_event_type(event_type),
        event_category=_validate_event_category(event_category),
        entity_type=normalized_entity_type,
        entity_id=entity_id,
        event_timestamp=timestamp,
        school_year_id=school_year_id,
        grading_period_id=grading_period_id,
        class_id=class_id,
        subject_id=subject_id,
        summary_text=_normalize_summary_text(summary_text),
        details_json=_normalize_details_json(details_json),
        workflow_id=workflow_id,
        created_at=timestamp,
    )
    db.add(row)
    return row


def list_recent_activity_events(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    limit: int = 50,
) -> list[TeacherAssistActivityEvent]:
    return db.scalars(
        select(TeacherAssistActivityEvent)
        .where(
            TeacherAssistActivityEvent.tenant_id == tenant_id,
            TeacherAssistActivityEvent.user_id == user_id,
        )
        .order_by(
            TeacherAssistActivityEvent.event_timestamp.desc(),
            TeacherAssistActivityEvent.created_at.desc(),
        )
        .limit(max(1, limit))
    ).all()
