from __future__ import annotations

from datetime import UTC, datetime
import uuid
from typing import Any

from sqlalchemy.orm import Session

from oziebot_api.models.teacher_assist_time_savings import TeacherAssistReuseEvent
from oziebot_api.services.teacher_assist.time_savings_constants import REUSE_EVENT_TYPES, TIME_SAVINGS_MINUTES


def _now() -> datetime:
    return datetime.now(UTC)


def record_reuse_event(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    event_type: str,
    artifact_type: str | None = None,
    source_entity_type: str | None = None,
    source_entity_id: uuid.UUID | None = None,
    target_entity_id: uuid.UUID | None = None,
    estimated_minutes_saved: int | None = None,
    metadata: dict[str, Any] | None = None,
) -> TeacherAssistReuseEvent:
    if event_type not in REUSE_EVENT_TYPES:
        raise ValueError("Unsupported reuse event type")
    minutes = estimated_minutes_saved
    if minutes is None and artifact_type:
        minutes = TIME_SAVINGS_MINUTES.get(artifact_type.upper(), 10)
    if minutes is None:
        minutes = 10
    row = TeacherAssistReuseEvent(
        tenant_id=tenant_id,
        user_id=user_id,
        event_type=event_type,
        artifact_type=artifact_type,
        source_entity_type=source_entity_type,
        source_entity_id=source_entity_id,
        target_entity_id=target_entity_id,
        estimated_minutes_saved=minutes,
        metadata_json=metadata,
        created_at=_now(),
    )
    db.add(row)
    db.flush()
    return row
