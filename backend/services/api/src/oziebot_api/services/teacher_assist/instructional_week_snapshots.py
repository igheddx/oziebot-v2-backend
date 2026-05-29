from __future__ import annotations

from datetime import UTC, datetime
import json
import uuid
from typing import Any

from sqlalchemy.orm import Session

from oziebot_api.models.teacher_assist_instructional_week import TeacherAssistInstructionalWeekSnapshot
from oziebot_api.models.user import User
from oziebot_api.services.teacher_assist.instructional_week_workspace import build_instructional_week_workspace
from oziebot_api.services.teacher_assist.instructional_weeks import get_instructional_week


def _now() -> datetime:
    return datetime.now(UTC)


def _json_safe(value: Any) -> Any:
    return json.loads(json.dumps(value, default=str))


def create_instructional_week_snapshot(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user: User,
    instructional_week_id: uuid.UUID,
    name: str,
) -> TeacherAssistInstructionalWeekSnapshot:
    get_instructional_week(db, tenant_id=tenant_id, user_id=user.id, instructional_week_id=instructional_week_id)
    payload = build_instructional_week_workspace(
        db, tenant_id=tenant_id, user=user, instructional_week_id=instructional_week_id
    )
    row = TeacherAssistInstructionalWeekSnapshot(
        instructional_week_id=instructional_week_id,
        name=name.strip(),
        snapshot_data=_json_safe(payload),
        created_by_user_id=user.id,
        created_at=_now(),
    )
    db.add(row)
    db.flush()
    return row


def serialize_snapshot(row: TeacherAssistInstructionalWeekSnapshot) -> dict[str, Any]:
    return {
        "id": str(row.id),
        "instructional_week_id": str(row.instructional_week_id),
        "name": row.name,
        "created_by_user_id": str(row.created_by_user_id),
        "created_at": row.created_at.isoformat(),
    }
