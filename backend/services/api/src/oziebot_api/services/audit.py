"""Append-only admin audit log."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

from sqlalchemy.orm import Session

from oziebot_api.models.admin_audit_log import AdminAuditLog


def record_admin_action(
    db: Session,
    *,
    actor_user_id: uuid.UUID,
    action: str,
    resource_type: str,
    resource_id: str | None = None,
    details: dict[str, Any] | None = None,
    ip_address: str | None = None,
    user_agent: str | None = None,
) -> AdminAuditLog:
    row = AdminAuditLog(
        id=uuid.uuid4(),
        actor_user_id=actor_user_id,
        action=action,
        resource_type=resource_type,
        resource_id=resource_id,
        details=details,
        ip_address=ip_address,
        user_agent=user_agent,
        created_at=datetime.now(UTC),
    )
    db.add(row)
    db.flush()
    return row
