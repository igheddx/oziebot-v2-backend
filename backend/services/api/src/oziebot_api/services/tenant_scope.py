"""Resolve tenant context for JWT claims and trading checks."""

from __future__ import annotations

import uuid

from sqlalchemy import select
from sqlalchemy.orm import Session

from oziebot_api.models.membership import TenantMembership
from oziebot_api.models.user import User


def primary_tenant_id(db: Session, user: User) -> uuid.UUID | None:
    m = db.scalars(
        select(TenantMembership)
        .where(TenantMembership.user_id == user.id)
        .order_by(TenantMembership.created_at.asc())
        .limit(1)
    ).first()
    return m.tenant_id if m else None
