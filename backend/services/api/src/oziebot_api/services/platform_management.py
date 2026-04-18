"""Root admin platform configuration (DB-backed, auditable)."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from oziebot_api.models.membership import TenantMembership
from oziebot_api.models.platform_setting import PlatformSetting
from oziebot_api.models.platform_trial_policy import PlatformTrialPolicy
from oziebot_api.models.tenant import Tenant
from oziebot_api.models.tenant_integration import TenantIntegration
from oziebot_api.models.user import User
from oziebot_api.services.audit import record_admin_action

SETTING_TRADING_GLOBAL_PAUSE = "trading.global_pause"


def get_all_settings(db: Session) -> dict[str, Any]:
    rows = db.scalars(select(PlatformSetting)).all()
    return {r.key: r.value for r in rows}


def upsert_setting(
    db: Session,
    *,
    key: str,
    value: dict[str, Any],
    updated_by_user_id: uuid.UUID | None,
) -> PlatformSetting:
    now = datetime.now(UTC)
    row = db.get(PlatformSetting, key)
    if row is None:
        row = PlatformSetting(
            key=key, value=value, updated_at=now, updated_by_user_id=updated_by_user_id
        )
        db.add(row)
    else:
        row.value = value
        row.updated_at = now
        row.updated_by_user_id = updated_by_user_id
    db.flush()
    return row


def set_global_trading_pause(
    db: Session,
    *,
    paused: bool,
    reason: str | None,
    actor_user_id: uuid.UUID,
    audit_ip: str | None,
    audit_ua: str | None,
) -> dict[str, Any]:
    payload = {
        "paused": paused,
        "reason": reason,
        "updated_at": datetime.now(UTC).isoformat(),
    }
    upsert_setting(
        db,
        key=SETTING_TRADING_GLOBAL_PAUSE,
        value=payload,
        updated_by_user_id=actor_user_id,
    )
    record_admin_action(
        db,
        actor_user_id=actor_user_id,
        action="trading.global_pause",
        resource_type="platform_settings",
        resource_id=SETTING_TRADING_GLOBAL_PAUSE,
        details={"after": payload},
        ip_address=audit_ip,
        user_agent=audit_ua,
    )
    return payload


def is_trading_globally_paused(db: Session) -> bool:
    row = db.get(PlatformSetting, SETTING_TRADING_GLOBAL_PAUSE)
    if row is None:
        return False
    return bool(row.value.get("paused"))


def get_or_create_trial_policy(db: Session) -> PlatformTrialPolicy:
    rows = db.scalars(select(PlatformTrialPolicy).limit(2)).all()
    if rows:
        return rows[0]
    now = datetime.now(UTC)
    p = PlatformTrialPolicy(
        id=uuid.uuid4(),
        is_enabled=True,
        trial_duration_days=30,
        max_trials_per_tenant=1,
        grace_period_days=0,
        metadata_=None,
        updated_at=now,
        updated_by_user_id=None,
    )
    db.add(p)
    db.flush()
    return p


def list_users_with_coinbase(
    db: Session,
    *,
    skip: int,
    limit: int,
) -> tuple[list[dict[str, Any]], int]:
    total = db.scalar(select(func.count()).select_from(User)) or 0
    users = db.scalars(
        select(User).order_by(User.created_at.desc()).offset(skip).limit(limit)
    ).all()
    out: list[dict[str, Any]] = []
    for u in users:
        memberships = db.scalars(
            select(TenantMembership).where(TenantMembership.user_id == u.id)
        ).all()
        tenants_info: list[dict[str, Any]] = []
        for m in memberships:
            t = db.get(Tenant, m.tenant_id)
            ti = db.get(TenantIntegration, m.tenant_id)
            tenants_info.append(
                {
                    "tenant_id": str(m.tenant_id),
                    "tenant_name": t.name if t else None,
                    "role": m.role,
                    "coinbase_connected": ti.coinbase_connected if ti else False,
                    "coinbase_last_check_at": ti.coinbase_last_check_at.isoformat()
                    if ti and ti.coinbase_last_check_at
                    else None,
                    "coinbase_health_status": ti.coinbase_health_status if ti else None,
                    "coinbase_last_error": ti.coinbase_last_error if ti else None,
                }
            )
        out.append(
            {
                "id": str(u.id),
                "email": u.email,
                "is_root_admin": u.is_root_admin,
                "is_active": u.is_active,
                "current_trading_mode": u.current_trading_mode,
                "email_verified_at": u.email_verified_at.isoformat()
                if u.email_verified_at
                else None,
                "created_at": u.created_at.isoformat(),
                "tenants": tenants_info,
            }
        )
    return out, int(total)
