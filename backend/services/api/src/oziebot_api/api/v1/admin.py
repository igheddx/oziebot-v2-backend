"""Platform admin (root_admin only) — tenant integration shortcuts."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel
from sqlalchemy import select

from oziebot_api.deps import DbSession
from oziebot_api.deps.auth import RootAdminUser
from oziebot_api.models.tenant import Tenant
from oziebot_api.models.tenant_integration import TenantIntegration
from oziebot_api.services.audit import record_admin_action

router = APIRouter(prefix="/admin", tags=["admin"])


class CoinbaseIntegrationBody(BaseModel):
    connected: bool


@router.put("/tenants/{tenant_id}/integrations/coinbase")
def set_coinbase_integration(
    tenant_id: uuid.UUID,
    body: CoinbaseIntegrationBody,
    admin: RootAdminUser,
    db: DbSession,
    request: Request,
) -> dict:
    """Mark Coinbase as connected (or disconnect). Prefer /v1/admin/platform/tenants/{id}/coinbase-health for probes."""
    tenant = db.get(Tenant, tenant_id)
    if tenant is None:
        raise HTTPException(status_code=404, detail="Tenant not found")
    now = datetime.now(UTC)
    row = db.scalars(
        select(TenantIntegration).where(TenantIntegration.tenant_id == tenant_id)
    ).one_or_none()
    before = None
    if row is None:
        row = TenantIntegration(
            tenant_id=tenant_id,
            coinbase_connected=body.connected,
            coinbase_last_check_at=now,
            coinbase_health_status="unknown",
            coinbase_last_error=None,
            updated_at=now,
        )
        db.add(row)
    else:
        before = {
            "coinbase_connected": row.coinbase_connected,
            "coinbase_health_status": row.coinbase_health_status,
        }
        row.coinbase_connected = body.connected
        row.coinbase_last_check_at = now
        row.updated_at = now
    db.flush()
    ip = request.client.host if request.client else None
    record_admin_action(
        db,
        actor_user_id=admin.id,
        action="tenant.coinbase_connection.set",
        resource_type="tenant_integrations",
        resource_id=str(tenant_id),
        details={"before": before, "after": {"connected": body.connected}},
        ip_address=ip,
        user_agent=request.headers.get("user-agent"),
    )
    return {"tenant_id": str(tenant_id), "coinbase_connected": body.connected}
