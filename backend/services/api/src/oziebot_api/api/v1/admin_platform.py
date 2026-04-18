"""Root admin: platform catalogs, settings, users overview, audit log."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from oziebot_api.config import Settings
from oziebot_api.deps import DbSession, settings_dep
from oziebot_api.deps.auth import RootAdminUser
from oziebot_api.models.admin_audit_log import AdminAuditLog
from oziebot_api.models.platform_setting import PlatformSetting
from oziebot_api.models.platform_strategy import PlatformStrategy
from oziebot_api.models.platform_token import PlatformTokenAllowlist
from oziebot_api.models.platform_trial_policy import PlatformTrialPolicy
from oziebot_api.models.subscription_plan import SubscriptionPlan
from oziebot_api.models.tenant import Tenant
from oziebot_api.models.tenant_integration import TenantIntegration
from oziebot_api.schemas.platform_admin import (
    GlobalPauseBody,
    SettingValueBody,
    StrategyCatalogCreate,
    StrategyCatalogPatch,
    SubscriptionPlanCreate,
    SubscriptionPlanPatch,
    TenantCoinbaseHealthPatch,
    TokenAllowlistCreate,
    TokenAllowlistPatch,
    TrialPolicyBody,
)
from oziebot_api.services.audit import record_admin_action
from oziebot_api.services.platform_management import (
    get_all_settings,
    get_or_create_trial_policy,
    list_users_with_coinbase,
    set_global_trading_pause,
    upsert_setting,
)

router = APIRouter(prefix="/admin/platform", tags=["admin-platform"])


def _audit(
    db: Session,
    admin: Any,
    *,
    action: str,
    resource_type: str,
    resource_id: str | None,
    details: dict[str, Any] | None,
    request: Request,
) -> None:
    ip = request.client.host if request.client else None
    ua = request.headers.get("user-agent")
    record_admin_action(
        db,
        actor_user_id=admin.id,
        action=action,
        resource_type=resource_type,
        resource_id=resource_id,
        details=details,
        ip_address=ip,
        user_agent=ua,
    )


@router.get("/overview")
def platform_overview(
    _admin: RootAdminUser,
    db: DbSession,
    app_settings: Settings = Depends(settings_dep),
) -> dict[str, Any]:
    return {
        "environment": app_settings.app_env,
        "settings": get_all_settings(db),
    }


@router.get("/settings")
def list_settings(_admin: RootAdminUser, db: DbSession) -> dict[str, Any]:
    return get_all_settings(db)


@router.put("/settings/{key}")
def put_setting(
    key: str,
    body: SettingValueBody,
    admin: RootAdminUser,
    db: DbSession,
    request: Request,
) -> dict[str, Any]:
    prev = db.get(PlatformSetting, key)
    row = upsert_setting(db, key=key, value=body.value, updated_by_user_id=admin.id)
    _audit(
        db,
        admin,
        action="platform_settings.upsert",
        resource_type="platform_settings",
        resource_id=key,
        details={"before": prev.value if prev else None, "after": body.value},
        request=request,
    )
    return {"key": row.key, "value": row.value, "updated_at": row.updated_at.isoformat()}


@router.put("/trading/global-pause")
def global_trading_pause(
    body: GlobalPauseBody,
    admin: RootAdminUser,
    db: DbSession,
    request: Request,
) -> dict[str, Any]:
    payload = set_global_trading_pause(
        db,
        paused=body.paused,
        reason=body.reason,
        actor_user_id=admin.id,
        audit_ip=request.client.host if request.client else None,
        audit_ua=request.headers.get("user-agent"),
    )
    return payload


@router.get("/tokens")
def list_tokens(_admin: RootAdminUser, db: DbSession) -> list[dict[str, Any]]:
    rows = db.scalars(
        select(PlatformTokenAllowlist).order_by(PlatformTokenAllowlist.sort_order)
    ).all()
    return [_token_out(r) for r in rows]


def _token_out(r: PlatformTokenAllowlist) -> dict[str, Any]:
    return {
        "id": str(r.id),
        "symbol": r.symbol,
        "quote_currency": r.quote_currency,
        "network": r.network,
        "contract_address": r.contract_address,
        "display_name": r.display_name,
        "is_enabled": r.is_enabled,
        "sort_order": r.sort_order,
        "extra": r.extra,
        "created_at": r.created_at.isoformat(),
        "updated_at": r.updated_at.isoformat(),
    }


@router.post("/tokens", status_code=201)
def create_token(
    body: TokenAllowlistCreate,
    admin: RootAdminUser,
    db: DbSession,
    request: Request,
) -> dict[str, Any]:
    now = datetime.now(UTC)
    row = PlatformTokenAllowlist(
        id=uuid.uuid4(),
        symbol=body.symbol.strip().upper(),
        quote_currency=body.quote_currency,
        network=body.network,
        contract_address=body.contract_address,
        display_name=body.display_name,
        is_enabled=body.is_enabled,
        sort_order=body.sort_order,
        extra=body.extra,
        created_at=now,
        updated_at=now,
    )
    db.add(row)
    try:
        db.flush()
    except IntegrityError as e:
        raise HTTPException(status_code=409, detail="Token row conflict") from e
    _audit(
        db,
        admin,
        action="token_allowlist.create",
        resource_type="platform_token_allowlist",
        resource_id=str(row.id),
        details={"record": _token_out(row)},
        request=request,
    )
    return _token_out(row)


@router.patch("/tokens/{token_id}")
def patch_token(
    token_id: uuid.UUID,
    body: TokenAllowlistPatch,
    admin: RootAdminUser,
    db: DbSession,
    request: Request,
) -> dict[str, Any]:
    row = db.get(PlatformTokenAllowlist, token_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Token not found")
    before = _token_out(row)
    data = body.model_dump(exclude_unset=True)
    if "symbol" in data and data["symbol"]:
        row.symbol = data["symbol"].strip().upper()
    for k in (
        "quote_currency",
        "network",
        "contract_address",
        "display_name",
        "is_enabled",
        "sort_order",
    ):
        if k in data and data[k] is not None:
            setattr(row, k, data[k])
    if "extra" in data:
        row.extra = data["extra"]
    row.updated_at = datetime.now(UTC)
    _audit(
        db,
        admin,
        action="token_allowlist.update",
        resource_type="platform_token_allowlist",
        resource_id=str(token_id),
        details={"before": before, "after": _token_out(row)},
        request=request,
    )
    return _token_out(row)


@router.delete("/tokens/{token_id}", status_code=204)
def delete_token(
    token_id: uuid.UUID,
    admin: RootAdminUser,
    db: DbSession,
    request: Request,
) -> None:
    row = db.get(PlatformTokenAllowlist, token_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Token not found")
    snap = _token_out(row)
    db.delete(row)
    try:
        db.flush()
    except IntegrityError as e:
        raise HTTPException(
            status_code=409,
            detail="Cannot delete strategy because it is referenced by other records",
        ) from e
    _audit(
        db,
        admin,
        action="token_allowlist.delete",
        resource_type="platform_token_allowlist",
        resource_id=str(token_id),
        details={"deleted": snap},
        request=request,
    )


def _strategy_out(r: PlatformStrategy) -> dict[str, Any]:
    return {
        "id": str(r.id),
        "slug": r.slug,
        "display_name": r.display_name,
        "description": r.description,
        "is_enabled": r.is_enabled,
        "entry_point": r.entry_point,
        "config_schema": r.config_schema,
        "sort_order": r.sort_order,
        "created_at": r.created_at.isoformat(),
        "updated_at": r.updated_at.isoformat(),
    }


@router.get("/strategies")
def list_strategies(_admin: RootAdminUser, db: DbSession) -> list[dict[str, Any]]:
    rows = db.scalars(select(PlatformStrategy).order_by(PlatformStrategy.sort_order)).all()
    return [_strategy_out(r) for r in rows]


@router.post("/strategies", status_code=201)
def create_strategy(
    body: StrategyCatalogCreate,
    admin: RootAdminUser,
    db: DbSession,
    request: Request,
) -> dict[str, Any]:
    now = datetime.now(UTC)
    row = PlatformStrategy(
        id=uuid.uuid4(),
        slug=body.slug.strip().lower(),
        display_name=body.display_name,
        description=body.description,
        is_enabled=body.is_enabled,
        entry_point=body.entry_point,
        config_schema=body.config_schema,
        sort_order=body.sort_order,
        created_at=now,
        updated_at=now,
    )
    db.add(row)
    try:
        db.flush()
    except IntegrityError as e:
        raise HTTPException(status_code=409, detail="Slug already exists") from e
    _audit(
        db,
        admin,
        action="platform_strategy.create",
        resource_type="platform_strategies",
        resource_id=str(row.id),
        details={"record": _strategy_out(row)},
        request=request,
    )
    return _strategy_out(row)


@router.patch("/strategies/{strategy_id}")
def patch_strategy(
    strategy_id: uuid.UUID,
    body: StrategyCatalogPatch,
    admin: RootAdminUser,
    db: DbSession,
    request: Request,
) -> dict[str, Any]:
    row = db.get(PlatformStrategy, strategy_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Strategy not found")
    before = _strategy_out(row)
    for k, v in body.model_dump(exclude_unset=True).items():
        if v is not None:
            setattr(row, k, v)
    row.updated_at = datetime.now(UTC)
    _audit(
        db,
        admin,
        action="platform_strategy.update",
        resource_type="platform_strategies",
        resource_id=str(strategy_id),
        details={"before": before, "after": _strategy_out(row)},
        request=request,
    )
    return _strategy_out(row)


@router.delete("/strategies/{strategy_id}", status_code=204)
def delete_strategy(
    strategy_id: uuid.UUID,
    admin: RootAdminUser,
    db: DbSession,
    request: Request,
) -> None:
    row = db.get(PlatformStrategy, strategy_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Strategy not found")
    snap = _strategy_out(row)
    db.delete(row)
    try:
        db.flush()
    except IntegrityError as e:
        raise HTTPException(
            status_code=409,
            detail="Cannot delete strategy because it is referenced by other records",
        ) from e
    _audit(
        db,
        admin,
        action="platform_strategy.delete",
        resource_type="platform_strategies",
        resource_id=str(strategy_id),
        details={"deleted": snap},
        request=request,
    )


def _plan_out(r: SubscriptionPlan) -> dict[str, Any]:
    return {
        "id": str(r.id),
        "slug": r.slug,
        "display_name": r.display_name,
        "description": r.description,
        "plan_kind": r.plan_kind,
        "stripe_price_id": r.stripe_price_id,
        "stripe_product_id": r.stripe_product_id,
        "billing_interval": r.billing_interval,
        "amount_cents": r.amount_cents,
        "currency": r.currency,
        "is_active": r.is_active,
        "features": r.features,
        "trial_days_override": r.trial_days_override,
        "sort_order": r.sort_order,
        "created_at": r.created_at.isoformat(),
        "updated_at": r.updated_at.isoformat(),
    }


@router.get("/subscription-plans")
def list_plans(_admin: RootAdminUser, db: DbSession) -> list[dict[str, Any]]:
    rows = db.scalars(select(SubscriptionPlan).order_by(SubscriptionPlan.sort_order)).all()
    return [_plan_out(r) for r in rows]


@router.post("/subscription-plans", status_code=201)
def create_plan(
    body: SubscriptionPlanCreate,
    admin: RootAdminUser,
    db: DbSession,
    request: Request,
) -> dict[str, Any]:
    now = datetime.now(UTC)
    row = SubscriptionPlan(
        id=uuid.uuid4(),
        slug=body.slug.strip().lower(),
        display_name=body.display_name,
        description=body.description,
        plan_kind=body.plan_kind,
        stripe_price_id=body.stripe_price_id,
        stripe_product_id=body.stripe_product_id,
        billing_interval=body.billing_interval,
        amount_cents=body.amount_cents,
        currency=body.currency.lower(),
        is_active=body.is_active,
        features=body.features,
        trial_days_override=body.trial_days_override,
        sort_order=body.sort_order,
        created_at=now,
        updated_at=now,
    )
    db.add(row)
    try:
        db.flush()
    except IntegrityError as e:
        raise HTTPException(status_code=409, detail="Plan slug or price id conflict") from e
    _audit(
        db,
        admin,
        action="subscription_plan.create",
        resource_type="subscription_plans",
        resource_id=str(row.id),
        details={"record": _plan_out(row)},
        request=request,
    )
    return _plan_out(row)


@router.patch("/subscription-plans/{plan_id}")
def patch_plan(
    plan_id: uuid.UUID,
    body: SubscriptionPlanPatch,
    admin: RootAdminUser,
    db: DbSession,
    request: Request,
) -> dict[str, Any]:
    row = db.get(SubscriptionPlan, plan_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Plan not found")
    before = _plan_out(row)
    for k, v in body.model_dump(exclude_unset=True).items():
        if v is None:
            continue
        if k == "currency":
            row.currency = str(v).lower()
        else:
            setattr(row, k, v)
    row.updated_at = datetime.now(UTC)
    _audit(
        db,
        admin,
        action="subscription_plan.update",
        resource_type="subscription_plans",
        resource_id=str(plan_id),
        details={"before": before, "after": _plan_out(row)},
        request=request,
    )
    return _plan_out(row)


@router.delete("/subscription-plans/{plan_id}", status_code=204)
def delete_plan(
    plan_id: uuid.UUID,
    admin: RootAdminUser,
    db: DbSession,
    request: Request,
) -> None:
    row = db.get(SubscriptionPlan, plan_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Plan not found")
    snap = _plan_out(row)
    db.delete(row)
    _audit(
        db,
        admin,
        action="subscription_plan.delete",
        resource_type="subscription_plans",
        resource_id=str(plan_id),
        details={"deleted": snap},
        request=request,
    )


@router.get("/trial-policy")
def get_trial_policy(_admin: RootAdminUser, db: DbSession) -> dict[str, Any]:
    p = get_or_create_trial_policy(db)
    return _trial_out(p)


def _trial_out(p: PlatformTrialPolicy) -> dict[str, Any]:
    return {
        "id": str(p.id),
        "is_enabled": p.is_enabled,
        "trial_duration_days": p.trial_duration_days,
        "max_trials_per_tenant": p.max_trials_per_tenant,
        "grace_period_days": p.grace_period_days,
        "policy_metadata": p.metadata_,
        "updated_at": p.updated_at.isoformat(),
        "updated_by_user_id": str(p.updated_by_user_id) if p.updated_by_user_id else None,
    }


@router.put("/trial-policy")
def put_trial_policy(
    body: TrialPolicyBody,
    admin: RootAdminUser,
    db: DbSession,
    request: Request,
) -> dict[str, Any]:
    p = get_or_create_trial_policy(db)
    before = _trial_out(p)
    p.is_enabled = body.is_enabled
    p.trial_duration_days = body.trial_duration_days
    p.max_trials_per_tenant = body.max_trials_per_tenant
    p.grace_period_days = body.grace_period_days
    p.metadata_ = body.policy_metadata
    p.updated_at = datetime.now(UTC)
    p.updated_by_user_id = admin.id
    _audit(
        db,
        admin,
        action="trial_policy.update",
        resource_type="platform_trial_policy",
        resource_id=str(p.id),
        details={"before": before, "after": _trial_out(p)},
        request=request,
    )
    return _trial_out(p)


@router.get("/users")
def admin_list_users(
    _admin: RootAdminUser,
    db: DbSession,
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=200),
) -> dict[str, Any]:
    items, total = list_users_with_coinbase(db, skip=skip, limit=limit)
    return {"total": total, "items": items, "skip": skip, "limit": limit}


@router.get("/audit-logs")
def admin_audit_logs(
    _admin: RootAdminUser,
    db: DbSession,
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=500),
) -> dict[str, Any]:
    q = select(AdminAuditLog).order_by(AdminAuditLog.created_at.desc()).offset(skip).limit(limit)
    rows = db.scalars(q).all()
    total = db.scalar(select(func.count()).select_from(AdminAuditLog)) or 0
    return {
        "total": int(total),
        "items": [
            {
                "id": str(r.id),
                "actor_user_id": str(r.actor_user_id),
                "action": r.action,
                "resource_type": r.resource_type,
                "resource_id": r.resource_id,
                "details": r.details,
                "ip_address": r.ip_address,
                "created_at": r.created_at.isoformat(),
            }
            for r in rows
        ],
        "skip": skip,
        "limit": limit,
    }


@router.put("/tenants/{tenant_id}/coinbase-health")
def patch_tenant_coinbase_health(
    tenant_id: uuid.UUID,
    body: TenantCoinbaseHealthPatch,
    admin: RootAdminUser,
    db: DbSession,
    request: Request,
) -> dict[str, Any]:
    tenant = db.get(Tenant, tenant_id)
    if tenant is None:
        raise HTTPException(status_code=404, detail="Tenant not found")
    row = db.get(TenantIntegration, tenant_id)
    now = datetime.now(UTC)
    if row is None:
        row = TenantIntegration(
            tenant_id=tenant_id,
            coinbase_connected=body.connected if body.connected is not None else False,
            coinbase_last_check_at=now,
            coinbase_health_status=body.health_status or "unknown",
            coinbase_last_error=body.last_error,
            updated_at=now,
        )
        db.add(row)
    else:
        if body.connected is not None:
            row.coinbase_connected = body.connected
        row.coinbase_last_check_at = now
        if body.health_status is not None:
            row.coinbase_health_status = body.health_status
        if body.last_error is not None:
            row.coinbase_last_error = body.last_error
        row.updated_at = now
    db.flush()
    _audit(
        db,
        admin,
        action="tenant.coinbase_health.update",
        resource_type="tenant_integrations",
        resource_id=str(tenant_id),
        details=body.model_dump(exclude_unset=True),
        request=request,
    )
    return {
        "tenant_id": str(tenant_id),
        "coinbase_connected": row.coinbase_connected,
        "coinbase_last_check_at": row.coinbase_last_check_at.isoformat()
        if row.coinbase_last_check_at
        else None,
        "coinbase_health_status": row.coinbase_health_status,
        "coinbase_last_error": row.coinbase_last_error,
    }
