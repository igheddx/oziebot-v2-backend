"""Current user profile and trading mode (tenant-safe)."""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
from datetime import UTC, datetime
from typing import Any

from fastapi import APIRouter, HTTPException
from sqlalchemy import select
from sqlalchemy.orm import joinedload

from oziebot_api.deps import DbSession
from oziebot_api.deps.auth import CurrentUser
from oziebot_api.models.membership import TenantMembership
from oziebot_api.models.execution import ExecutionOrder, ExecutionPosition, ExecutionTradeRecord
from oziebot_api.models.platform_strategy import PlatformStrategy
from oziebot_api.models.strategy_allocation import StrategyCapitalBucket
from oziebot_api.models.tenant import Tenant
from oziebot_api.models.user import User
from oziebot_api.models.user_strategy import UserStrategy
from oziebot_api.schemas.me import MeOut, TenantBrief, TradingModePatch
from oziebot_api.services.entitlements import has_strategy_entitlement
from oziebot_api.services.tenant_scope import primary_tenant_id
from oziebot_api.services.trading_mode_policy import can_set_trading_mode
from oziebot_domain.trading_mode import TradingMode

router = APIRouter(prefix="/me", tags=["me"])


def _build_me(db: DbSession, user: User) -> MeOut:
    rows = (
        db.scalars(
            select(TenantMembership)
            .where(TenantMembership.user_id == user.id)
            .options(joinedload(TenantMembership.tenant))
        )
        .unique()
        .all()
    )
    tenants: list[TenantBrief] = []
    for m in rows:
        t = m.tenant
        if t is None:
            t = db.get(Tenant, m.tenant_id)
        if t is None:
            continue
        tenants.append(TenantBrief(id=t.id, name=t.name, role=m.role))
    try:
        mode = TradingMode(user.current_trading_mode)
    except ValueError:
        mode = TradingMode.PAPER
    return MeOut(
        id=user.id,
        email=user.email,
        role="root_admin" if user.is_root_admin else "user",
        is_root_admin=user.is_root_admin,
        current_trading_mode=mode,
        email_verified_at=user.email_verified_at,
        tenants=tenants,
    )


@router.get("", response_model=MeOut)
def read_me(user: CurrentUser, db: DbSession) -> MeOut:
    return _build_me(db, user)


def _format_strategy_name(strategy_id: str) -> str:
    parts = [p for p in strategy_id.replace(".", "-").replace("_", "-").split("-") if p]
    return " ".join(p[:1].upper() + p[1:] for p in parts) if parts else strategy_id


def _to_float(value: str | None) -> float:
    if value is None:
        return 0.0
    try:
        return float(Decimal(value))
    except (InvalidOperation, ValueError):
        return 0.0


@router.get("/dashboard")
def dashboard_summary(
    user: CurrentUser,
    db: DbSession,
    trading_mode: TradingMode | None = None,
) -> dict[str, Any]:
    mode = (
        trading_mode.value if trading_mode is not None else (user.current_trading_mode or "paper")
    )
    tenant_id = primary_tenant_id(db, user)
    uses_tenant_scope = tenant_id is not None

    platform_rows = db.scalars(
        select(PlatformStrategy).order_by(PlatformStrategy.sort_order, PlatformStrategy.slug)
    ).all()
    configured = {
        row.strategy_id: row
        for row in db.query(UserStrategy)
        .filter(UserStrategy.user_id == user.id)
        .order_by(UserStrategy.strategy_id)
        .all()
    }

    enabled_strategies: list[dict[str, Any]] = []
    for row in platform_rows:
        assigned = (
            bool(tenant_id and has_strategy_entitlement(db, tenant_id, row.slug))
            if uses_tenant_scope
            else user.is_root_admin
        )
        if not assigned:
            continue
        configured_row = configured.get(row.slug)
        is_enabled = (
            configured_row.is_enabled
            if configured_row is not None
            else bool(user.is_root_admin and not uses_tenant_scope)
        )
        enabled_strategies.append(
            {
                "id": row.slug,
                "name": row.display_name or _format_strategy_name(row.slug),
                "enabled": is_enabled,
                "allocationPct": 0,
            }
        )

    buckets = (
        db.query(StrategyCapitalBucket)
        .filter(
            StrategyCapitalBucket.user_id == user.id,
            StrategyCapitalBucket.trading_mode == mode,
        )
        .all()
    )
    bucket_by_strategy = {b.strategy_id: b for b in buckets}
    total_assigned = sum(max(0, b.assigned_capital_cents) for b in buckets)
    for item in enabled_strategies:
        b = bucket_by_strategy.get(item["id"])
        assigned = b.assigned_capital_cents if b else 0
        item["allocationPct"] = (
            int(round((assigned / total_assigned) * 100)) if total_assigned > 0 else 0
        )

    available_balance_cents = sum(max(0, b.available_cash_cents) for b in buckets)
    portfolio_cents = sum(
        b.available_cash_cents
        + b.reserved_cash_cents
        + b.locked_capital_cents
        + b.unrealized_pnl_cents
        for b in buckets
    )
    pnl_cents = sum(b.realized_pnl_cents + b.unrealized_pnl_cents for b in buckets)
    portfolio_value = portfolio_cents / 100
    pnl_value = pnl_cents / 100
    base = max(1.0, portfolio_value - pnl_value)
    pnl_percent = (pnl_value / base) * 100

    positions_rows = (
        db.query(ExecutionPosition)
        .filter(
            ExecutionPosition.user_id == user.id,
            ExecutionPosition.trading_mode == mode,
        )
        .order_by(ExecutionPosition.updated_at.desc())
        .limit(50)
        .all()
    )
    positions: list[dict[str, Any]] = []
    for row in positions_rows:
        qty = _to_float(row.quantity)
        if abs(qty) <= 0.0:
            continue
        mark = _to_float(row.avg_entry_price)
        exposure = qty * mark
        positions.append(
            {
                "id": str(row.id),
                "symbol": row.symbol,
                "strategy": row.strategy_id,
                "side": "long" if qty >= 0 else "short",
                "quantity": row.quantity,
                "markPrice": mark,
                "unrealizedPnl": 0,
                "exposure": exposure,
            }
        )

    active_order_states = ("pending", "submitted", "open", "partially_filled")
    orders = (
        db.query(ExecutionOrder)
        .filter(
            ExecutionOrder.user_id == user.id,
            ExecutionOrder.trading_mode == mode,
            ExecutionOrder.state.in_(active_order_states),
        )
        .order_by(ExecutionOrder.created_at.desc())
        .limit(20)
        .all()
    )

    progress_map = {
        "pending": 15,
        "submitted": 35,
        "open": 60,
        "partially_filled": 80,
    }
    active_trades = [
        {
            "id": str(o.id),
            "symbol": o.symbol,
            "strategy": o.strategy_id,
            "status": o.state if o.state in {"pending", "partially_filled", "open"} else "open",
            "progressPct": progress_map.get(o.state, 60),
            "submittedAt": (o.submitted_at or o.created_at).strftime("%H:%M"),
            "notional": max(0, o.requested_notional_cents) / 100,
        }
        for o in orders
    ]

    trades = (
        db.query(ExecutionTradeRecord)
        .filter(
            ExecutionTradeRecord.user_id == user.id,
            ExecutionTradeRecord.trading_mode == mode,
        )
        .order_by(ExecutionTradeRecord.executed_at.desc())
        .limit(20)
        .all()
    )
    recent_activity = [
        {
            "id": str(t.id),
            "symbol": t.symbol,
            "side": "buy" if t.side.lower() == "buy" else "sell",
            "status": "filled",
            "amount": t.quantity,
            "price": _to_float(t.price),
            "timestamp": t.executed_at.strftime("%Y-%m-%d %H:%M"),
        }
        for t in trades
    ]

    growth_points = 8
    if growth_points <= 1:
        growth = [portfolio_value]
    else:
        start = max(0.0, portfolio_value - pnl_value)
        growth = [
            round(start + ((portfolio_value - start) * i / (growth_points - 1)), 2)
            for i in range(growth_points)
        ]

    return {
        "availableBalance": round(available_balance_cents / 100, 2),
        "portfolioValue": round(portfolio_value, 2),
        "pnlValue": round(pnl_value, 2),
        "pnlPercent": round(pnl_percent, 4),
        "gainLossLabel": "P&L",
        "growth": growth,
        "enabledStrategies": enabled_strategies,
        "positions": positions,
        "activeTrades": active_trades,
        "recentActivity": recent_activity,
    }


@router.patch("/trading-mode", response_model=MeOut)
def update_trading_mode(
    body: TradingModePatch,
    user: CurrentUser,
    db: DbSession,
) -> MeOut:
    tenant_id = primary_tenant_id(db, user)
    if tenant_id is None:
        raise HTTPException(status_code=400, detail="No tenant membership")
    ok, err = can_set_trading_mode(db, tenant_id=tenant_id, new_mode=body.trading_mode)
    if not ok:
        raise HTTPException(status_code=403, detail=err or "Cannot switch trading mode")
    user.current_trading_mode = body.trading_mode.value
    user.updated_at = datetime.now(UTC)
    db.add(user)
    return _build_me(db, user)
