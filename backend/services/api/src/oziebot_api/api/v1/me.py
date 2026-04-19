"""Current user profile and trading mode (tenant-safe)."""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
from datetime import UTC, datetime, timedelta
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.orm import joinedload

from oziebot_api.config import Settings
from oziebot_api.deps import DbSession, settings_dep
from oziebot_api.deps.auth import CurrentUser
from oziebot_api.models.risk_event import RiskEvent
from oziebot_api.models.membership import TenantMembership
from oziebot_api.models.execution import ExecutionOrder, ExecutionPosition, ExecutionTradeRecord
from oziebot_api.models.platform_strategy import PlatformStrategy
from oziebot_api.models.strategy_allocation import StrategyCapitalBucket
from oziebot_api.models.tenant import Tenant
from oziebot_api.models.user import User
from oziebot_api.models.user_strategy import UserStrategy
from oziebot_api.schemas.me import MeOut, TenantBrief, TradingModePatch
from oziebot_api.services.entitlements import has_strategy_entitlement
from oziebot_api.services.live_coinbase import (
    CASH_EQUIVALENT_CURRENCIES,
    load_live_coinbase_accounts,
    sum_coinbase_cash_cents,
)
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


def _to_decimal(value: str | None) -> Decimal:
    if value is None:
        return Decimal("0")
    try:
        return Decimal(value)
    except (InvalidOperation, ValueError):
        return Decimal("0")


def _as_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _cents(amount: Decimal) -> int:
    return int((amount * Decimal("100")).quantize(Decimal("1")))


def _live_coinbase_balance_snapshot(
    db: DbSession,
    user: User,
    settings: Settings,
    positions_rows: list[ExecutionPosition],
) -> tuple[int, int] | None:
    accounts = load_live_coinbase_accounts(db, user=user, settings=settings)
    if accounts is None:
        return None

    mark_prices: dict[str, Decimal] = {}
    for row in positions_rows:
        symbol = (row.symbol or "").upper()
        if "-" not in symbol:
            continue
        base_currency = symbol.split("-", 1)[0]
        if base_currency and base_currency not in mark_prices:
            mark_prices[base_currency] = _to_decimal(row.avg_entry_price)

    available_balance_cents = sum_coinbase_cash_cents(accounts, include_hold=False)
    portfolio_cents = sum_coinbase_cash_cents(accounts, include_hold=True)
    for account in accounts:
        currency = str(
            account.get("currency")
            or (account.get("available_balance") or {}).get("currency")
            or ""
        ).upper()
        available = _to_decimal((account.get("available_balance") or {}).get("value"))
        hold = _to_decimal((account.get("hold") or {}).get("value"))
        total = available + hold
        if total <= 0:
            continue
        if currency in CASH_EQUIVALENT_CURRENCIES:
            continue
        mark_price = mark_prices.get(currency)
        if mark_price is None or mark_price <= 0:
            continue
        portfolio_cents += _cents(total * mark_price)

    return available_balance_cents, portfolio_cents


@router.get("/dashboard")
def dashboard_summary(
    user: CurrentUser,
    db: DbSession,
    trading_mode: TradingMode | None = None,
    settings: Settings = Depends(settings_dep),
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
    if mode == TradingMode.LIVE.value and tenant_id is not None:
        live_balances = _live_coinbase_balance_snapshot(db, user, settings, positions_rows)
        if live_balances is not None:
            available_balance_cents, portfolio_cents = live_balances
            portfolio_value = portfolio_cents / 100
            base = max(1.0, portfolio_value - pnl_value)
            pnl_percent = (pnl_value / base) * 100
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

    now = datetime.now(UTC)
    today_cutoff = now - timedelta(days=1)
    week_cutoff = now - timedelta(days=7)
    month_cutoff = now - timedelta(days=30)
    mode_orders = (
        db.query(ExecutionOrder)
        .filter(
            ExecutionOrder.user_id == user.id,
            ExecutionOrder.trading_mode == mode,
        )
        .all()
    )
    mode_trades = (
        db.query(ExecutionTradeRecord)
        .filter(
            ExecutionTradeRecord.user_id == user.id,
            ExecutionTradeRecord.trading_mode == mode,
        )
        .all()
    )
    mode_risk_events = (
        db.query(RiskEvent)
        .filter(
            RiskEvent.user_id == user.id,
            RiskEvent.trading_mode == mode,
        )
        .all()
    )

    def _fees_since(cutoff: datetime) -> float:
        return round(
            sum(
                (trade.fee_cents or 0)
                for trade in mode_trades
                if _as_utc(trade.executed_at) and _as_utc(trade.executed_at) >= cutoff
            )
            / 100,
            2,
        )

    fees_by_strategy: dict[str, int] = {}
    fees_by_symbol: dict[str, int] = {}
    for trade in mode_trades:
        fees_by_strategy[trade.strategy_id] = fees_by_strategy.get(trade.strategy_id, 0) + int(
            trade.fee_cents or 0
        )
        fees_by_symbol[trade.symbol] = fees_by_symbol.get(trade.symbol, 0) + int(
            trade.fee_cents or 0
        )
    executed_entry_orders = [
        order
        for order in mode_orders
        if order.side.lower() == "buy"
        and order.state in {"submitted", "pending", "partially_filled", "filled"}
    ]
    maker_count = sum(1 for order in mode_orders if (order.actual_fill_type or "") == "maker")
    taker_count = sum(1 for order in mode_orders if (order.actual_fill_type or "") == "taker")
    mixed_count = sum(1 for order in mode_orders if (order.actual_fill_type or "") == "mixed")
    skipped_due_to_fees = sum(
        1
        for event in mode_risk_events
        if (event.detail or "").startswith("fee_economics:")
        or (event.detail or "").find("fee_economics") >= 0
    )
    total_mode_fees_cents = sum(int(trade.fee_cents or 0) for trade in mode_trades)
    total_mode_net_pnl_cents = sum(int(trade.realized_pnl_cents or 0) for trade in mode_trades)
    total_mode_gross_pnl_cents = total_mode_net_pnl_cents + total_mode_fees_cents

    comparison: dict[str, dict[str, float]] = {}
    for compare_mode in ("paper", "live"):
        compare_trades = (
            db.query(ExecutionTradeRecord)
            .filter(
                ExecutionTradeRecord.user_id == user.id,
                ExecutionTradeRecord.trading_mode == compare_mode,
            )
            .all()
        )
        fees_cents = sum(int(trade.fee_cents or 0) for trade in compare_trades)
        net_pnl_cents = sum(int(trade.realized_pnl_cents or 0) for trade in compare_trades)
        comparison[compare_mode] = {
            "fees": round(fees_cents / 100, 2),
            "netPnl": round(net_pnl_cents / 100, 2),
        }

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
        "feeAnalytics": {
            "grossPnl": round(total_mode_gross_pnl_cents / 100, 2),
            "netPnl": round(total_mode_net_pnl_cents / 100, 2),
            "totalFeesToday": _fees_since(today_cutoff),
            "totalFeesWeek": _fees_since(week_cutoff),
            "totalFeesMonth": _fees_since(month_cutoff),
            "feesByStrategy": [
                {"strategy": strategy, "fees": round(cents / 100, 2)}
                for strategy, cents in sorted(
                    fees_by_strategy.items(), key=lambda item: item[1], reverse=True
                )
            ],
            "feesBySymbol": [
                {"symbol": symbol, "fees": round(cents / 100, 2)}
                for symbol, cents in sorted(
                    fees_by_symbol.items(), key=lambda item: item[1], reverse=True
                )
            ],
            "makerCount": maker_count,
            "takerCount": taker_count,
            "mixedCount": mixed_count,
            "avgEstimatedSlippageBps": round(
                (
                    sum(order.estimated_slippage_bps or 0 for order in executed_entry_orders)
                    / max(1, len(executed_entry_orders))
                ),
                2,
            ),
            "avgNetEdgeAtEntryBps": round(
                (
                    sum(order.expected_net_edge_bps or 0 for order in executed_entry_orders)
                    / max(1, len(executed_entry_orders))
                ),
                2,
            ),
            "skippedTradesDueToFees": skipped_due_to_fees,
            "paperLiveComparison": comparison,
        },
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
