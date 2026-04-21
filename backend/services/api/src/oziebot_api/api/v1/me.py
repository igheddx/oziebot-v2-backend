"""Current user profile and trading mode (tenant-safe)."""

from __future__ import annotations

from collections import defaultdict
from decimal import Decimal, InvalidOperation
from datetime import UTC, datetime, timedelta
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import case, func, select
from sqlalchemy.orm import joinedload

from oziebot_api.config import Settings
from oziebot_api.deps import DbSession, settings_dep
from oziebot_api.deps.auth import CurrentUser
from oziebot_api.models.risk_event import RiskEvent
from oziebot_api.models.membership import TenantMembership
from oziebot_api.models.execution import ExecutionOrder, ExecutionPosition, ExecutionTradeRecord
from oziebot_api.models.platform_strategy import PlatformStrategy
from oziebot_api.models.strategy_allocation import StrategyCapitalBucket
from oziebot_api.models.trade_intelligence import (
    StrategyDecisionAudit,
    StrategySignalSnapshot,
)
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
from oziebot_api.services.trade_review_analytics import (
    AnalyticsFilters,
    TradeReviewAnalyticsService,
)
from oziebot_api.services.trading_mode_policy import can_set_trading_mode
from oziebot_api.services.read_model_cache import ReadModelCache
from oziebot_domain.trading_mode import TradingMode

router = APIRouter(prefix="/me", tags=["me"])

DASHBOARD_CACHE_TTL_SECONDS = 30
ANALYTICS_CACHE_TTL_SECONDS = 120
DASHBOARD_HISTORY_LOOKBACK_DAYS = 30
DASHBOARD_POSITIONS_LIMIT = 50
DASHBOARD_ACTIVE_TRADES_LIMIT = 20
DASHBOARD_RECENT_ACTIVITY_LIMIT = 20
DASHBOARD_FEE_BREAKDOWN_LIMIT = 12
DASHBOARD_REJECTION_EVENT_LIMIT = 100
ANALYTICS_DEFAULT_LOOKBACK_DAYS = 30
ANALYTICS_MAX_LOOKBACK_DAYS = 90


def _dashboard_mode(user: User, trading_mode: TradingMode | None) -> str:
    return (
        trading_mode.value if trading_mode is not None else (user.current_trading_mode or "paper")
    )


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


def _analytics_filters(
    *,
    user: User,
    trading_mode: TradingMode | None,
    strategy_name: str | None,
    symbol: str | None,
    start_at: datetime | None,
    end_at: datetime | None,
) -> tuple[AnalyticsFilters, dict[str, Any]]:
    normalized_start_at, normalized_end_at, window_meta = _normalize_time_window(
        start_at=start_at,
        end_at=end_at,
        default_lookback_days=ANALYTICS_DEFAULT_LOOKBACK_DAYS,
        max_lookback_days=ANALYTICS_MAX_LOOKBACK_DAYS,
    )
    return (
        AnalyticsFilters(
            user_id=user.id,
            trading_mode=trading_mode.value if trading_mode is not None else None,
            strategy_name=strategy_name,
            symbol=symbol.upper() if symbol else None,
            start_at=normalized_start_at,
            end_at=normalized_end_at,
        ),
        window_meta,
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


def _normalize_time_window(
    *,
    start_at: datetime | None,
    end_at: datetime | None,
    default_lookback_days: int,
    max_lookback_days: int,
) -> tuple[datetime, datetime, dict[str, Any]]:
    now = datetime.now(UTC)
    normalized_end = _as_utc(end_at) or now
    if normalized_end > now:
        normalized_end = now
    requested_start = _as_utc(start_at)
    max_start = normalized_end - timedelta(days=max_lookback_days)
    default_start = normalized_end - timedelta(days=default_lookback_days)
    if requested_start is None:
        normalized_start = default_start
        defaulted = True
    else:
        normalized_start = requested_start
        defaulted = False
    if normalized_start > normalized_end:
        normalized_start = default_start
        defaulted = True
    window_clamped = normalized_start < max_start
    if window_clamped:
        normalized_start = max_start
    applied_days = max(1, int((normalized_end - normalized_start).total_seconds() // 86400) or 1)
    return (
        normalized_start,
        normalized_end,
        {
            "requestedStartAt": requested_start.isoformat() if requested_start else None,
            "requestedEndAt": _as_utc(end_at).isoformat() if end_at else None,
            "startAt": normalized_start.isoformat(),
            "endAt": normalized_end.isoformat(),
            "defaulted": defaulted,
            "windowClamped": window_clamped,
            "lookbackDaysApplied": min(applied_days, max_lookback_days),
        },
    )


def _cents(amount: Decimal) -> int:
    return int((amount * Decimal("100")).quantize(Decimal("1")))


def _format_rejection_record(
    *,
    stage: str,
    reason_code: str | None,
    reason_detail: str | None,
    strategy: str | None,
    symbol: str | None,
    created_at: datetime | None,
) -> dict[str, Any]:
    return {
        "stage": stage,
        "reasonCode": reason_code or "unspecified",
        "reasonDetail": reason_detail,
        "strategy": strategy,
        "symbol": symbol,
        "createdAt": _as_utc(created_at).isoformat() if created_at else None,
    }


def _build_rejection_diagnostics(
    *,
    strategy_records: list[dict[str, Any]],
    risk_records: list[dict[str, Any]],
    execution_records: list[dict[str, Any]],
) -> dict[str, Any]:
    all_records = [*strategy_records, *risk_records, *execution_records]
    all_records.sort(key=lambda item: item["createdAt"] or "", reverse=True)

    breakdown: dict[tuple[str, str], dict[str, Any]] = {}
    stage_counts: defaultdict[str, int] = defaultdict(int)
    for record in all_records:
        stage = str(record["stage"])
        reason_code = str(record["reasonCode"] or "unspecified")
        stage_counts[stage] += 1
        key = (stage, reason_code)
        entry = breakdown.get(key)
        if entry is None:
            entry = {
                "stage": stage,
                "reasonCode": reason_code,
                "count": 0,
                "lastSeenAt": record["createdAt"],
                "latestDetail": record["reasonDetail"],
                "strategies": [],
                "symbols": [],
            }
            breakdown[key] = entry
        entry["count"] += 1
        if record["strategy"] and record["strategy"] not in entry["strategies"]:
            entry["strategies"].append(record["strategy"])
        if record["symbol"] and record["symbol"] not in entry["symbols"]:
            entry["symbols"].append(record["symbol"])

    breakdown_rows = sorted(
        breakdown.values(),
        key=lambda item: (-int(item["count"]), str(item["stage"]), str(item["reasonCode"])),
    )
    by_stage = [
        {"stage": stage, "count": count}
        for stage, count in sorted(stage_counts.items(), key=lambda item: (-item[1], item[0]))
    ]
    return {
        "totalRejected": len(all_records),
        "byStage": by_stage,
        "breakdown": breakdown_rows[:8],
        "recent": all_records[:8],
    }


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


def _dashboard_cache_params(*, user: User, trading_mode: str) -> dict[str, Any]:
    return {"user_id": str(user.id), "trading_mode": trading_mode, "version": 2}


def _dashboard_summary_cache_params(*, user: User, trading_mode: str) -> dict[str, Any]:
    return {"user_id": str(user.id), "trading_mode": trading_mode, "version": 1}


def _dashboard_details_cache_params(*, user: User, trading_mode: str) -> dict[str, Any]:
    return {"user_id": str(user.id), "trading_mode": trading_mode, "version": 1}


def _dashboard_summary_payload(payload: dict[str, Any]) -> dict[str, Any]:
    fee_analytics = payload.get("feeAnalytics") or {}
    rejection_diagnostics = payload.get("rejectionDiagnostics") or {}
    return {
        "availableBalance": payload.get("availableBalance", 0),
        "portfolioValue": payload.get("portfolioValue", 0),
        "pnlValue": payload.get("pnlValue", 0),
        "pnlPercent": payload.get("pnlPercent", 0),
        "gainLossLabel": payload.get("gainLossLabel", "P&L"),
        "growth": payload.get("growth") or [],
        "positionsCount": len(payload.get("positions") or []),
        "activeTradesCount": len(payload.get("activeTrades") or []),
        "recentActivityCount": len(payload.get("recentActivity") or []),
        "totalFeesMonth": fee_analytics.get("totalFeesMonth", 0),
        "avgNetEdgeAtEntryBps": fee_analytics.get("avgNetEdgeAtEntryBps", 0),
        "totalRejected": rejection_diagnostics.get("totalRejected", 0),
        "budget": payload.get("budget") or {},
    }


def _dashboard_details_payload(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "enabledStrategies": payload.get("enabledStrategies") or [],
        "positions": payload.get("positions") or [],
        "activeTrades": payload.get("activeTrades") or [],
        "recentActivity": payload.get("recentActivity") or [],
        "feeAnalytics": payload.get("feeAnalytics") or {},
        "rejectionDiagnostics": payload.get("rejectionDiagnostics") or {},
        "budget": payload.get("budget") or {},
    }


def _analytics_cache_params(filters: AnalyticsFilters) -> dict[str, Any]:
    return {
        "user_id": str(filters.user_id),
        "trading_mode": filters.trading_mode,
        "strategy_name": filters.strategy_name,
        "symbol": filters.symbol,
        "start_at": _as_utc(filters.start_at).isoformat() if filters.start_at else None,
        "end_at": _as_utc(filters.end_at).isoformat() if filters.end_at else None,
        "version": 2,
    }


def _analytics_summary_payload(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "filters": payload.get("filters") or {},
        "summary": payload.get("summary") or {},
        "availableStrategies": payload.get("availableStrategies") or [],
        "availableSymbols": payload.get("availableSymbols") or [],
        "budget": payload.get("budget") or {},
    }


def _analytics_rows_payload(
    service: TradeReviewAnalyticsService,
    filters: AnalyticsFilters,
    *,
    grouping: str,
) -> dict[str, Any]:
    rows = {
        "strategy": service.build_strategy_rows,
        "token": service.build_token_rows,
        "pair": service.build_pair_rows,
    }[grouping](filters)
    return {
        "filters": service.filters_payload(filters),
        "budget": service.budget_payload(),
        "rows": rows,
    }


def _analytics_rejection_payload(
    service: TradeReviewAnalyticsService, filters: AnalyticsFilters
) -> dict[str, Any]:
    rejection_breakdown = service.build_rejection_breakdown(filters)
    return {
        "filters": service.filters_payload(filters),
        "budget": service.budget_payload(),
        "rejectionBreakdown": rejection_breakdown,
    }


def _analytics_comparison_payload(
    service: TradeReviewAnalyticsService, filters: AnalyticsFilters
) -> dict[str, Any]:
    comparison = service.build_paper_live_comparison(filters)
    return {
        "filters": service.filters_payload(filters),
        "budget": service.budget_payload(),
        "paperLiveComparison": comparison,
    }


def _cached_dashboard_payload(
    *,
    user: User,
    db: DbSession,
    settings: Settings,
    trading_mode: TradingMode | None,
    force_refresh: bool = False,
) -> dict[str, Any]:
    mode = _dashboard_mode(user, trading_mode)
    cache = ReadModelCache(settings)
    return cache.get_or_build(
        namespace="dashboard-v1",
        identity=str(user.id),
        params=_dashboard_cache_params(user=user, trading_mode=mode),
        ttl_seconds=DASHBOARD_CACHE_TTL_SECONDS,
        force_refresh=force_refresh,
        builder=lambda: _build_dashboard_payload(
            user=user,
            db=db,
            trading_mode=trading_mode,
            settings=settings,
        ),
    )


def _cached_dashboard_summary_payload(
    *,
    user: User,
    db: DbSession,
    settings: Settings,
    trading_mode: TradingMode | None,
    force_refresh: bool = False,
) -> dict[str, Any]:
    mode = _dashboard_mode(user, trading_mode)
    cache = ReadModelCache(settings)
    return cache.get_or_build(
        namespace="dashboard-summary-v1",
        identity=str(user.id),
        params=_dashboard_summary_cache_params(user=user, trading_mode=mode),
        ttl_seconds=DASHBOARD_CACHE_TTL_SECONDS,
        force_refresh=force_refresh,
        builder=lambda: _build_dashboard_summary_payload(
            user=user,
            db=db,
            trading_mode=trading_mode,
        ),
    )


def _cached_dashboard_details_payload(
    *,
    user: User,
    db: DbSession,
    settings: Settings,
    trading_mode: TradingMode | None,
    force_refresh: bool = False,
) -> dict[str, Any]:
    mode = _dashboard_mode(user, trading_mode)
    cache = ReadModelCache(settings)
    return cache.get_or_build(
        namespace="dashboard-details-v1",
        identity=str(user.id),
        params=_dashboard_details_cache_params(user=user, trading_mode=mode),
        ttl_seconds=DASHBOARD_CACHE_TTL_SECONDS,
        force_refresh=force_refresh,
        builder=lambda: _build_dashboard_payload(
            user=user,
            db=db,
            trading_mode=trading_mode,
            settings=settings,
            use_live_balances=False,
        ),
    )


def _cached_analytics_payload(
    *,
    user: User,
    db: DbSession,
    settings: Settings,
    trading_mode: TradingMode | None,
    strategy_name: str | None,
    symbol: str | None,
    start_at: datetime | None,
    end_at: datetime | None,
    force_refresh: bool = False,
) -> dict[str, Any]:
    filters, window_meta = _analytics_filters(
        user=user,
        trading_mode=trading_mode,
        strategy_name=strategy_name,
        symbol=symbol,
        start_at=start_at,
        end_at=end_at,
    )
    cache = ReadModelCache(settings)
    payload = cache.get_or_build(
        namespace="analytics-v1",
        identity=str(user.id),
        params=_analytics_cache_params(filters),
        ttl_seconds=ANALYTICS_CACHE_TTL_SECONDS,
        force_refresh=force_refresh,
        builder=lambda: TradeReviewAnalyticsService(db).build_overview(filters),
    )
    budget = dict(payload.get("budget") or {})
    budget.update(window_meta)
    return {**payload, "budget": budget}


def _cached_analytics_slice_payload(
    *,
    namespace: str,
    user: User,
    db: DbSession,
    settings: Settings,
    trading_mode: TradingMode | None,
    strategy_name: str | None,
    symbol: str | None,
    start_at: datetime | None,
    end_at: datetime | None,
    force_refresh: bool = False,
    builder,
) -> dict[str, Any]:
    filters, window_meta = _analytics_filters(
        user=user,
        trading_mode=trading_mode,
        strategy_name=strategy_name,
        symbol=symbol,
        start_at=start_at,
        end_at=end_at,
    )
    cache = ReadModelCache(settings)
    payload = cache.get_or_build(
        namespace=namespace,
        identity=str(user.id),
        params=_analytics_cache_params(filters),
        ttl_seconds=ANALYTICS_CACHE_TTL_SECONDS,
        force_refresh=force_refresh,
        builder=lambda: builder(TradeReviewAnalyticsService(db), filters),
    )
    budget = dict(payload.get("budget") or {})
    budget.update(window_meta)
    return {**payload, "budget": budget}


def _build_dashboard_summary_payload(
    user: CurrentUser,
    db: DbSession,
    trading_mode: TradingMode | None = None,
) -> dict[str, Any]:
    mode = _dashboard_mode(user, trading_mode)
    buckets = (
        db.query(StrategyCapitalBucket)
        .filter(
            StrategyCapitalBucket.user_id == user.id,
            StrategyCapitalBucket.trading_mode == mode,
        )
        .all()
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
    now = datetime.now(UTC)
    dashboard_cutoff = now - timedelta(days=DASHBOARD_HISTORY_LOOKBACK_DAYS)

    positions_count = int(
        db.scalar(
            select(func.count())
            .select_from(ExecutionPosition)
            .where(
                ExecutionPosition.user_id == user.id,
                ExecutionPosition.trading_mode == mode,
            )
        )
        or 0
    )
    active_order_states = ("pending", "submitted", "open", "partially_filled")
    active_trades_count = int(
        db.scalar(
            select(func.count())
            .select_from(ExecutionOrder)
            .where(
                ExecutionOrder.user_id == user.id,
                ExecutionOrder.trading_mode == mode,
                ExecutionOrder.state.in_(active_order_states),
            )
        )
        or 0
    )
    recent_activity_total = int(
        db.scalar(
            select(func.count())
            .select_from(ExecutionTradeRecord)
            .where(
                ExecutionTradeRecord.user_id == user.id,
                ExecutionTradeRecord.trading_mode == mode,
                ExecutionTradeRecord.executed_at >= dashboard_cutoff,
            )
        )
        or 0
    )
    month_cutoff = now - timedelta(days=30)
    trade_stats = db.execute(
        select(
            func.coalesce(
                func.sum(
                    case(
                        (
                            ExecutionTradeRecord.executed_at >= month_cutoff,
                            ExecutionTradeRecord.fee_cents,
                        ),
                        else_=0,
                    )
                ),
                0,
            ).label("fees_month_cents"),
        ).where(
            ExecutionTradeRecord.user_id == user.id,
            ExecutionTradeRecord.trading_mode == mode,
            ExecutionTradeRecord.executed_at >= dashboard_cutoff,
        )
    ).one()
    entry_order_stats = db.execute(
        select(
            func.coalesce(func.avg(ExecutionOrder.expected_net_edge_bps), 0).label(
                "avg_net_edge_bps"
            ),
        ).where(
            ExecutionOrder.user_id == user.id,
            ExecutionOrder.trading_mode == mode,
            ExecutionOrder.side == "buy",
            ExecutionOrder.state.in_(("submitted", "pending", "partially_filled", "filled")),
            ExecutionOrder.created_at >= dashboard_cutoff,
        )
    ).one()
    strategy_reject_total = int(
        db.scalar(
            select(func.count())
            .select_from(StrategyDecisionAudit)
            .join(
                StrategySignalSnapshot,
                StrategyDecisionAudit.signal_snapshot_id == StrategySignalSnapshot.id,
            )
            .where(
                StrategySignalSnapshot.user_id == user.id,
                StrategySignalSnapshot.trading_mode == mode,
                StrategyDecisionAudit.decision == "rejected",
                StrategyDecisionAudit.stage.in_(("strategy", "suppression")),
                StrategyDecisionAudit.created_at >= dashboard_cutoff,
            )
        )
        or 0
    )
    risk_reject_total = int(
        db.scalar(
            select(func.count())
            .select_from(RiskEvent)
            .where(
                RiskEvent.user_id == user.id,
                RiskEvent.trading_mode == mode,
                RiskEvent.outcome == "reject",
                RiskEvent.created_at >= dashboard_cutoff,
            )
        )
        or 0
    )
    execution_reject_total = int(
        db.scalar(
            select(func.count())
            .select_from(ExecutionOrder)
            .where(
                ExecutionOrder.user_id == user.id,
                ExecutionOrder.trading_mode == mode,
                ExecutionOrder.state.in_(("failed", "cancelled")),
                func.coalesce(
                    ExecutionOrder.failed_at,
                    ExecutionOrder.cancelled_at,
                    ExecutionOrder.updated_at,
                )
                >= dashboard_cutoff,
            )
        )
        or 0
    )
    growth_points = 8
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
        "positions": [None] * min(positions_count, DASHBOARD_POSITIONS_LIMIT),
        "activeTrades": [None] * min(active_trades_count, DASHBOARD_ACTIVE_TRADES_LIMIT),
        "recentActivity": [None] * min(recent_activity_total, DASHBOARD_RECENT_ACTIVITY_LIMIT),
        "feeAnalytics": {
            "totalFeesMonth": round(int(trade_stats.fees_month_cents or 0) / 100, 2),
            "avgNetEdgeAtEntryBps": round(float(entry_order_stats.avg_net_edge_bps or 0), 2),
        },
        "rejectionDiagnostics": {
            "totalRejected": strategy_reject_total + risk_reject_total + execution_reject_total,
        },
        "budget": {
            "historyLookbackDaysApplied": DASHBOARD_HISTORY_LOOKBACK_DAYS,
            "summaryOnly": True,
            "positionLimit": DASHBOARD_POSITIONS_LIMIT,
            "activeTradeLimit": DASHBOARD_ACTIVE_TRADES_LIMIT,
            "recentActivityLimit": DASHBOARD_RECENT_ACTIVITY_LIMIT,
            "historyStartAt": dashboard_cutoff.isoformat(),
            "historyEndAt": now.isoformat(),
        },
    }


def _build_dashboard_payload(
    user: CurrentUser,
    db: DbSession,
    settings: Settings,
    trading_mode: TradingMode | None = None,
    *,
    use_live_balances: bool = True,
) -> dict[str, Any]:
    mode = _dashboard_mode(user, trading_mode)
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
        .limit(DASHBOARD_POSITIONS_LIMIT)
        .all()
    )
    if use_live_balances and mode == TradingMode.LIVE.value and tenant_id is not None:
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
        .limit(DASHBOARD_ACTIVE_TRADES_LIMIT)
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

    now = datetime.now(UTC)
    dashboard_cutoff = now - timedelta(days=DASHBOARD_HISTORY_LOOKBACK_DAYS)
    trades = (
        db.query(ExecutionTradeRecord)
        .filter(
            ExecutionTradeRecord.user_id == user.id,
            ExecutionTradeRecord.trading_mode == mode,
            ExecutionTradeRecord.executed_at >= dashboard_cutoff,
        )
        .order_by(ExecutionTradeRecord.executed_at.desc())
        .limit(DASHBOARD_RECENT_ACTIVITY_LIMIT)
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

    today_cutoff = now - timedelta(days=1)
    week_cutoff = now - timedelta(days=7)
    month_cutoff = now - timedelta(days=30)
    trade_stats = db.execute(
        select(
            func.coalesce(func.sum(ExecutionTradeRecord.fee_cents), 0).label("total_fees_cents"),
            func.coalesce(func.sum(ExecutionTradeRecord.realized_pnl_cents), 0).label(
                "total_net_pnl_cents"
            ),
            func.coalesce(
                func.sum(
                    case(
                        (
                            ExecutionTradeRecord.executed_at >= today_cutoff,
                            ExecutionTradeRecord.fee_cents,
                        ),
                        else_=0,
                    )
                ),
                0,
            ).label("fees_today_cents"),
            func.coalesce(
                func.sum(
                    case(
                        (
                            ExecutionTradeRecord.executed_at >= week_cutoff,
                            ExecutionTradeRecord.fee_cents,
                        ),
                        else_=0,
                    )
                ),
                0,
            ).label("fees_week_cents"),
            func.coalesce(
                func.sum(
                    case(
                        (
                            ExecutionTradeRecord.executed_at >= month_cutoff,
                            ExecutionTradeRecord.fee_cents,
                        ),
                        else_=0,
                    )
                ),
                0,
            ).label("fees_month_cents"),
        ).where(
            ExecutionTradeRecord.user_id == user.id,
            ExecutionTradeRecord.trading_mode == mode,
            ExecutionTradeRecord.executed_at >= dashboard_cutoff,
        )
    ).one()
    fees_by_strategy_rows = db.execute(
        select(
            ExecutionTradeRecord.strategy_id,
            func.coalesce(func.sum(ExecutionTradeRecord.fee_cents), 0).label("fees_cents"),
        )
        .where(
            ExecutionTradeRecord.user_id == user.id,
            ExecutionTradeRecord.trading_mode == mode,
            ExecutionTradeRecord.executed_at >= dashboard_cutoff,
        )
        .group_by(ExecutionTradeRecord.strategy_id)
        .order_by(func.sum(ExecutionTradeRecord.fee_cents).desc())
        .limit(DASHBOARD_FEE_BREAKDOWN_LIMIT)
    ).all()
    fees_by_symbol_rows = db.execute(
        select(
            ExecutionTradeRecord.symbol,
            func.coalesce(func.sum(ExecutionTradeRecord.fee_cents), 0).label("fees_cents"),
        )
        .where(
            ExecutionTradeRecord.user_id == user.id,
            ExecutionTradeRecord.trading_mode == mode,
            ExecutionTradeRecord.executed_at >= dashboard_cutoff,
        )
        .group_by(ExecutionTradeRecord.symbol)
        .order_by(func.sum(ExecutionTradeRecord.fee_cents).desc())
        .limit(DASHBOARD_FEE_BREAKDOWN_LIMIT)
    ).all()
    fill_mix = db.execute(
        select(
            func.coalesce(
                func.sum(case((ExecutionOrder.actual_fill_type == "maker", 1), else_=0)), 0
            ).label("maker_count"),
            func.coalesce(
                func.sum(case((ExecutionOrder.actual_fill_type == "taker", 1), else_=0)), 0
            ).label("taker_count"),
            func.coalesce(
                func.sum(case((ExecutionOrder.actual_fill_type == "mixed", 1), else_=0)), 0
            ).label("mixed_count"),
        ).where(
            ExecutionOrder.user_id == user.id,
            ExecutionOrder.trading_mode == mode,
            ExecutionOrder.created_at >= dashboard_cutoff,
        )
    ).one()
    entry_order_stats = db.execute(
        select(
            func.coalesce(func.avg(ExecutionOrder.estimated_slippage_bps), 0).label(
                "avg_slippage_bps"
            ),
            func.coalesce(func.avg(ExecutionOrder.expected_net_edge_bps), 0).label(
                "avg_net_edge_bps"
            ),
        ).where(
            ExecutionOrder.user_id == user.id,
            ExecutionOrder.trading_mode == mode,
            ExecutionOrder.side == "buy",
            ExecutionOrder.state.in_(("submitted", "pending", "partially_filled", "filled")),
            ExecutionOrder.created_at >= dashboard_cutoff,
        )
    ).one()
    skipped_due_to_fees = int(
        db.scalar(
            select(func.count())
            .select_from(RiskEvent)
            .where(
                RiskEvent.user_id == user.id,
                RiskEvent.trading_mode == mode,
                RiskEvent.detail.is_not(None),
                RiskEvent.detail.ilike("%fee_economics%"),
                RiskEvent.created_at >= dashboard_cutoff,
            )
        )
        or 0
    )
    strategy_audits = (
        db.query(StrategyDecisionAudit, StrategySignalSnapshot)
        .join(
            StrategySignalSnapshot,
            StrategyDecisionAudit.signal_snapshot_id == StrategySignalSnapshot.id,
        )
        .filter(
            StrategySignalSnapshot.user_id == user.id,
            StrategySignalSnapshot.trading_mode == mode,
            StrategyDecisionAudit.decision == "rejected",
            StrategyDecisionAudit.stage.in_(("strategy", "suppression")),
            StrategyDecisionAudit.created_at >= dashboard_cutoff,
        )
        .order_by(StrategyDecisionAudit.created_at.desc())
        .limit(DASHBOARD_REJECTION_EVENT_LIMIT)
        .all()
    )
    recent_risk_rejects = (
        db.query(RiskEvent)
        .filter(
            RiskEvent.user_id == user.id,
            RiskEvent.trading_mode == mode,
            RiskEvent.outcome == "reject",
            RiskEvent.created_at >= dashboard_cutoff,
        )
        .order_by(RiskEvent.created_at.desc())
        .limit(DASHBOARD_REJECTION_EVENT_LIMIT)
        .all()
    )
    recent_failed_orders = (
        db.query(ExecutionOrder)
        .filter(
            ExecutionOrder.user_id == user.id,
            ExecutionOrder.trading_mode == mode,
            ExecutionOrder.state.in_(("failed", "cancelled")),
            func.coalesce(
                ExecutionOrder.failed_at,
                ExecutionOrder.cancelled_at,
                ExecutionOrder.updated_at,
            )
            >= dashboard_cutoff,
        )
        .order_by(
            func.coalesce(
                ExecutionOrder.failed_at,
                ExecutionOrder.cancelled_at,
                ExecutionOrder.updated_at,
            ).desc()
        )
        .limit(DASHBOARD_REJECTION_EVENT_LIMIT)
        .all()
    )
    strategy_reject_total = int(
        db.scalar(
            select(func.count())
            .select_from(StrategyDecisionAudit)
            .join(
                StrategySignalSnapshot,
                StrategyDecisionAudit.signal_snapshot_id == StrategySignalSnapshot.id,
            )
            .where(
                StrategySignalSnapshot.user_id == user.id,
                StrategySignalSnapshot.trading_mode == mode,
                StrategyDecisionAudit.decision == "rejected",
                StrategyDecisionAudit.stage.in_(("strategy", "suppression")),
                StrategyDecisionAudit.created_at >= dashboard_cutoff,
            )
        )
        or 0
    )
    risk_reject_total = int(
        db.scalar(
            select(func.count())
            .select_from(RiskEvent)
            .where(
                RiskEvent.user_id == user.id,
                RiskEvent.trading_mode == mode,
                RiskEvent.outcome == "reject",
                RiskEvent.created_at >= dashboard_cutoff,
            )
        )
        or 0
    )
    execution_reject_total = int(
        db.scalar(
            select(func.count())
            .select_from(ExecutionOrder)
            .where(
                ExecutionOrder.user_id == user.id,
                ExecutionOrder.trading_mode == mode,
                ExecutionOrder.state.in_(("failed", "cancelled")),
                func.coalesce(
                    ExecutionOrder.failed_at,
                    ExecutionOrder.cancelled_at,
                    ExecutionOrder.updated_at,
                )
                >= dashboard_cutoff,
            )
        )
        or 0
    )
    total_mode_fees_cents = int(trade_stats.total_fees_cents or 0)
    total_mode_net_pnl_cents = int(trade_stats.total_net_pnl_cents or 0)
    total_mode_gross_pnl_cents = total_mode_net_pnl_cents + total_mode_fees_cents
    rejection_diagnostics = _build_rejection_diagnostics(
        strategy_records=[
            _format_rejection_record(
                stage=str(audit.stage),
                reason_code=audit.reason_code,
                reason_detail=audit.reason_detail,
                strategy=snapshot.strategy_name,
                symbol=snapshot.token_symbol,
                created_at=audit.created_at,
            )
            for audit, snapshot in strategy_audits
        ],
        risk_records=[
            _format_rejection_record(
                stage="risk",
                reason_code=event.reason,
                reason_detail=event.detail,
                strategy=event.strategy_name,
                symbol=event.symbol,
                created_at=event.created_at,
            )
            for event in recent_risk_rejects
        ],
        execution_records=[
            _format_rejection_record(
                stage="execution",
                reason_code=order.failure_code or order.state,
                reason_detail=order.failure_detail or f"Order {order.state}",
                strategy=order.strategy_id,
                symbol=order.symbol,
                created_at=order.failed_at or order.cancelled_at or order.updated_at,
            )
            for order in recent_failed_orders
        ],
    )
    rejection_diagnostics["byStage"] = [
        {"stage": "risk", "count": risk_reject_total},
        {"stage": "suppression", "count": strategy_reject_total},
        {"stage": "execution", "count": execution_reject_total},
    ]
    rejection_diagnostics["byStage"] = [
        row for row in rejection_diagnostics["byStage"] if int(row["count"]) > 0
    ]
    rejection_diagnostics["byStage"].sort(key=lambda row: (-int(row["count"]), str(row["stage"])))
    rejection_diagnostics["totalRejected"] = (
        strategy_reject_total + risk_reject_total + execution_reject_total
    )

    comparison: dict[str, dict[str, float]] = {
        "paper": {"fees": 0.0, "netPnl": 0.0},
        "live": {"fees": 0.0, "netPnl": 0.0},
    }
    comparison_rows = db.execute(
        select(
            ExecutionTradeRecord.trading_mode,
            func.coalesce(func.sum(ExecutionTradeRecord.fee_cents), 0).label("fees_cents"),
            func.coalesce(func.sum(ExecutionTradeRecord.realized_pnl_cents), 0).label(
                "net_pnl_cents"
            ),
        )
        .where(
            ExecutionTradeRecord.user_id == user.id,
            ExecutionTradeRecord.executed_at >= dashboard_cutoff,
        )
        .group_by(ExecutionTradeRecord.trading_mode)
    ).all()
    for row in comparison_rows:
        comparison[str(row.trading_mode)] = {
            "fees": round(int(row.fees_cents or 0) / 100, 2),
            "netPnl": round(int(row.net_pnl_cents or 0) / 100, 2),
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
        "budget": {
            "historyLookbackDaysApplied": DASHBOARD_HISTORY_LOOKBACK_DAYS,
            "positionLimit": DASHBOARD_POSITIONS_LIMIT,
            "activeTradeLimit": DASHBOARD_ACTIVE_TRADES_LIMIT,
            "recentActivityLimit": DASHBOARD_RECENT_ACTIVITY_LIMIT,
            "feeBreakdownLimit": DASHBOARD_FEE_BREAKDOWN_LIMIT,
            "rejectionEventLimit": DASHBOARD_REJECTION_EVENT_LIMIT,
            "historyStartAt": dashboard_cutoff.isoformat(),
            "historyEndAt": now.isoformat(),
        },
        "feeAnalytics": {
            "grossPnl": round(total_mode_gross_pnl_cents / 100, 2),
            "netPnl": round(total_mode_net_pnl_cents / 100, 2),
            "totalFeesToday": round(int(trade_stats.fees_today_cents or 0) / 100, 2),
            "totalFeesWeek": round(int(trade_stats.fees_week_cents or 0) / 100, 2),
            "totalFeesMonth": round(int(trade_stats.fees_month_cents or 0) / 100, 2),
            "feesByStrategy": [
                {"strategy": str(strategy), "fees": round(int(cents or 0) / 100, 2)}
                for strategy, cents in fees_by_strategy_rows
            ],
            "feesBySymbol": [
                {"symbol": str(symbol), "fees": round(int(cents or 0) / 100, 2)}
                for symbol, cents in fees_by_symbol_rows
            ],
            "makerCount": int(fill_mix.maker_count or 0),
            "takerCount": int(fill_mix.taker_count or 0),
            "mixedCount": int(fill_mix.mixed_count or 0),
            "avgEstimatedSlippageBps": round(float(entry_order_stats.avg_slippage_bps or 0), 2),
            "avgNetEdgeAtEntryBps": round(float(entry_order_stats.avg_net_edge_bps or 0), 2),
            "skippedTradesDueToFees": skipped_due_to_fees,
            "paperLiveComparison": comparison,
        },
        "rejectionDiagnostics": rejection_diagnostics,
    }


@router.get("/dashboard")
def dashboard_summary(
    user: CurrentUser,
    db: DbSession,
    trading_mode: TradingMode | None = None,
    force_refresh: bool = Query(default=False),
    settings: Settings = Depends(settings_dep),
) -> dict[str, Any]:
    return _cached_dashboard_payload(
        user=user,
        db=db,
        settings=settings,
        trading_mode=trading_mode,
        force_refresh=force_refresh,
    )


@router.get("/dashboard/summary")
def dashboard_summary_overview(
    user: CurrentUser,
    db: DbSession,
    trading_mode: TradingMode | None = None,
    force_refresh: bool = Query(default=False),
    settings: Settings = Depends(settings_dep),
) -> dict[str, Any]:
    payload = _cached_dashboard_summary_payload(
        user=user,
        db=db,
        settings=settings,
        trading_mode=trading_mode,
        force_refresh=force_refresh,
    )
    return _dashboard_summary_payload(payload)


@router.get("/dashboard/details")
def dashboard_summary_details(
    user: CurrentUser,
    db: DbSession,
    trading_mode: TradingMode | None = None,
    force_refresh: bool = Query(default=False),
    settings: Settings = Depends(settings_dep),
) -> dict[str, Any]:
    payload = _cached_dashboard_details_payload(
        user=user,
        db=db,
        settings=settings,
        trading_mode=trading_mode,
        force_refresh=force_refresh,
    )
    return _dashboard_details_payload(payload)


@router.get("/analytics")
def read_trade_review_analytics(
    user: CurrentUser,
    db: DbSession,
    trading_mode: TradingMode | None = None,
    strategy_name: str | None = Query(default=None),
    symbol: str | None = Query(default=None),
    start_at: datetime | None = Query(default=None),
    end_at: datetime | None = Query(default=None),
    force_refresh: bool = Query(default=False),
    settings: Settings = Depends(settings_dep),
) -> dict[str, Any]:
    return _cached_analytics_payload(
        user=user,
        db=db,
        settings=settings,
        trading_mode=trading_mode,
        strategy_name=strategy_name,
        symbol=symbol,
        start_at=start_at,
        end_at=end_at,
        force_refresh=force_refresh,
    )


@router.get("/analytics/summary")
def read_trade_review_analytics_summary(
    user: CurrentUser,
    db: DbSession,
    trading_mode: TradingMode | None = None,
    strategy_name: str | None = Query(default=None),
    symbol: str | None = Query(default=None),
    start_at: datetime | None = Query(default=None),
    end_at: datetime | None = Query(default=None),
    force_refresh: bool = Query(default=False),
    settings: Settings = Depends(settings_dep),
) -> dict[str, Any]:
    payload = _cached_analytics_slice_payload(
        namespace="analytics-summary-v1",
        user=user,
        db=db,
        settings=settings,
        trading_mode=trading_mode,
        strategy_name=strategy_name,
        symbol=symbol,
        start_at=start_at,
        end_at=end_at,
        force_refresh=force_refresh,
        builder=lambda service, filters: service.build_summary(filters),
    )
    return _analytics_summary_payload(payload)


@router.get("/analytics/strategies")
def read_trade_review_strategy_rows(
    user: CurrentUser,
    db: DbSession,
    trading_mode: TradingMode | None = None,
    strategy_name: str | None = Query(default=None),
    symbol: str | None = Query(default=None),
    start_at: datetime | None = Query(default=None),
    end_at: datetime | None = Query(default=None),
    force_refresh: bool = Query(default=False),
    settings: Settings = Depends(settings_dep),
) -> dict[str, Any]:
    return _cached_analytics_slice_payload(
        namespace="analytics-strategies-v1",
        user=user,
        db=db,
        settings=settings,
        trading_mode=trading_mode,
        strategy_name=strategy_name,
        symbol=symbol,
        start_at=start_at,
        end_at=end_at,
        force_refresh=force_refresh,
        builder=lambda service, filters: _analytics_rows_payload(
            service, filters, grouping="strategy"
        ),
    )


@router.get("/analytics/tokens")
def read_trade_review_token_rows(
    user: CurrentUser,
    db: DbSession,
    trading_mode: TradingMode | None = None,
    strategy_name: str | None = Query(default=None),
    symbol: str | None = Query(default=None),
    start_at: datetime | None = Query(default=None),
    end_at: datetime | None = Query(default=None),
    force_refresh: bool = Query(default=False),
    settings: Settings = Depends(settings_dep),
) -> dict[str, Any]:
    return _cached_analytics_slice_payload(
        namespace="analytics-tokens-v1",
        user=user,
        db=db,
        settings=settings,
        trading_mode=trading_mode,
        strategy_name=strategy_name,
        symbol=symbol,
        start_at=start_at,
        end_at=end_at,
        force_refresh=force_refresh,
        builder=lambda service, filters: _analytics_rows_payload(
            service, filters, grouping="token"
        ),
    )


@router.get("/analytics/pairs")
def read_trade_review_pair_rows(
    user: CurrentUser,
    db: DbSession,
    trading_mode: TradingMode | None = None,
    strategy_name: str | None = Query(default=None),
    symbol: str | None = Query(default=None),
    start_at: datetime | None = Query(default=None),
    end_at: datetime | None = Query(default=None),
    force_refresh: bool = Query(default=False),
    settings: Settings = Depends(settings_dep),
) -> dict[str, Any]:
    return _cached_analytics_slice_payload(
        namespace="analytics-pairs-v1",
        user=user,
        db=db,
        settings=settings,
        trading_mode=trading_mode,
        strategy_name=strategy_name,
        symbol=symbol,
        start_at=start_at,
        end_at=end_at,
        force_refresh=force_refresh,
        builder=lambda service, filters: _analytics_rows_payload(service, filters, grouping="pair"),
    )


@router.get("/analytics/rejections")
def read_trade_review_rejection_breakdown(
    user: CurrentUser,
    db: DbSession,
    trading_mode: TradingMode | None = None,
    strategy_name: str | None = Query(default=None),
    symbol: str | None = Query(default=None),
    start_at: datetime | None = Query(default=None),
    end_at: datetime | None = Query(default=None),
    force_refresh: bool = Query(default=False),
    settings: Settings = Depends(settings_dep),
) -> dict[str, Any]:
    return _cached_analytics_slice_payload(
        namespace="analytics-rejections-v1",
        user=user,
        db=db,
        settings=settings,
        trading_mode=trading_mode,
        strategy_name=strategy_name,
        symbol=symbol,
        start_at=start_at,
        end_at=end_at,
        force_refresh=force_refresh,
        builder=_analytics_rejection_payload,
    )


@router.get("/analytics/comparison")
def read_trade_review_paper_live_comparison(
    user: CurrentUser,
    db: DbSession,
    trading_mode: TradingMode | None = None,
    strategy_name: str | None = Query(default=None),
    symbol: str | None = Query(default=None),
    start_at: datetime | None = Query(default=None),
    end_at: datetime | None = Query(default=None),
    force_refresh: bool = Query(default=False),
    settings: Settings = Depends(settings_dep),
) -> dict[str, Any]:
    return _cached_analytics_slice_payload(
        namespace="analytics-comparison-v1",
        user=user,
        db=db,
        settings=settings,
        trading_mode=trading_mode,
        strategy_name=strategy_name,
        symbol=symbol,
        start_at=start_at,
        end_at=end_at,
        force_refresh=force_refresh,
        builder=_analytics_comparison_payload,
    )


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
