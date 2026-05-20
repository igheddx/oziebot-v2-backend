"""Current user profile and trading mode (tenant-safe)."""

from __future__ import annotations

from collections import Counter, defaultdict
from decimal import Decimal, InvalidOperation
from datetime import UTC, datetime, timedelta
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import Response
from sqlalchemy import case, func, select
from sqlalchemy.orm import joinedload

from oziebot_common.reason_codes import normalize_reason_code
from oziebot_api.config import Settings
from oziebot_api.deps import DbSession, settings_dep
from oziebot_api.deps.auth import CurrentUser
from oziebot_api.models.risk_event import RiskEvent
from oziebot_api.models.membership import TenantMembership
from oziebot_api.models.execution import ExecutionOrder, ExecutionPosition, ExecutionTradeRecord
from oziebot_api.models.exchange_connection import ExchangeConnection
from oziebot_api.models.execution_reconciliation import ExecutionReconciliationEvent
from oziebot_api.models.market_data import MarketDataBboSnapshot
from oziebot_api.models.platform_strategy import PlatformStrategy
from oziebot_api.models.strategy_lifecycle import StrategyLifecycleEvent
from oziebot_api.models.strategy_allocation import StrategyCapitalBucket
from oziebot_api.models.strategy_signal_pipeline import StrategyRun, StrategySignalRecord
from oziebot_api.models.trade_intelligence import (
    StrategyDecisionAudit,
    StrategySignalSnapshot,
)
from oziebot_api.models.ai_diagnostics import (
    AiDiagnosticFinding,
    AiDiagnosticReview,
    DiagnosticSnapshot,
)
from oziebot_api.models.tenant import Tenant
from oziebot_api.models.user import User
from oziebot_api.models.user_strategy import UserStrategy
from oziebot_api.schemas.me import MeOut, TenantBrief, TradingModePatch
from oziebot_api.services.entitlements import tenant_strategy_entitlement_gate
from oziebot_api.services.coinbase import coinbase_valid_for_live_trading
from oziebot_api.services.entitlements import has_live_trading_billing
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
from oziebot_api.services.runtime_status import build_runtime_status_payload
from oziebot_domain.trading_mode import TradingMode

router = APIRouter(prefix="/me", tags=["me"])

DASHBOARD_CACHE_TTL_SECONDS = 120
DASHBOARD_REJECTION_CACHE_TTL_SECONDS = 300
ANALYTICS_CACHE_TTL_SECONDS = 120
EXIT_RELIABILITY_STALL_MINUTES = 15
EXIT_VISIBILITY_STAGES = {
    "exit_monitoring_started",
    "take_profit_triggered",
    "stop_loss_triggered",
    "trailing_stop_triggered",
    "exit_execution_requested",
}
ANALYTICS_SUMMARY_CACHE_TTL_SECONDS = 300
DASHBOARD_HISTORY_LOOKBACK_DAYS = 30
DASHBOARD_POSITIONS_LIMIT = 50
DASHBOARD_ACTIVE_TRADES_LIMIT = 20
DASHBOARD_RECENT_ACTIVITY_LIMIT = 20
DASHBOARD_FEE_BREAKDOWN_LIMIT = 12
DASHBOARD_REJECTION_EVENT_LIMIT = 100
DASHBOARD_REJECTION_AUDIT_SCAN_LIMIT = 250
DASHBOARD_GROWTH_POINTS = 8
DASHBOARD_GROWTH_TRADE_LIMIT = 500
DASHBOARD_ACTIVITY_SCAN_LIMIT = 500
DASHBOARD_POSITION_DUST_NOTIONAL_USD = Decimal("1")
DASHBOARD_MARK_LOOKBACK_MINUTES = 30
DASHBOARD_MARKET_DATA_STALE_SECONDS = 120
DASHBOARD_CRITICAL_FINDING_OPEN_STATUSES = ("new", "acknowledged")
PAPER_LIVE_VALIDATION_LOOKBACK_DAYS = 7
STRATEGY_WAITING_REASON_CODES = {
    "allocation_unavailable",
    "cooldown_active",
    "existing_open_position",
    "insufficient_buying_power",
    "insufficient_confidence",
    "insufficient_volume",
    "liquidity_window_closed",
    "max_exposure_reached",
    "spread_too_wide",
    "stale_market_data",
}
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
        full_name=user.full_name,
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


def _build_dashboard_growth_series(
    *,
    db: DbSession,
    user_id: Any,
    trading_mode: str,
    cutoff: datetime,
    now: datetime,
    portfolio_value: float,
    unrealized_pnl_value: float,
) -> list[float]:
    if DASHBOARD_GROWTH_POINTS <= 1:
        return [round(portfolio_value, 2)]

    trade_rows = (
        db.query(
            ExecutionTradeRecord.executed_at,
            ExecutionTradeRecord.realized_pnl_cents,
        )
        .filter(
            ExecutionTradeRecord.user_id == user_id,
            ExecutionTradeRecord.trading_mode == trading_mode,
            ExecutionTradeRecord.executed_at >= cutoff,
        )
        .order_by(ExecutionTradeRecord.executed_at.desc())
        .limit(DASHBOARD_GROWTH_TRADE_LIMIT)
        .all()
    )
    if not trade_rows:
        return [round(portfolio_value, 2)] * DASHBOARD_GROWTH_POINTS

    sorted_rows = sorted(
        trade_rows,
        key=lambda row: _as_utc(row.executed_at) or cutoff,
    )
    first_trade_at = _as_utc(sorted_rows[0].executed_at) or cutoff
    realized_total = sum(int(row.realized_pnl_cents or 0) for row in sorted_rows) / 100
    start_value = max(0.0, portfolio_value - unrealized_pnl_value - realized_total)
    series_start = max(cutoff, first_trade_at - timedelta(seconds=1))
    span_seconds = max((now - series_start).total_seconds(), 1.0)
    bucket_seconds = span_seconds / max(DASHBOARD_GROWTH_POINTS - 1, 1)
    growth: list[float] = []
    cumulative_realized = 0.0
    next_trade = 0

    for index in range(DASHBOARD_GROWTH_POINTS):
        point_time = series_start + timedelta(seconds=bucket_seconds * index)
        while next_trade < len(sorted_rows):
            trade_time = _as_utc(sorted_rows[next_trade].executed_at)
            if trade_time is None or trade_time > point_time:
                break
            cumulative_realized += int(sorted_rows[next_trade].realized_pnl_cents or 0) / 100
            next_trade += 1
        point_value = start_value + cumulative_realized
        if index == DASHBOARD_GROWTH_POINTS - 1:
            point_value += unrealized_pnl_value
            point_value = portfolio_value
        growth.append(round(max(0.0, point_value), 2))
    return growth


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


def _safe_iso(value: datetime | None) -> str | None:
    normalized = _as_utc(value)
    return normalized.isoformat() if normalized is not None else None


def _normalize_dashboard_reason_code(
    reason_code: str | None,
    *,
    reason_detail: str | None = None,
) -> str:
    return normalize_reason_code(reason_code, reason_detail=reason_detail)


def _latest_by_key(rows: list[Any], key_name: str) -> dict[str, Any]:
    latest: dict[str, Any] = {}
    for row in rows:
        key = str(getattr(row, key_name))
        if key not in latest:
            latest[key] = row
    return latest


def _latest_by_pair(rows: list[Any], first_key: str, second_key: str) -> dict[tuple[str, str], Any]:
    latest: dict[tuple[str, str], Any] = {}
    for row in rows:
        key = (str(getattr(row, first_key)), str(getattr(row, second_key)))
        if key not in latest:
            latest[key] = row
    return latest


def _latest_market_data_health(runtime_payload: dict[str, Any], *, now: datetime) -> dict[str, Any]:
    market_data = (runtime_payload.get("activity") or {}).get("market_data") or {}
    raw_last_at = market_data.get("last_at")
    last_at: datetime | None = None
    if raw_last_at:
        try:
            last_at = _as_utc(datetime.fromisoformat(str(raw_last_at).replace("Z", "+00:00")))
        except ValueError:
            last_at = None
    age_seconds = round(max((now - last_at).total_seconds(), 0), 2) if last_at is not None else None
    if age_seconds is None:
        status = "unknown"
    elif age_seconds <= DASHBOARD_MARKET_DATA_STALE_SECONDS:
        status = "fresh"
    elif age_seconds <= DASHBOARD_MARKET_DATA_STALE_SECONDS * 5:
        status = "warning"
    else:
        status = "stale"
    return {
        "status": status,
        "lastAt": _safe_iso(last_at),
        "ageSeconds": age_seconds,
        "tradeTicksRecent": int(market_data.get("trade_ticks") or 0),
        "bboUpdatesRecent": int(market_data.get("bbo_updates") or 0),
    }


def _build_reconciliation_health(
    *,
    db: DbSession,
    user: User,
    trading_mode: str,
    positions_rows: list[ExecutionPosition],
    buckets: list[StrategyCapitalBucket],
) -> dict[str, Any]:
    trade_rows = (
        db.query(ExecutionTradeRecord)
        .filter(
            ExecutionTradeRecord.user_id == user.id,
            ExecutionTradeRecord.trading_mode == trading_mode,
        )
        .order_by(ExecutionTradeRecord.executed_at.asc())
        .all()
    )
    trades_by_scope: defaultdict[tuple[str, str], list[ExecutionTradeRecord]] = defaultdict(list)
    for row in trade_rows:
        trades_by_scope[(str(row.strategy_id), str(row.symbol))].append(row)

    positions_by_scope = {(str(row.strategy_id), str(row.symbol)): row for row in positions_rows}
    mismatch_types: Counter[str] = Counter()
    mismatched_scopes: set[tuple[str, str]] = set()
    mismatched_buckets: set[str] = set()
    scopes = sorted(set(trades_by_scope) | set(positions_by_scope))
    per_bucket_expected: defaultdict[str, dict[str, int]] = defaultdict(
        lambda: {"locked_capital_cents": 0, "realized_pnl_cents": 0}
    )

    for scope in scopes:
        scoped_trades = trades_by_scope.get(scope, [])
        position = positions_by_scope.get(scope)
        expected_quantity = Decimal("0")
        expected_realized_pnl_cents = 0
        latest_trade_quantity: Decimal | None = None
        for trade in scoped_trades:
            quantity = _to_decimal(trade.quantity)
            if str(trade.side).lower() == "buy":
                expected_quantity += quantity
            else:
                expected_quantity -= quantity
                expected_realized_pnl_cents += int(trade.realized_pnl_cents or 0)
            latest_trade_quantity = _to_decimal(trade.position_quantity_after)

        actual_quantity = _to_decimal(position.quantity) if position is not None else Decimal("0")
        actual_realized_pnl_cents = (
            int(position.realized_pnl_cents or 0) if position is not None else 0
        )
        avg_entry_price = (
            _to_decimal(position.avg_entry_price) if position is not None else Decimal("0")
        )

        if abs(expected_quantity - actual_quantity) > Decimal("0.00000001"):
            mismatch_types["position_quantity_mismatch"] += 1
            mismatched_scopes.add(scope)
        if latest_trade_quantity is not None and abs(
            latest_trade_quantity - actual_quantity
        ) > Decimal("0.00000001"):
            mismatch_types["latest_trade_quantity_mismatch"] += 1
            mismatched_scopes.add(scope)
        if expected_realized_pnl_cents != actual_realized_pnl_cents:
            mismatch_types["realized_pnl_mismatch"] += 1
            mismatched_scopes.add(scope)

        expected_bucket = per_bucket_expected[str(scope[0])]
        expected_bucket["realized_pnl_cents"] += actual_realized_pnl_cents
        if actual_quantity > 0 and avg_entry_price > 0:
            expected_bucket["locked_capital_cents"] += _cents(actual_quantity * avg_entry_price)

    for bucket in buckets:
        expected = per_bucket_expected[str(bucket.strategy_id)]
        if int(bucket.locked_capital_cents or 0) != expected["locked_capital_cents"]:
            mismatch_types["bucket_locked_capital_mismatch"] += 1
            mismatched_buckets.add(str(bucket.strategy_id))
        if int(bucket.realized_pnl_cents or 0) != expected["realized_pnl_cents"]:
            mismatch_types["bucket_realized_pnl_mismatch"] += 1
            mismatched_buckets.add(str(bucket.strategy_id))

    recent_reconcile_cutoff = datetime.now(UTC) - timedelta(days=7)
    recent_events = (
        db.query(ExecutionReconciliationEvent)
        .filter(
            ExecutionReconciliationEvent.tenant_id == primary_tenant_id(db, user),
            ExecutionReconciliationEvent.trading_mode == trading_mode,
            ExecutionReconciliationEvent.created_at >= recent_reconcile_cutoff,
        )
        .order_by(ExecutionReconciliationEvent.created_at.desc())
        .limit(10)
        .all()
    )
    external_errors = sum(1 for event in recent_events if event.status == "error")
    mismatch_count = len(mismatched_scopes) + len(mismatched_buckets)
    if mismatch_count > 0 or external_errors > 0:
        status = "critical" if mismatch_count >= 3 or external_errors > 0 else "warning"
    else:
        status = "healthy"
    return {
        "status": status,
        "mismatchCount": mismatch_count,
        "scopeCount": len(scopes),
        "bucketCount": len(buckets),
        "externalErrorCount": external_errors,
        "topMismatchTypes": [
            {"type": mismatch_type, "count": int(count)}
            for mismatch_type, count in mismatch_types.most_common(4)
        ],
    }


def _build_strategy_health(
    *,
    user: User,
    db: DbSession,
    trading_mode: str,
    enabled_strategies: list[dict[str, Any]],
    buckets: list[StrategyCapitalBucket],
    positions_rows: list[ExecutionPosition],
    dashboard_cutoff: datetime,
) -> list[dict[str, Any]]:
    if not enabled_strategies:
        return []

    now = datetime.now(UTC)
    strategy_ids = [str(item["id"]) for item in enabled_strategies]
    bucket_by_strategy = {str(bucket.strategy_id): bucket for bucket in buckets}
    position_counts: Counter[str] = Counter(
        str(row.strategy_id) for row in positions_rows if _to_decimal(row.quantity) != 0
    )
    latest_trade_rows = _latest_by_key(
        (
            db.query(ExecutionTradeRecord)
            .filter(
                ExecutionTradeRecord.user_id == user.id,
                ExecutionTradeRecord.trading_mode == trading_mode,
                ExecutionTradeRecord.strategy_id.in_(strategy_ids),
                ExecutionTradeRecord.executed_at >= dashboard_cutoff,
            )
            .order_by(ExecutionTradeRecord.executed_at.desc())
            .limit(DASHBOARD_ACTIVITY_SCAN_LIMIT)
            .all()
        ),
        "strategy_id",
    )
    latest_buy_trade_rows = _latest_by_key(
        (
            db.query(ExecutionTradeRecord)
            .filter(
                ExecutionTradeRecord.user_id == user.id,
                ExecutionTradeRecord.trading_mode == trading_mode,
                ExecutionTradeRecord.strategy_id.in_(strategy_ids),
                ExecutionTradeRecord.side == "buy",
                ExecutionTradeRecord.executed_at >= dashboard_cutoff,
            )
            .order_by(ExecutionTradeRecord.executed_at.desc())
            .limit(DASHBOARD_ACTIVITY_SCAN_LIMIT)
            .all()
        ),
        "strategy_id",
    )
    latest_signal_rows = _latest_by_key(
        (
            db.query(StrategySignalRecord)
            .filter(
                StrategySignalRecord.user_id == user.id,
                StrategySignalRecord.trading_mode == trading_mode,
                StrategySignalRecord.strategy_name.in_(strategy_ids),
                StrategySignalRecord.timestamp >= dashboard_cutoff,
            )
            .order_by(StrategySignalRecord.timestamp.desc())
            .limit(DASHBOARD_ACTIVITY_SCAN_LIMIT)
            .all()
        ),
        "strategy_name",
    )
    latest_run_rows = _latest_by_key(
        (
            db.query(StrategyRun)
            .filter(
                StrategyRun.user_id == user.id,
                StrategyRun.trading_mode == trading_mode,
                StrategyRun.strategy_name.in_(strategy_ids),
                StrategyRun.started_at >= dashboard_cutoff,
            )
            .order_by(StrategyRun.started_at.desc())
            .limit(DASHBOARD_ACTIVITY_SCAN_LIMIT)
            .all()
        ),
        "strategy_name",
    )
    latest_failed_lifecycle_rows = _latest_by_key(
        (
            db.query(StrategyLifecycleEvent)
            .filter(
                StrategyLifecycleEvent.user_id == user.id,
                StrategyLifecycleEvent.trading_mode == trading_mode,
                StrategyLifecycleEvent.strategy_name.in_(strategy_ids),
                StrategyLifecycleEvent.status == "failed",
                StrategyLifecycleEvent.occurred_at >= dashboard_cutoff,
            )
            .order_by(StrategyLifecycleEvent.occurred_at.desc())
            .limit(DASHBOARD_ACTIVITY_SCAN_LIMIT)
            .all()
        ),
        "strategy_name",
    )
    latest_lifecycle_rows = _latest_by_key(
        (
            db.query(StrategyLifecycleEvent)
            .filter(
                StrategyLifecycleEvent.user_id == user.id,
                StrategyLifecycleEvent.trading_mode == trading_mode,
                StrategyLifecycleEvent.strategy_name.in_(strategy_ids),
                StrategyLifecycleEvent.occurred_at >= dashboard_cutoff,
            )
            .order_by(StrategyLifecycleEvent.occurred_at.desc())
            .limit(DASHBOARD_ACTIVITY_SCAN_LIMIT)
            .all()
        ),
        "strategy_name",
    )
    open_position_symbols = sorted({str(row.symbol) for row in positions_rows if row.symbol})
    latest_exit_lifecycle_rows = _latest_by_pair(
        (
            db.query(StrategyLifecycleEvent)
            .filter(
                StrategyLifecycleEvent.user_id == user.id,
                StrategyLifecycleEvent.trading_mode == trading_mode,
                StrategyLifecycleEvent.stage.in_(EXIT_VISIBILITY_STAGES),
                StrategyLifecycleEvent.strategy_name.in_(strategy_ids),
                StrategyLifecycleEvent.symbol.in_(open_position_symbols),
            )
            .order_by(StrategyLifecycleEvent.occurred_at.desc())
            .limit(DASHBOARD_ACTIVITY_SCAN_LIMIT)
            .all()
        )
        if open_position_symbols
        else [],
        "strategy_name",
        "symbol",
    )
    strategy_exit_rollups: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "positions": 0,
            "stalled": 0,
            "latest_event": None,
        }
    )
    exit_stall_cutoff = now - timedelta(minutes=EXIT_RELIABILITY_STALL_MINUTES)
    for row in positions_rows:
        exit_event = latest_exit_lifecycle_rows.get((str(row.strategy_id), str(row.symbol)))
        if exit_event is None:
            continue
        rollup = strategy_exit_rollups[str(row.strategy_id)]
        rollup["positions"] += 1
        occurred_at = _as_utc(exit_event.occurred_at)
        if occurred_at is not None and occurred_at <= exit_stall_cutoff:
            rollup["stalled"] += 1
        latest_event = rollup.get("latest_event")
        latest_event_at = _as_utc(getattr(latest_event, "occurred_at", None))
        if latest_event is None or (
            occurred_at is not None and (latest_event_at is None or occurred_at >= latest_event_at)
        ):
            rollup["latest_event"] = exit_event

    summaries: list[dict[str, Any]] = []
    for strategy in enabled_strategies:
        strategy_id = str(strategy["id"])
        strategy_config = strategy.get("config") or {}
        strategy_params = (
            strategy_config.get("strategy_params") if isinstance(strategy_config, dict) else {}
        )
        if not isinstance(strategy_params, dict):
            strategy_params = {}
        bucket = bucket_by_strategy.get(strategy_id)
        last_trade = latest_trade_rows.get(strategy_id)
        last_buy = latest_buy_trade_rows.get(strategy_id)
        last_signal = latest_signal_rows.get(strategy_id)
        last_run = latest_run_rows.get(strategy_id)
        last_failed_event = latest_failed_lifecycle_rows.get(strategy_id)
        latest_lifecycle = latest_lifecycle_rows.get(strategy_id)
        open_positions = int(position_counts.get(strategy_id, 0))
        exit_rollup = strategy_exit_rollups.get(strategy_id) or {}
        latest_exit_event = exit_rollup.get("latest_event")
        latest_exit_at = _as_utc(getattr(latest_exit_event, "occurred_at", None))
        latest_exit_reason_code = (
            _normalize_dashboard_reason_code(
                getattr(latest_exit_event, "reason_code", None),
                reason_detail=getattr(latest_exit_event, "reason_detail", None),
            )
            if latest_exit_event is not None
            else None
        )
        latest_exit_reason_detail = (
            getattr(latest_exit_event, "reason_detail", None)
            or getattr(latest_exit_event, "reason_code", None)
            if latest_exit_event is not None
            else None
        )
        exit_monitored_positions = int(exit_rollup.get("positions") or 0)
        stalled_exit_count = int(exit_rollup.get("stalled") or 0)
        dca_interval_hours = (
            int(
                strategy_params.get("buy_interval_hours")
                or (
                    strategy_config.get("buy_interval_hours")
                    if isinstance(strategy_config, dict)
                    else 0
                )
                or 24
            )
            if strategy_id == "dca"
            else None
        )

        blocking_reason_code: str | None = None
        blocking_reason_detail: str | None = None
        next_eligible_at: str | None = None
        blocker_observed_at: datetime | None = None

        run_metadata = (last_run.run_metadata or {}) if last_run is not None else {}
        if last_run is not None and run_metadata.get("suppressed"):
            blocking_reason_code = _normalize_dashboard_reason_code(
                str(run_metadata.get("suppression_reason") or "suppressed"),
                reason_detail=str(run_metadata.get("suppression_reason") or ""),
            )
            blocking_reason_detail = str(
                run_metadata.get("suppression_reason") or "Strategy suppressed"
            )
            blocker_observed_at = _as_utc(last_run.started_at)
            next_eligible_at = (
                str(run_metadata.get("next_eligible_buy_time"))
                if run_metadata.get("next_eligible_buy_time")
                else None
            )

        if last_failed_event is not None:
            failed_at = _as_utc(last_failed_event.occurred_at)
            if blocker_observed_at is None or (
                failed_at is not None and failed_at >= blocker_observed_at
            ):
                blocking_reason_code = _normalize_dashboard_reason_code(
                    last_failed_event.reason_code,
                    reason_detail=last_failed_event.reason_detail,
                )
                blocking_reason_detail = (
                    last_failed_event.reason_detail or last_failed_event.reason_code
                )
                blocker_observed_at = failed_at
                event_metadata = last_failed_event.event_metadata or {}
                next_eligible_at = str(
                    event_metadata.get("next_eligible_buy_time") or next_eligible_at
                )

        if (
            strategy_id == "dca"
            and next_eligible_at is None
            and dca_interval_hours
            and last_buy is not None
        ):
            last_buy_at = _as_utc(last_buy.executed_at)
            if last_buy_at is not None:
                computed_next_eligible = last_buy_at + timedelta(hours=dca_interval_hours)
                if computed_next_eligible > datetime.now(UTC):
                    next_eligible_at = computed_next_eligible.isoformat()

        if (
            blocking_reason_code is None
            and last_signal is not None
            and str(last_signal.action).lower() == "hold"
        ):
            signal_reason = str(
                (last_signal.reasoning_metadata or {}).get("reason") or "Waiting for setup"
            )
            signal_reason_code = str(
                (last_signal.reasoning_metadata or {}).get("reason_code") or "hold_signal"
            )
            blocking_reason_code = _normalize_dashboard_reason_code(
                signal_reason_code,
                reason_detail=signal_reason,
            )
            blocking_reason_detail = signal_reason

        if not bool(strategy.get("enabled")):
            current_status = "disabled"
            blocking_reason_code = "strategy_disabled"
            blocking_reason_detail = "Strategy is turned off."
        elif open_positions > 0:
            if exit_monitored_positions > 0 or (
                latest_lifecycle is not None
                and str(latest_lifecycle.stage) in EXIT_VISIBILITY_STAGES
            ):
                current_status = "exit_monitoring"
                if stalled_exit_count > 0:
                    blocking_reason_code = "exit_stalled"
                    blocking_reason_detail = (
                        f"{stalled_exit_count} exit request"
                        f"{'s look' if stalled_exit_count > 1 else ' looks'} stale and should be reviewed."
                    )
                elif blocking_reason_code is None and latest_exit_reason_code is not None:
                    blocking_reason_code = latest_exit_reason_code
                    blocking_reason_detail = latest_exit_reason_detail
            else:
                current_status = "managing_position"
        elif blocking_reason_code in STRATEGY_WAITING_REASON_CODES:
            current_status = "waiting"
        elif blocking_reason_code is not None:
            current_status = "blocked"
        elif last_run is not None or last_signal is not None:
            current_status = "ready"
        else:
            current_status = "inactive"

        summaries.append(
            {
                "id": strategy_id,
                "name": strategy["name"],
                "enabled": bool(strategy.get("enabled")),
                "allocationPct": float(strategy.get("allocationPct") or 0),
                "assignedCapital": round(int(bucket.assigned_capital_cents or 0) / 100, 2)
                if bucket
                else 0.0,
                "availableCash": round(int(bucket.available_cash_cents or 0) / 100, 2)
                if bucket
                else 0.0,
                "deployedCapital": (
                    round(
                        (
                            int(bucket.reserved_cash_cents or 0)
                            + int(bucket.locked_capital_cents or 0)
                        )
                        / 100,
                        2,
                    )
                    if bucket
                    else 0.0
                ),
                "utilizationPct": (
                    round(
                        (
                            (
                                int(bucket.reserved_cash_cents or 0)
                                + int(bucket.locked_capital_cents or 0)
                            )
                            / max(int(bucket.assigned_capital_cents or 0), 1)
                        )
                        * 100,
                        2,
                    )
                    if bucket and int(bucket.assigned_capital_cents or 0) > 0
                    else 0.0
                ),
                "realizedPnl": round(int(bucket.realized_pnl_cents or 0) / 100, 2)
                if bucket
                else 0.0,
                "unrealizedPnl": round(int(bucket.unrealized_pnl_cents or 0) / 100, 2)
                if bucket
                else 0.0,
                "currentStatus": current_status,
                "openPositions": open_positions,
                "lastEvaluatedAt": _safe_iso(last_run.started_at if last_run is not None else None),
                "lastSignalAt": _safe_iso(
                    last_signal.timestamp if last_signal is not None else None
                ),
                "lastSignalAction": (
                    str(last_signal.action).lower() if last_signal is not None else None
                ),
                "lastSignalReason": (
                    str((last_signal.reasoning_metadata or {}).get("reason"))
                    if last_signal is not None
                    else None
                ),
                "lastTradeAt": _safe_iso(
                    last_trade.executed_at if last_trade is not None else None
                ),
                "lastBuyAt": _safe_iso(last_buy.executed_at if last_buy is not None else None),
                "dcaIntervalHours": dca_interval_hours,
                "exitMonitoredPositions": exit_monitored_positions,
                "stalledExitCount": stalled_exit_count,
                "latestExitAt": _safe_iso(latest_exit_at),
                "latestExitReasonCode": latest_exit_reason_code,
                "latestExitReasonDetail": latest_exit_reason_detail,
                "blockingReasonCode": blocking_reason_code,
                "blockingReasonDetail": blocking_reason_detail,
                "nextEligibleAt": next_eligible_at,
                "latestLifecycleStage": (
                    str(latest_lifecycle.stage) if latest_lifecycle is not None else None
                ),
                "latestLifecycleStatus": (
                    str(latest_lifecycle.status) if latest_lifecycle is not None else None
                ),
            }
        )

    status_order = {
        "blocked": 0,
        "waiting": 1,
        "exit_monitoring": 2,
        "managing_position": 3,
        "ready": 4,
        "inactive": 5,
        "disabled": 6,
    }
    return sorted(
        summaries,
        key=lambda item: (
            status_order.get(str(item["currentStatus"]), 99),
            -float(item["allocationPct"]),
            str(item["name"]),
        ),
    )


def _build_bot_health(
    *,
    user: User,
    db: DbSession,
    tenant_id: Any,
    trading_mode: str,
    enabled_strategies: list[dict[str, Any]],
    positions: list[dict[str, Any]],
    active_trades: list[dict[str, Any]],
    recent_activity: list[dict[str, Any]],
    strategy_health: list[dict[str, Any]],
    reconciliation_health: dict[str, Any],
) -> dict[str, Any]:
    now = datetime.now(UTC)
    runtime_payload = build_runtime_status_payload(db)
    market_data_health = _latest_market_data_health(runtime_payload, now=now)
    critical_diagnostics_count = 0
    if tenant_id is not None:
        latest_review_id = db.scalar(
            select(AiDiagnosticReview.id)
            .join(DiagnosticSnapshot, DiagnosticSnapshot.id == AiDiagnosticReview.snapshot_id)
            .where(AiDiagnosticReview.tenant_id == tenant_id)
            .where(DiagnosticSnapshot.trading_mode == trading_mode)
            .order_by(
                DiagnosticSnapshot.generated_at.desc(),
                AiDiagnosticReview.created_at.desc(),
            )
            .limit(1)
        )
        if latest_review_id is not None:
            critical_diagnostics_count = int(
                db.scalar(
                    select(func.count(AiDiagnosticFinding.id))
                    .where(AiDiagnosticFinding.review_id == latest_review_id)
                    .where(AiDiagnosticFinding.severity == "critical")
                    .where(AiDiagnosticFinding.status.in_(DASHBOARD_CRITICAL_FINDING_OPEN_STATUSES))
                )
                or 0
            )

    paper_live = _build_paper_live_health(
        user=user,
        db=db,
        tenant_id=tenant_id,
        trading_mode=trading_mode,
        enabled_strategies=enabled_strategies,
        strategy_health=strategy_health,
        reconciliation_health=reconciliation_health,
        critical_diagnostics_count=critical_diagnostics_count,
        market_data_health=market_data_health,
    )

    if positions:
        quiet_reason_code = "managing_open_positions"
        quiet_reason = f"Bot is actively managing {len(positions)} open position(s)."
    elif active_trades:
        quiet_reason_code = "orders_in_flight"
        quiet_reason = f"{len(active_trades)} order(s) are still working through execution."
    elif recent_activity:
        quiet_reason_code = "recent_trade_completed"
        quiet_reason = (
            "Recent trade activity exists; strategies may be waiting for the next valid setup."
        )
    else:
        blocker = next(
            (
                item
                for item in strategy_health
                if item.get("enabled")
                and item.get("blockingReasonCode")
                and item.get("currentStatus") in {"blocked", "waiting"}
            ),
            None,
        )
        if blocker is not None:
            quiet_reason_code = str(blocker.get("blockingReasonCode"))
            quiet_reason = str(
                blocker.get("blockingReasonDetail")
                or blocker.get("blockingReasonCode")
                or "Strategies are waiting for eligibility."
            )
        elif any(item.get("enabled") for item in strategy_health):
            quiet_reason_code = "waiting_for_valid_setups"
            quiet_reason = "Strategies are evaluating, but no setup has recently passed policy, risk, and execution gates."
        else:
            quiet_reason_code = "no_strategies_enabled"
            quiet_reason = "No strategies are enabled in this trading mode."

    overall_status = str(runtime_payload.get("overall_status") or "unknown")
    if reconciliation_health.get("status") == "critical" or critical_diagnostics_count > 0:
        overall_status = "critical"
    elif overall_status not in {"critical"} and (
        reconciliation_health.get("status") == "warning"
        or market_data_health.get("status") in {"warning", "stale"}
    ):
        overall_status = "warning"
    elif overall_status == "unknown" and quiet_reason_code not in {"no_strategies_enabled"}:
        overall_status = "warning"

    last_successful_trade_at = recent_activity[0]["timestamp"] if recent_activity else None
    return {
        "overallStatus": overall_status,
        "runtimeStatus": str(runtime_payload.get("overall_status") or "unknown"),
        "pipelineStatus": str(runtime_payload.get("pipeline_status") or "unknown"),
        "mode": trading_mode,
        "activeStrategies": sum(1 for item in enabled_strategies if item.get("enabled")),
        "activePositions": len(positions),
        "criticalDiagnosticsCount": critical_diagnostics_count,
        "lastSuccessfulTradeAt": last_successful_trade_at,
        "quietReasonCode": quiet_reason_code,
        "quietReason": quiet_reason,
        "marketData": market_data_health,
        "reconciliation": reconciliation_health,
        "paperLive": paper_live,
    }


def _build_paper_live_health(
    *,
    user: User,
    db: DbSession,
    tenant_id: Any,
    trading_mode: str,
    enabled_strategies: list[dict[str, Any]],
    strategy_health: list[dict[str, Any]],
    reconciliation_health: dict[str, Any],
    critical_diagnostics_count: int,
    market_data_health: dict[str, Any],
) -> dict[str, Any]:
    now = datetime.now(UTC)
    live_mode = TradingMode.LIVE
    live_ready = False
    live_reason: str | None = "Tenant context required for live readiness"
    has_billing = False
    coinbase_ready = False
    connection_status = "not_connected"
    connection_detail: str | None = "No validated Coinbase connection found."

    if tenant_id is not None:
        live_ready, live_reason = can_set_trading_mode(db, tenant_id=tenant_id, new_mode=live_mode)
        has_billing = has_live_trading_billing(db, tenant_id)
        coinbase_ready = coinbase_valid_for_live_trading(db, tenant_id)
        connection = db.scalars(
            select(ExchangeConnection)
            .where(
                ExchangeConnection.tenant_id == tenant_id,
                ExchangeConnection.provider == "coinbase",
            )
            .limit(1)
        ).first()
        if connection is not None:
            connection_status = str(connection.health_status or connection.validation_status)
            connection_detail = connection.last_error or (
                "Validated Coinbase connection is ready for balances and trading."
                if coinbase_ready
                else "Coinbase connection exists but is not fully validated for live trading."
            )

    validation_cutoff = now - timedelta(days=PAPER_LIVE_VALIDATION_LOOKBACK_DAYS)
    validation_failure_row = db.execute(
        select(
            func.count(ExecutionOrder.id).label("count"),
            func.max(ExecutionOrder.created_at).label("last_at"),
        ).where(
            ExecutionOrder.user_id == user.id,
            ExecutionOrder.trading_mode == trading_mode,
            ExecutionOrder.failure_code == "execution_validation_failed",
            ExecutionOrder.created_at >= validation_cutoff,
        )
    ).one()
    validation_failures = int(validation_failure_row.count or 0)
    validation_last_at = _as_utc(validation_failure_row.last_at)
    lifecycle_healthy = not any(
        bool(item.get("enabled"))
        and (
            int(item.get("stalledExitCount") or 0) > 0
            or (
                str(item.get("currentStatus")) == "blocked"
                and str(item.get("blockingReasonCode") or "") not in STRATEGY_WAITING_REASON_CODES
            )
        )
        for item in strategy_health
    )
    exposure_configured = any(
        float(item.get("assignedCapital") or 0) > 0 for item in strategy_health
    )

    checklist = [
        {
            "id": "reconciliation",
            "label": "Reconciliation healthy",
            "passed": reconciliation_health.get("status") == "healthy",
            "detail": (
                "Execution, position, and capital buckets reconcile cleanly."
                if reconciliation_health.get("status") == "healthy"
                else f"{int(reconciliation_health.get('mismatchCount') or 0)} mismatch(es) need review."
            ),
        },
        {
            "id": "diagnostics",
            "label": "No critical diagnostics",
            "passed": critical_diagnostics_count == 0,
            "detail": (
                "No open critical diagnostic findings."
                if critical_diagnostics_count == 0
                else f"{critical_diagnostics_count} critical diagnostic finding(s) remain open."
            ),
        },
        {
            "id": "execution_validation",
            "label": f"No recent execution validation failures ({PAPER_LIVE_VALIDATION_LOOKBACK_DAYS}d)",
            "passed": validation_failures == 0,
            "detail": (
                (
                    "No zero-value or precision validation failures were recorded in the "
                    f"last {PAPER_LIVE_VALIDATION_LOOKBACK_DAYS} days."
                )
                if validation_failures == 0
                else (
                    f"{validation_failures} validation failure(s) were recorded in the last "
                    f"{PAPER_LIVE_VALIDATION_LOOKBACK_DAYS} days"
                    + (
                        f"; last seen {validation_last_at.isoformat()}."
                        if validation_last_at is not None
                        else "."
                    )
                )
            ),
        },
        {
            "id": "strategy_lifecycle",
            "label": "Strategy lifecycle healthy",
            "passed": lifecycle_healthy,
            "detail": (
                "No blocked strategies or stalled exits are currently visible."
                if lifecycle_healthy
                else "One or more strategies are blocked or have stalled exits."
            ),
        },
        {
            "id": "market_data",
            "label": "Market data fresh",
            "passed": market_data_health.get("status") == "fresh",
            "detail": (
                "Recent BBO and trade activity look fresh."
                if market_data_health.get("status") == "fresh"
                else "Market data freshness needs attention before trusting live readiness."
            ),
        },
        {
            "id": "coinbase",
            "label": "Valid Coinbase credentials",
            "passed": coinbase_ready,
            "detail": connection_detail,
        },
        {
            "id": "exposure",
            "label": "Exposure configured",
            "passed": exposure_configured,
            "detail": (
                "At least one strategy has assigned capital."
                if exposure_configured
                else "No assigned capital is configured yet."
            ),
        },
        {
            "id": "billing",
            "label": "Live billing active",
            "passed": has_billing,
            "detail": (
                "Trial or subscription covers live trading."
                if has_billing
                else "Live mode still needs an active trial or subscription."
            ),
        },
    ]

    return {
        "currentMode": trading_mode,
        "canSwitchToLive": live_ready,
        "liveReadinessReason": live_reason,
        "strictPaperModeAvailable": False,
        "paperWarning": "Paper results may not fully represent live trading conditions.",
        "connectionStatus": connection_status,
        "checklist": checklist,
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


def _latest_symbol_marks(db: DbSession, symbols: list[str]) -> dict[str, Decimal]:
    normalized_symbols = sorted({str(symbol or "").upper() for symbol in symbols if symbol})
    if not normalized_symbols:
        return {}

    recent_cutoff = datetime.now(UTC) - timedelta(minutes=DASHBOARD_MARK_LOOKBACK_MINUTES)
    # Latest row per product: global ORDER BY + LIMIT scanned the whole hot window for busy pairs.
    rn = (
        func.row_number()
        .over(
            partition_by=MarketDataBboSnapshot.product_id,
            order_by=MarketDataBboSnapshot.event_time.desc(),
        )
        .label("rn")
    )
    ranked = (
        select(
            MarketDataBboSnapshot.product_id.label("pid"),
            MarketDataBboSnapshot.best_bid_price.label("bid"),
            MarketDataBboSnapshot.best_ask_price.label("ask"),
            rn,
        )
        .where(
            MarketDataBboSnapshot.product_id.in_(normalized_symbols),
            MarketDataBboSnapshot.event_time >= recent_cutoff,
        )
        .subquery("ranked_bbo")
    )
    stmt = select(ranked.c.pid, ranked.c.bid, ranked.c.ask).where(ranked.c.rn == 1)
    rows = db.execute(stmt).all()
    marks: dict[str, Decimal] = {}
    for product_id, bid_price, ask_price in rows:
        key = str(product_id).upper()
        bid = _to_decimal(str(bid_price))
        ask = _to_decimal(str(ask_price))
        if bid > 0 and ask > 0:
            marks[key] = (bid + ask) / Decimal("2")
        elif bid > 0:
            marks[key] = bid
        elif ask > 0:
            marks[key] = ask
    return marks


def _paper_unrealized_pnl_cents(
    db: DbSession,
    positions: list[ExecutionPosition],
) -> int:
    marks = _latest_symbol_marks(db, [row.symbol for row in positions])
    total_unrealized = Decimal("0")
    for row in positions:
        quantity = _to_decimal(row.quantity)
        if quantity == 0:
            continue
        entry_price = _to_decimal(row.avg_entry_price)
        mark = marks.get((row.symbol or "").upper(), entry_price)
        total_unrealized += (mark - entry_price) * quantity
    return _cents(total_unrealized)


def _paper_positions(db: DbSession, user_id: Any, trading_mode: str) -> list[ExecutionPosition]:
    return (
        db.query(ExecutionPosition)
        .filter(
            ExecutionPosition.user_id == user_id,
            ExecutionPosition.trading_mode == trading_mode,
        )
        .order_by(ExecutionPosition.updated_at.desc())
        .all()
    )


def _paper_balance_snapshot(
    db: DbSession,
    *,
    user_id: Any,
    trading_mode: str,
    buckets: list[StrategyCapitalBucket],
    positions: list[ExecutionPosition] | None = None,
) -> tuple[int, int, int]:
    positions_rows = (
        positions if positions is not None else _paper_positions(db, user_id, trading_mode)
    )
    available_balance_cents = sum(max(0, b.available_cash_cents) for b in buckets)
    total_reserved_cents = sum(max(0, b.reserved_cash_cents) for b in buckets)
    total_locked_cents = sum(max(0, b.locked_capital_cents) for b in buckets)
    unrealized_pnl_cents = _paper_unrealized_pnl_cents(db, positions_rows)
    portfolio_cents = (
        available_balance_cents + total_reserved_cents + total_locked_cents + unrealized_pnl_cents
    )
    return available_balance_cents, portfolio_cents, unrealized_pnl_cents


def _recent_strategy_rejection_records(
    *,
    user: User,
    db: DbSession,
    trading_mode: str,
    cutoff: datetime,
    limit: int,
) -> tuple[list[dict[str, Any]], bool]:
    # Join snapshots so we filter audits by tenant user + mode in the DB. A global audit
    # scan (missing user predicates) degenerates into full-table reads and gateway timeouts.
    rows = (
        db.query(
            StrategyDecisionAudit,
            StrategySignalSnapshot.strategy_name,
            StrategySignalSnapshot.token_symbol,
        )
        .join(
            StrategySignalSnapshot,
            StrategyDecisionAudit.signal_snapshot_id == StrategySignalSnapshot.id,
        )
        .filter(
            StrategySignalSnapshot.user_id == user.id,
            StrategySignalSnapshot.trading_mode == trading_mode,
            StrategyDecisionAudit.decision == "rejected",
            StrategyDecisionAudit.stage.in_(("strategy", "suppression")),
            StrategyDecisionAudit.created_at >= cutoff,
        )
        .order_by(StrategyDecisionAudit.created_at.desc())
        .limit(DASHBOARD_REJECTION_AUDIT_SCAN_LIMIT)
        .all()
    )

    records: list[dict[str, Any]] = []
    for audit, strategy_name, token_symbol in rows:
        records.append(
            _format_rejection_record(
                stage=str(audit.stage),
                reason_code=audit.reason_code,
                reason_detail=audit.reason_detail,
                strategy=str(strategy_name or ""),
                symbol=str(token_symbol or ""),
                created_at=audit.created_at,
            )
        )
        if len(records) >= limit:
            break

    capped = len(rows) >= DASHBOARD_REJECTION_AUDIT_SCAN_LIMIT or len(records) >= limit
    return records, capped


def _live_coinbase_balance_snapshot(
    db: DbSession,
    user: User,
    settings: Settings,
    positions_rows: list[ExecutionPosition],
) -> tuple[int, int] | None:
    accounts = load_live_coinbase_accounts(db, user=user, settings=settings)
    if accounts is None:
        return None

    symbol_marks = _latest_symbol_marks(db, [row.symbol for row in positions_rows])
    mark_prices: dict[str, Decimal] = {}
    for row in positions_rows:
        symbol = (row.symbol or "").upper()
        if "-" not in symbol:
            continue
        base_currency = symbol.split("-", 1)[0]
        if base_currency and base_currency not in mark_prices:
            mark_prices[base_currency] = symbol_marks.get(symbol) or _to_decimal(
                row.avg_entry_price
            )

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
    return {"user_id": str(user.id), "trading_mode": trading_mode, "version": 5}


def _dashboard_summary_cache_params(*, user: User, trading_mode: str) -> dict[str, Any]:
    return {"user_id": str(user.id), "trading_mode": trading_mode, "version": 6}


def _dashboard_details_cache_params(*, user: User, trading_mode: str) -> dict[str, Any]:
    return {"user_id": str(user.id), "trading_mode": trading_mode, "version": 5}


def _dashboard_summary_payload(payload: dict[str, Any]) -> dict[str, Any]:
    fee_analytics = payload.get("feeAnalytics") or {}
    rejection_diagnostics = payload.get("rejectionDiagnostics") or {}
    return {
        "availableBalance": payload.get("availableBalance", 0),
        "portfolioValue": payload.get("portfolioValue", 0),
        "pnlValue": payload.get("pnlValue", 0),
        "realizedPnlValue": payload.get("realizedPnlValue", 0),
        "unrealizedPnlValue": payload.get("unrealizedPnlValue", 0),
        "pnlPercent": payload.get("pnlPercent", 0),
        "gainLossLabel": payload.get("gainLossLabel", "Total P&L"),
        "growth": payload.get("growth") or [],
        "positionsCount": len(payload.get("positions") or []),
        "activeTradesCount": len(payload.get("activeTrades") or []),
        "recentActivityCount": len(payload.get("recentActivity") or []),
        "totalFeesMonth": fee_analytics.get("totalFeesMonth", 0),
        "avgNetEdgeAtEntryBps": fee_analytics.get("avgNetEdgeAtEntryBps", 0),
        "totalRejected": rejection_diagnostics.get("totalRejected", 0),
        "budget": payload.get("budget") or {},
    }


def _dashboard_rejection_payload(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "windowHours": payload.get("windowHours", 24),
        "budget": payload.get("budget") or {},
        "rejectionDiagnostics": payload.get("rejectionDiagnostics")
        or {"totalRejected": 0, "byStage": [], "breakdown": [], "recent": []},
        "skippedTradesDueToFees": payload.get("skippedTradesDueToFees", 0),
    }


def _dashboard_details_payload(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "enabledStrategies": payload.get("enabledStrategies") or [],
        "strategyHealth": payload.get("strategyHealth") or [],
        "botHealth": payload.get("botHealth") or {},
        "positions": payload.get("positions") or [],
        "activeTrades": payload.get("activeTrades") or [],
        "recentActivity": payload.get("recentActivity") or [],
        "capitalUtilization": payload.get("capitalUtilization") or {},
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
        "version": 5,
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


def _analytics_outcomes_payload(
    service: TradeReviewAnalyticsService, filters: AnalyticsFilters
) -> dict[str, Any]:
    return {
        "filters": service.filters_payload(filters),
        "budget": service.budget_payload(),
        "rows": service.build_outcome_rows(filters),
    }


def _analytics_validation_payload(
    service: TradeReviewAnalyticsService, filters: AnalyticsFilters
) -> dict[str, Any]:
    return {
        "filters": service.filters_payload(filters),
        "budget": service.budget_payload(),
        "paperLiveValidation": service.build_paper_live_validation(filters),
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
    cache = ReadModelCache(settings, db.get_bind())
    return cache.get_or_build(
        namespace="dashboard-v2",
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
    cache = ReadModelCache(settings, db.get_bind())
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
    cache = ReadModelCache(settings, db.get_bind())
    return cache.get_or_build(
        namespace="dashboard-details-v2",
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
            include_rejection_diagnostics=False,
        ),
    )


def _cached_dashboard_rejection_payload(
    *,
    user: User,
    db: DbSession,
    settings: Settings,
    trading_mode: TradingMode | None,
    window_hours: int,
    force_refresh: bool = False,
) -> dict[str, Any]:
    mode = _dashboard_mode(user, trading_mode)
    cache = ReadModelCache(settings, db.get_bind())
    return cache.get_or_build(
        namespace="dashboard-rejections-v1",
        identity=str(user.id),
        params={
            "user_id": str(user.id),
            "trading_mode": mode,
            "window_hours": window_hours,
            "version": 3,
        },
        ttl_seconds=DASHBOARD_REJECTION_CACHE_TTL_SECONDS,
        force_refresh=force_refresh,
        builder=lambda: _build_dashboard_rejection_history_payload(
            user=user,
            db=db,
            trading_mode=trading_mode,
            window_hours=window_hours,
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
    cache = ReadModelCache(settings, db.get_bind())
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
    ttl_seconds: int | None = None,
) -> dict[str, Any]:
    filters, window_meta = _analytics_filters(
        user=user,
        trading_mode=trading_mode,
        strategy_name=strategy_name,
        symbol=symbol,
        start_at=start_at,
        end_at=end_at,
    )
    cache_ttl = ANALYTICS_CACHE_TTL_SECONDS if ttl_seconds is None else ttl_seconds
    cache = ReadModelCache(settings, db.get_bind())
    payload = cache.get_or_build(
        namespace=namespace,
        identity=str(user.id),
        params=_analytics_cache_params(filters),
        ttl_seconds=cache_ttl,
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
    if mode == TradingMode.PAPER.value:
        available_balance_cents, portfolio_cents, unrealized_pnl_cents = _paper_balance_snapshot(
            db,
            user_id=user.id,
            trading_mode=mode,
            buckets=buckets,
        )
    else:
        available_balance_cents = sum(max(0, b.available_cash_cents) for b in buckets)
        portfolio_cents = sum(
            b.available_cash_cents
            + b.reserved_cash_cents
            + b.locked_capital_cents
            + b.unrealized_pnl_cents
            for b in buckets
        )
        unrealized_pnl_cents = sum(b.unrealized_pnl_cents for b in buckets)
    realized_pnl_cents = sum(b.realized_pnl_cents for b in buckets)
    pnl_cents = realized_pnl_cents + unrealized_pnl_cents
    if mode == TradingMode.PAPER.value:
        pnl_cents = realized_pnl_cents + unrealized_pnl_cents
    portfolio_value = portfolio_cents / 100
    pnl_value = pnl_cents / 100
    base = max(1.0, portfolio_value - pnl_value)
    pnl_percent = (pnl_value / base) * 100
    now = datetime.now(UTC)
    dashboard_cutoff = now - timedelta(days=DASHBOARD_HISTORY_LOOKBACK_DAYS)
    growth = _build_dashboard_growth_series(
        db=db,
        user_id=user.id,
        trading_mode=mode,
        cutoff=dashboard_cutoff,
        now=now,
        portfolio_value=portfolio_value,
        unrealized_pnl_value=unrealized_pnl_cents / 100,
    )
    return {
        "availableBalance": round(available_balance_cents / 100, 2),
        "portfolioValue": round(portfolio_value, 2),
        "pnlValue": round(pnl_value, 2),
        "realizedPnlValue": round(realized_pnl_cents / 100, 2),
        "unrealizedPnlValue": round(unrealized_pnl_cents / 100, 2),
        "pnlPercent": round(pnl_percent, 4),
        "gainLossLabel": "Total P&L",
        "growth": growth,
        "positions": [],
        "activeTrades": [],
        "recentActivity": [],
        "feeAnalytics": {
            "totalFeesMonth": 0,
            "avgNetEdgeAtEntryBps": 0,
        },
        "rejectionDiagnostics": {
            "totalRejected": 0,
        },
        "budget": {
            "historyLookbackDaysApplied": DASHBOARD_HISTORY_LOOKBACK_DAYS,
            "summaryOnly": True,
            "deferredMetrics": [
                "positionsCount",
                "activeTradesCount",
                "recentActivityCount",
                "totalFeesMonth",
                "avgNetEdgeAtEntryBps",
            ],
            "historyStartAt": dashboard_cutoff.isoformat(),
            "historyEndAt": now.isoformat(),
        },
    }


def _build_dashboard_rejection_history_payload(
    user: CurrentUser,
    db: DbSession,
    trading_mode: TradingMode | None = None,
    *,
    window_hours: int,
) -> dict[str, Any]:
    mode = _dashboard_mode(user, trading_mode)
    now = datetime.now(UTC)
    cutoff = now - timedelta(hours=window_hours)

    strategy_records, strategy_capped = _recent_strategy_rejection_records(
        user=user,
        db=db,
        trading_mode=mode,
        cutoff=cutoff,
        limit=DASHBOARD_REJECTION_EVENT_LIMIT,
    )
    recent_risk_rejects = (
        db.query(RiskEvent)
        .filter(
            RiskEvent.user_id == user.id,
            RiskEvent.trading_mode == mode,
            RiskEvent.outcome == "reject",
            RiskEvent.created_at >= cutoff,
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
            >= cutoff,
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
    rejection_diagnostics = _build_rejection_diagnostics(
        strategy_records=strategy_records,
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
    skipped_trades_due_to_fees = sum(
        1 for event in recent_risk_rejects if "fee_economics" in str(event.detail or "").lower()
    )
    return {
        "windowHours": window_hours,
        "budget": {
            "windowHours": window_hours,
            "eventLimit": DASHBOARD_REJECTION_EVENT_LIMIT,
            "startAt": cutoff.isoformat(),
            "endAt": now.isoformat(),
            "auditScanLimit": DASHBOARD_REJECTION_AUDIT_SCAN_LIMIT,
            "capped": strategy_capped
            or any(
                len(items) >= DASHBOARD_REJECTION_EVENT_LIMIT
                for items in (recent_risk_rejects, recent_failed_orders)
            ),
        },
        "rejectionDiagnostics": rejection_diagnostics,
        "skippedTradesDueToFees": skipped_trades_due_to_fees,
    }


def _build_dashboard_payload(
    user: CurrentUser,
    db: DbSession,
    settings: Settings,
    trading_mode: TradingMode | None = None,
    *,
    use_live_balances: bool = True,
    include_rejection_diagnostics: bool = True,
) -> dict[str, Any]:
    mode = _dashboard_mode(user, trading_mode)
    tenant_id = primary_tenant_id(db, user)
    uses_tenant_scope = tenant_id is not None
    now = datetime.now(UTC)

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

    if uses_tenant_scope and tenant_id is not None:
        global_entitled, entitled_strategy_ids = tenant_strategy_entitlement_gate(db, tenant_id)
    else:
        global_entitled = False
        entitled_strategy_ids = set()

    enabled_strategies: list[dict[str, Any]] = []
    for row in platform_rows:
        assigned = (
            bool(global_entitled or row.id in entitled_strategy_ids)
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
                "config": (
                    configured_row.config
                    if configured_row is not None and isinstance(configured_row.config, dict)
                    else row.config_schema or {}
                ),
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
    known_strategy_ids = {str(item["id"]) for item in enabled_strategies}
    for strategy_id in sorted(bucket_by_strategy):
        if strategy_id in known_strategy_ids:
            continue
        enabled_strategies.append(
            {
                "id": strategy_id,
                "name": _format_strategy_name(strategy_id),
                "enabled": True,
                "allocationPct": 0,
                "config": {},
            }
        )
        known_strategy_ids.add(strategy_id)
    total_assigned = sum(max(0, b.assigned_capital_cents) for b in buckets)
    for item in enabled_strategies:
        b = bucket_by_strategy.get(item["id"])
        assigned = b.assigned_capital_cents if b else 0
        item["allocationPct"] = (
            int(round((assigned / total_assigned) * 100)) if total_assigned > 0 else 0
        )

    available_balance_cents = sum(max(0, b.available_cash_cents) for b in buckets)
    total_assigned_capital_cents = sum(max(0, b.assigned_capital_cents) for b in buckets)
    total_reserved_cents = sum(max(0, b.reserved_cash_cents) for b in buckets)
    total_locked_cents = sum(max(0, b.locked_capital_cents) for b in buckets)
    total_deployed_cents = total_reserved_cents + total_locked_cents
    all_positions_rows = (
        db.query(ExecutionPosition)
        .filter(
            ExecutionPosition.user_id == user.id,
            ExecutionPosition.trading_mode == mode,
        )
        .order_by(ExecutionPosition.updated_at.desc())
        .all()
    )
    if mode == TradingMode.PAPER.value:
        available_balance_cents, portfolio_cents, unrealized_pnl_cents = _paper_balance_snapshot(
            db,
            user_id=user.id,
            trading_mode=mode,
            buckets=buckets,
            positions=all_positions_rows,
        )
        realized_pnl_cents = sum(b.realized_pnl_cents for b in buckets)
        pnl_cents = realized_pnl_cents + unrealized_pnl_cents
    else:
        portfolio_cents = sum(
            b.available_cash_cents
            + b.reserved_cash_cents
            + b.locked_capital_cents
            + b.unrealized_pnl_cents
            for b in buckets
        )
        unrealized_pnl_cents = sum(b.unrealized_pnl_cents for b in buckets)
        realized_pnl_cents = sum(b.realized_pnl_cents for b in buckets)
        pnl_cents = realized_pnl_cents + unrealized_pnl_cents
    portfolio_value = portfolio_cents / 100
    pnl_value = pnl_cents / 100
    base = max(1.0, portfolio_value - pnl_value)
    pnl_percent = (pnl_value / base) * 100
    positions_rows = all_positions_rows[:DASHBOARD_POSITIONS_LIMIT]
    if use_live_balances and mode == TradingMode.LIVE.value and tenant_id is not None:
        live_balances = _live_coinbase_balance_snapshot(db, user, settings, positions_rows)
        if live_balances is not None:
            available_balance_cents, portfolio_cents = live_balances
            portfolio_value = portfolio_cents / 100
            base = max(1.0, portfolio_value - pnl_value)
            pnl_percent = (pnl_value / base) * 100
    position_marks = _latest_symbol_marks(db, [row.symbol for row in positions_rows])
    latest_exit_lifecycle_rows = _latest_by_pair(
        (
            db.query(StrategyLifecycleEvent)
            .filter(
                StrategyLifecycleEvent.user_id == user.id,
                StrategyLifecycleEvent.trading_mode == mode,
                StrategyLifecycleEvent.stage.in_(EXIT_VISIBILITY_STAGES),
                StrategyLifecycleEvent.symbol.in_(
                    [str(row.symbol) for row in positions_rows if row.symbol]
                ),
                StrategyLifecycleEvent.strategy_name.in_(
                    [str(row.strategy_id) for row in positions_rows if row.strategy_id]
                ),
            )
            .order_by(StrategyLifecycleEvent.occurred_at.desc())
            .limit(DASHBOARD_ACTIVITY_SCAN_LIMIT)
            .all()
        )
        if positions_rows
        else [],
        "strategy_name",
        "symbol",
    )
    exit_stall_cutoff = now - timedelta(minutes=EXIT_RELIABILITY_STALL_MINUTES)
    positions: list[dict[str, Any]] = []
    for row in positions_rows:
        qty = _to_float(row.quantity)
        if abs(qty) <= 0.0:
            continue
        entry_price = _to_float(row.avg_entry_price)
        mark = float(position_marks.get((row.symbol or "").upper()) or Decimal(str(entry_price)))
        exposure = abs(qty) * mark
        if exposure < float(DASHBOARD_POSITION_DUST_NOTIONAL_USD):
            continue
        unrealized_pnl = (mark - entry_price) * qty
        opened_at = _as_utc(row.opened_at)
        last_trade_at = _as_utc(row.last_trade_at)
        closed_at = _as_utc(row.closed_at)
        age_seconds = max(0.0, (now - opened_at).total_seconds()) if opened_at is not None else None
        exit_event = latest_exit_lifecycle_rows.get((str(row.strategy_id), str(row.symbol)))
        exit_stage = str(exit_event.stage) if exit_event is not None else None
        exit_event_at = _as_utc(exit_event.occurred_at if exit_event is not None else None)
        exit_reason_code = (
            _normalize_dashboard_reason_code(
                exit_event.reason_code,
                reason_detail=exit_event.reason_detail,
            )
            if exit_event is not None
            else None
        )
        exit_reason_detail = (
            exit_event.reason_detail or exit_event.reason_code if exit_event is not None else None
        )
        exit_stalled = bool(exit_event_at is not None and exit_event_at <= exit_stall_cutoff)
        if exit_stage == "exit_execution_requested":
            exit_status = "stalled" if exit_stalled else "requested"
        elif exit_stage in {
            "take_profit_triggered",
            "stop_loss_triggered",
            "trailing_stop_triggered",
        }:
            exit_status = "stalled" if exit_stalled else "triggered"
        elif exit_stage == "exit_monitoring_started":
            exit_status = "monitoring"
        else:
            exit_status = None
        positions.append(
            {
                "id": str(row.id),
                "symbol": row.symbol,
                "strategy": row.strategy_id,
                "side": "long" if qty >= 0 else "short",
                "quantity": row.quantity,
                "entryPrice": entry_price,
                "markPrice": mark,
                "unrealizedPnl": unrealized_pnl,
                "exposure": exposure,
                "openedAt": opened_at.isoformat() if opened_at is not None else None,
                "lastTradeAt": (last_trade_at.isoformat() if last_trade_at is not None else None),
                "closedAt": closed_at.isoformat() if closed_at is not None else None,
                "ageMinutes": (round(age_seconds / 60, 2) if age_seconds is not None else None),
                "ageHours": (round(age_seconds / 3600, 4) if age_seconds is not None else None),
                "exitStatus": exit_status,
                "exitStage": exit_stage,
                "exitReasonCode": exit_reason_code,
                "exitReasonDetail": exit_reason_detail,
                "exitUpdatedAt": exit_event_at.isoformat() if exit_event_at is not None else None,
                "exitStalled": exit_stalled,
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
    avg_trade_size_rows = db.execute(
        select(
            ExecutionTradeRecord.strategy_id,
            func.coalesce(func.avg(ExecutionTradeRecord.gross_notional_cents), 0).label(
                "avg_notional_cents"
            ),
        )
        .where(
            ExecutionTradeRecord.user_id == user.id,
            ExecutionTradeRecord.trading_mode == mode,
            ExecutionTradeRecord.executed_at >= dashboard_cutoff,
        )
        .group_by(ExecutionTradeRecord.strategy_id)
        .order_by(ExecutionTradeRecord.strategy_id.asc())
    ).all()
    capital_utilization = {
        "totalCapital": round(total_assigned_capital_cents / 100, 2),
        "availableCash": round(available_balance_cents / 100, 2),
        "reservedCash": round(total_reserved_cents / 100, 2),
        "lockedCapital": round(total_locked_cents / 100, 2),
        "deployedCapital": round(total_deployed_cents / 100, 2),
        "totalDeployedPct": (
            round((total_deployed_cents / total_assigned_capital_cents) * 100, 2)
            if total_assigned_capital_cents > 0
            else 0.0
        ),
        "byStrategy": [
            {
                "strategy": bucket.strategy_id,
                "assignedCapital": round(bucket.assigned_capital_cents / 100, 2),
                "availableCash": round(bucket.available_cash_cents / 100, 2),
                "reservedCash": round(bucket.reserved_cash_cents / 100, 2),
                "lockedCapital": round(bucket.locked_capital_cents / 100, 2),
                "deployedCapital": round(
                    (bucket.reserved_cash_cents + bucket.locked_capital_cents) / 100,
                    2,
                ),
                "utilizationPct": (
                    round(
                        (
                            (bucket.reserved_cash_cents + bucket.locked_capital_cents)
                            / max(bucket.assigned_capital_cents, 1)
                        )
                        * 100,
                        2,
                    )
                    if bucket.assigned_capital_cents > 0
                    else 0.0
                ),
            }
            for bucket in sorted(buckets, key=lambda item: item.strategy_id)
        ],
        "avgTradeSizeByStrategy": [
            {
                "strategy": str(strategy_id),
                "avgTradeSize": round(int(avg_notional_cents or 0) / 100, 2),
            }
            for strategy_id, avg_notional_cents in avg_trade_size_rows
        ],
    }

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
    total_mode_fees_cents = int(trade_stats.total_fees_cents or 0)
    total_mode_net_pnl_cents = int(trade_stats.total_net_pnl_cents or 0)
    total_mode_gross_pnl_cents = total_mode_net_pnl_cents + total_mode_fees_cents
    skipped_due_to_fees = 0
    rejection_diagnostics = {
        "totalRejected": 0,
        "byStage": [],
        "breakdown": [],
        "recent": [],
    }
    if include_rejection_diagnostics:
        strategy_records, _ = _recent_strategy_rejection_records(
            user=user,
            db=db,
            trading_mode=mode,
            cutoff=dashboard_cutoff,
            limit=DASHBOARD_REJECTION_EVENT_LIMIT,
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
        skipped_due_to_fees = sum(
            1 for event in recent_risk_rejects if "fee_economics" in str(event.detail or "").lower()
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
        rejection_diagnostics = _build_rejection_diagnostics(
            strategy_records=strategy_records,
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
        strategy_reject_total = len(strategy_records)
        risk_reject_total = len(recent_risk_rejects)
        execution_reject_total = len(recent_failed_orders)
        rejection_diagnostics["byStage"] = [
            {"stage": "risk", "count": risk_reject_total},
            {"stage": "suppression", "count": strategy_reject_total},
            {"stage": "execution", "count": execution_reject_total},
        ]
        rejection_diagnostics["byStage"] = [
            row for row in rejection_diagnostics["byStage"] if int(row["count"]) > 0
        ]
        rejection_diagnostics["byStage"].sort(
            key=lambda row: (-int(row["count"]), str(row["stage"]))
        )
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

    growth = _build_dashboard_growth_series(
        db=db,
        user_id=user.id,
        trading_mode=mode,
        cutoff=dashboard_cutoff,
        now=now,
        portfolio_value=portfolio_value,
        unrealized_pnl_value=unrealized_pnl_cents / 100,
    )
    strategy_health = _build_strategy_health(
        user=user,
        db=db,
        trading_mode=mode,
        enabled_strategies=enabled_strategies,
        buckets=buckets,
        positions_rows=all_positions_rows,
        dashboard_cutoff=dashboard_cutoff,
    )
    bot_health = _build_bot_health(
        user=user,
        db=db,
        tenant_id=tenant_id,
        trading_mode=mode,
        enabled_strategies=enabled_strategies,
        positions=positions,
        active_trades=active_trades,
        recent_activity=recent_activity,
        strategy_health=strategy_health,
        reconciliation_health=_build_reconciliation_health(
            db=db,
            user=user,
            trading_mode=mode,
            positions_rows=all_positions_rows,
            buckets=buckets,
        ),
    )

    return {
        "availableBalance": round(available_balance_cents / 100, 2),
        "portfolioValue": round(portfolio_value, 2),
        "pnlValue": round(pnl_value, 2),
        "realizedPnlValue": round(realized_pnl_cents / 100, 2),
        "unrealizedPnlValue": round(unrealized_pnl_cents / 100, 2),
        "pnlPercent": round(pnl_percent, 4),
        "gainLossLabel": "Total P&L",
        "growth": growth,
        "enabledStrategies": enabled_strategies,
        "strategyHealth": strategy_health,
        "botHealth": bot_health,
        "positions": positions,
        "activeTrades": active_trades,
        "recentActivity": recent_activity,
        "capitalUtilization": capital_utilization,
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


@router.get("/dashboard/rejections")
def dashboard_rejection_history(
    user: CurrentUser,
    db: DbSession,
    trading_mode: TradingMode | None = None,
    window_hours: int = Query(default=24),
    force_refresh: bool = Query(default=False),
    settings: Settings = Depends(settings_dep),
) -> dict[str, Any]:
    if window_hours not in {1, 3, 6, 24, 48}:
        raise HTTPException(status_code=422, detail="window_hours must be one of 1, 3, 6, 24, 48")
    payload = _cached_dashboard_rejection_payload(
        user=user,
        db=db,
        settings=settings,
        trading_mode=trading_mode,
        window_hours=window_hours,
        force_refresh=force_refresh,
    )
    return _dashboard_rejection_payload(payload)


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
        ttl_seconds=ANALYTICS_SUMMARY_CACHE_TTL_SECONDS,
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


@router.get("/analytics/outcomes")
def read_trade_review_outcomes(
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
        namespace="analytics-outcomes-v1",
        user=user,
        db=db,
        settings=settings,
        trading_mode=trading_mode,
        strategy_name=strategy_name,
        symbol=symbol,
        start_at=start_at,
        end_at=end_at,
        force_refresh=force_refresh,
        builder=_analytics_outcomes_payload,
    )


@router.get("/analytics/validation")
def read_trade_review_paper_live_validation(
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
        namespace="analytics-validation-v1",
        user=user,
        db=db,
        settings=settings,
        trading_mode=trading_mode,
        strategy_name=strategy_name,
        symbol=symbol,
        start_at=start_at,
        end_at=end_at,
        force_refresh=force_refresh,
        builder=_analytics_validation_payload,
    )


@router.get("/exports/trading-performance.csv")
def export_trading_performance_csv(
    user: CurrentUser,
    db: DbSession,
    trading_mode: TradingMode | None = None,
    limit: int = Query(default=100, ge=1, le=5000),
) -> Response:
    """Download recent trade outcome rows for the current user as CSV."""
    from oziebot_api.scripts import trading_performance_report as tpr

    effective_mode = trading_mode
    if effective_mode is None:
        try:
            effective_mode = TradingMode(user.current_trading_mode or "paper")
        except ValueError:
            effective_mode = TradingMode.PAPER

    data = tpr.build_report(
        db,
        limit=limit,
        user_id=user.id,
        trading_mode=effective_mode.value,
    )
    csv_text = tpr.trades_to_csv_string(data["trades"])
    safe_mode = effective_mode.value.replace(" ", "_")
    filename = f"oziebot-trade-outcomes-{safe_mode}-n{limit}.csv"
    return Response(
        content=csv_text.encode("utf-8"),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
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
