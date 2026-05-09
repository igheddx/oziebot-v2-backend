from __future__ import annotations

from collections import defaultdict
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import Numeric, Select, cast, func, select
from sqlalchemy.orm import Session

from oziebot_api.models.execution import ExecutionPosition
from oziebot_api.models.strategy_lifecycle import StrategyLifecycleEvent
from oziebot_api.services.admin_trading_diagnostics import TradingDiagnosticsFilters

LIFECYCLE_STAGE_ORDER = [
    "signal_generated",
    "signal_emitted",
    "validation_started",
    "confidence_validation",
    "volume_validation",
    "trend_validation",
    "cooldown_validation",
    "allocation_validation",
    "policy_validation",
    "risk_validation",
    "execution_requested",
    "execution_succeeded",
    "execution_failed",
    "position_opened",
    "exit_monitoring_started",
    "take_profit_triggered",
    "stop_loss_triggered",
    "trailing_stop_triggered",
    "exit_execution_requested",
    "position_closed",
]


def build_strategy_lifecycle_diagnostics(
    db: Session,
    *,
    filters: TradingDiagnosticsFilters,
) -> dict[str, Any]:
    now = datetime.now(UTC)
    window_start = filters.window_start(now)
    stage_counts = _stage_counts(db, filters=filters, window_start=window_start)
    failure_counts = _failure_counts(db, filters=filters, window_start=window_start)
    reason_rows = _failure_reason_rows(db, filters=filters, window_start=window_start)
    latest_ids = _latest_trace_ids(
        db, filters=filters, window_start=window_start, limit=filters.limit
    )
    latest_traces = _load_traces(db, trace_ids=latest_ids)
    open_positions = _load_open_positions(db, filters=filters)

    funnel: list[dict[str, Any]] = []
    previous_count: int | None = None
    for stage in LIFECYCLE_STAGE_ORDER:
        count = stage_counts.get(stage, 0)
        failed = failure_counts.get(stage, 0)
        funnel.append(
            {
                "stage": stage,
                "trace_count": count,
                "failed_count": failed,
                "conversion_from_previous_pct": (
                    round((count / previous_count) * 100, 4)
                    if previous_count not in (None, 0)
                    else None
                ),
            }
        )
        previous_count = count

    reasons_by_stage: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for stage, reason_code, failure_count in reason_rows:
        reasons_by_stage[str(stage)].append(
            {
                "reason_code": str(reason_code or "unspecified"),
                "failure_count": int(failure_count or 0),
            }
        )

    stage_failures = [
        {
            "stage": stage,
            "failure_count": failure_counts.get(stage, 0),
            "top_reasons": reasons_by_stage.get(stage, [])[:5],
        }
        for stage in LIFECYCLE_STAGE_ORDER
        if failure_counts.get(stage, 0) > 0
    ]

    summary = {
        "trace_count": _trace_count(db, filters=filters, window_start=window_start),
        "blocked_by_policy": failure_counts.get("policy_validation", 0),
        "blocked_by_risk": failure_counts.get("risk_validation", 0),
        "execution_failures": failure_counts.get("execution_failed", 0),
        "exit_engine_failures": _stage_side_failure_count(
            db,
            filters=filters,
            window_start=window_start,
            stage="execution_failed",
            side="sell",
        ),
        "positions_without_exits": sum(1 for row in open_positions if not row["has_exit_request"]),
        "stuck_open_positions": sum(1 for row in open_positions if row["is_stuck_open"]),
        "closed_positions": stage_counts.get("position_closed", 0),
    }

    note = (
        "Open positions are a current snapshot from execution_positions and are filtered by token, strategy, "
        "and trading mode only; the days filter applies to lifecycle traces."
    )
    return {
        "generated_at": now.isoformat(),
        "summary": summary,
        "funnel": funnel,
        "stage_failures": stage_failures,
        "open_positions": open_positions,
        "latest_traces": latest_traces,
        "data_sources": ["strategy_lifecycle_events", "execution_positions"],
        "note": note,
    }


def list_strategy_lifecycle_traces(
    db: Session,
    *,
    filters: TradingDiagnosticsFilters,
) -> dict[str, Any]:
    now = datetime.now(UTC)
    window_start = filters.window_start(now)
    trace_ids = _latest_trace_ids(
        db,
        filters=filters,
        window_start=window_start,
        limit=filters.limit,
    )
    traces = _load_traces(db, trace_ids=trace_ids)
    return {
        "generated_at": now.isoformat(),
        "trace_count": len(traces),
        "traces": traces,
    }


def _lifecycle_stmt(*, filters: TradingDiagnosticsFilters, window_start: datetime) -> Select[Any]:
    stmt = select(StrategyLifecycleEvent).where(StrategyLifecycleEvent.occurred_at >= window_start)
    if filters.normalized_token:
        stmt = stmt.where(StrategyLifecycleEvent.symbol == filters.normalized_token)
    if filters.normalized_strategy:
        stmt = stmt.where(StrategyLifecycleEvent.strategy_name == filters.normalized_strategy)
    if filters.normalized_mode:
        stmt = stmt.where(StrategyLifecycleEvent.trading_mode == filters.normalized_mode)
    return stmt


def _trace_count(db: Session, *, filters: TradingDiagnosticsFilters, window_start: datetime) -> int:
    stmt = _lifecycle_stmt(filters=filters, window_start=window_start).with_only_columns(
        func.count(func.distinct(StrategyLifecycleEvent.correlation_id))
    )
    return int(db.execute(stmt).scalar() or 0)


def _stage_counts(
    db: Session,
    *,
    filters: TradingDiagnosticsFilters,
    window_start: datetime,
) -> dict[str, int]:
    stmt = (
        _lifecycle_stmt(filters=filters, window_start=window_start)
        .with_only_columns(
            StrategyLifecycleEvent.stage,
            func.count(func.distinct(StrategyLifecycleEvent.correlation_id)),
        )
        .group_by(StrategyLifecycleEvent.stage)
    )
    return {str(stage): int(count or 0) for stage, count in db.execute(stmt).all()}


def _failure_counts(
    db: Session,
    *,
    filters: TradingDiagnosticsFilters,
    window_start: datetime,
) -> dict[str, int]:
    stmt = (
        _lifecycle_stmt(filters=filters, window_start=window_start)
        .where(StrategyLifecycleEvent.status == "failed")
        .with_only_columns(
            StrategyLifecycleEvent.stage,
            func.count(func.distinct(StrategyLifecycleEvent.correlation_id)),
        )
        .group_by(StrategyLifecycleEvent.stage)
    )
    return {str(stage): int(count or 0) for stage, count in db.execute(stmt).all()}


def _failure_reason_rows(
    db: Session,
    *,
    filters: TradingDiagnosticsFilters,
    window_start: datetime,
) -> list[tuple[str, str, int]]:
    stmt = (
        _lifecycle_stmt(filters=filters, window_start=window_start)
        .where(StrategyLifecycleEvent.status == "failed")
        .with_only_columns(
            StrategyLifecycleEvent.stage,
            StrategyLifecycleEvent.reason_code,
            func.count(func.distinct(StrategyLifecycleEvent.correlation_id)),
        )
        .group_by(StrategyLifecycleEvent.stage, StrategyLifecycleEvent.reason_code)
        .order_by(
            StrategyLifecycleEvent.stage.asc(),
            func.count(func.distinct(StrategyLifecycleEvent.correlation_id)).desc(),
        )
    )
    return [
        (str(stage), str(reason_code or "unspecified"), int(count or 0))
        for stage, reason_code, count in db.execute(stmt).all()
    ]


def _stage_side_failure_count(
    db: Session,
    *,
    filters: TradingDiagnosticsFilters,
    window_start: datetime,
    stage: str,
    side: str,
) -> int:
    stmt = (
        _lifecycle_stmt(filters=filters, window_start=window_start)
        .where(StrategyLifecycleEvent.stage == stage)
        .where(StrategyLifecycleEvent.status == "failed")
        .where(StrategyLifecycleEvent.side == side)
        .with_only_columns(func.count(func.distinct(StrategyLifecycleEvent.correlation_id)))
    )
    return int(db.execute(stmt).scalar() or 0)


def _latest_trace_ids(
    db: Session,
    *,
    filters: TradingDiagnosticsFilters,
    window_start: datetime,
    limit: int,
) -> list[str]:
    stmt = (
        _lifecycle_stmt(filters=filters, window_start=window_start)
        .with_only_columns(
            StrategyLifecycleEvent.correlation_id,
            func.max(StrategyLifecycleEvent.occurred_at).label("last_seen_at"),
        )
        .group_by(StrategyLifecycleEvent.correlation_id)
        .order_by(func.max(StrategyLifecycleEvent.occurred_at).desc())
        .limit(max(1, min(limit, 100)))
    )
    return [str(correlation_id) for correlation_id, _ in db.execute(stmt).all()]


def _load_traces(db: Session, *, trace_ids: list[str]) -> list[dict[str, Any]]:
    if not trace_ids:
        return []
    stmt = (
        select(StrategyLifecycleEvent)
        .where(StrategyLifecycleEvent.correlation_id.in_(trace_ids))
        .order_by(
            StrategyLifecycleEvent.correlation_id.asc(),
            StrategyLifecycleEvent.occurred_at.asc(),
            StrategyLifecycleEvent.stage.asc(),
        )
    )
    grouped: dict[str, list[StrategyLifecycleEvent]] = defaultdict(list)
    for event in db.scalars(stmt).all():
        grouped[event.correlation_id].append(event)
    traces: list[dict[str, Any]] = []
    for trace_id in trace_ids:
        events = grouped.get(trace_id, [])
        if not events:
            continue
        last_event = events[-1]
        latest_reason = next(
            (event for event in reversed(events) if event.reason_code or event.reason_detail),
            last_event,
        )
        traces.append(
            {
                "correlation_id": trace_id,
                "strategy": last_event.strategy_name,
                "token": last_event.symbol,
                "trading_mode": last_event.trading_mode,
                "current_stage": last_event.stage,
                "current_status": last_event.status,
                "started_at": _aware_iso(events[0].occurred_at),
                "last_event_at": _aware_iso(last_event.occurred_at),
                "latest_reason_code": latest_reason.reason_code,
                "latest_reason_detail": latest_reason.reason_detail,
                "events": [
                    {
                        "stage": event.stage,
                        "status": event.status,
                        "occurred_at": _aware_iso(event.occurred_at),
                        "side": event.side,
                        "reason_code": event.reason_code,
                        "reason_detail": event.reason_detail,
                        "metadata": dict(event.event_metadata or {}),
                    }
                    for event in events
                ],
            }
        )
    return traces


def _load_open_positions(
    db: Session,
    *,
    filters: TradingDiagnosticsFilters,
) -> list[dict[str, Any]]:
    stmt = select(ExecutionPosition).where(_numeric_string_expr(ExecutionPosition.quantity) > 0)
    if filters.normalized_token:
        stmt = stmt.where(ExecutionPosition.symbol == filters.normalized_token)
    if filters.normalized_strategy:
        stmt = stmt.where(ExecutionPosition.strategy_id == filters.normalized_strategy)
    if filters.normalized_mode:
        stmt = stmt.where(ExecutionPosition.trading_mode == filters.normalized_mode)
    positions = list(db.scalars(stmt.order_by(ExecutionPosition.updated_at.desc())).all())
    if not positions:
        return []

    rows: list[dict[str, Any]] = []
    for position in positions:
        lifecycle_stmt = (
            select(StrategyLifecycleEvent)
            .where(StrategyLifecycleEvent.user_id == position.user_id)
            .where(StrategyLifecycleEvent.strategy_name == position.strategy_id)
            .where(StrategyLifecycleEvent.symbol == position.symbol)
            .where(StrategyLifecycleEvent.trading_mode == position.trading_mode)
            .where(
                StrategyLifecycleEvent.stage.in_(
                    ("position_opened", "exit_execution_requested", "position_closed")
                )
            )
            .order_by(StrategyLifecycleEvent.occurred_at.asc())
        )
        if position.opened_at is not None:
            lifecycle_stmt = lifecycle_stmt.where(
                StrategyLifecycleEvent.occurred_at >= position.opened_at
            )
        lifecycle_events = list(db.scalars(lifecycle_stmt).all())
        latest_open = next(
            (event for event in reversed(lifecycle_events) if event.stage == "position_opened"),
            None,
        )
        has_exit_request = any(
            event.stage == "exit_execution_requested" for event in lifecycle_events
        )
        has_position_closed = any(event.stage == "position_closed" for event in lifecycle_events)
        rows.append(
            {
                "position_id": str(position.id),
                "strategy": position.strategy_id,
                "token": position.symbol,
                "trading_mode": position.trading_mode,
                "quantity": _float_or_none(position.quantity),
                "avg_entry_price": _float_or_none(position.avg_entry_price),
                "opened_at": _aware_iso(position.opened_at),
                "updated_at": _aware_iso(position.updated_at),
                "last_trade_at": _aware_iso(position.last_trade_at),
                "latest_correlation_id": latest_open.correlation_id
                if latest_open is not None
                else None,
                "has_exit_request": has_exit_request,
                "is_stuck_open": has_exit_request and not has_position_closed,
            }
        )
    return rows


def _numeric_string_expr(column) -> Any:
    return cast(column, Numeric(28, 10))


def _float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    return round(float(value), 8)


def _aware_iso(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.isoformat()
