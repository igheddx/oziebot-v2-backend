from __future__ import annotations

import csv
import io
import os
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

from sqlalchemy import Select, select
from sqlalchemy.orm import Session

from oziebot_common.token_policy import normalize_missing_policy_behavior
from oziebot_api.config import Settings
from oziebot_api.models.execution import ExecutionOrder, ExecutionTradeRecord
from oziebot_api.models.platform_strategy import PlatformStrategy
from oziebot_api.models.risk_event import RiskEvent
from oziebot_api.models.strategy_allocation import StrategyCapitalBucket, StrategyCapitalLedger
from oziebot_api.models.strategy_signal_pipeline import StrategyRun, StrategySignalRecord
from oziebot_api.models.trade_intelligence import StrategySignalSnapshot, TradeOutcomeFeature
from oziebot_api.services.token_policy import TokenPolicyService


@dataclass(slots=True)
class TradingDiagnosticsFilters:
    days: int = 7
    token: str | None = None
    strategy: str | None = None
    trading_mode: str | None = None
    limit: int = 100

    @property
    def normalized_token(self) -> str | None:
        value = (self.token or "").strip().upper()
        return value or None

    @property
    def normalized_strategy(self) -> str | None:
        value = (self.strategy or "").strip().lower()
        return value or None

    @property
    def normalized_mode(self) -> str | None:
        value = (self.trading_mode or "").strip().lower()
        return value or None

    def window_start(self, now: datetime) -> datetime:
        return now - timedelta(days=max(1, self.days))


def build_trading_diagnostics_report(
    db: Session,
    settings: Settings,
    *,
    filters: TradingDiagnosticsFilters,
) -> dict[str, Any]:
    now = datetime.now(UTC)
    window_start = filters.window_start(now)
    trade_rows = _load_trade_rows(db, filters=filters, window_start=window_start)
    trade_details = [_trade_row_to_payload(feat, trade, snapshot) for feat, trade, snapshot in trade_rows]

    signal_funnel = _build_signal_funnel(
        db,
        filters=filters,
        window_start=window_start,
        trade_count=len(trade_details),
    )
    capital_utilization = _build_capital_utilization(
        db,
        filters=filters,
        window_start=window_start,
    )
    active_strategy_config = _build_active_strategy_config(db, settings)

    return {
        "generated_at": now.isoformat(),
        "trade_count": len(trade_details),
        "trade_details": trade_details,
        "strategy_summary": _build_strategy_summary(trade_details),
        "token_summary": _build_token_summary(trade_details),
        "signal_funnel": signal_funnel,
        "capital_utilization": capital_utilization,
        "exit_analysis": _build_exit_analysis(trade_details),
        "active_strategy_config": active_strategy_config,
    }


def render_trading_diagnostics_csv(report: dict[str, Any]) -> str:
    buf = io.StringIO()
    writer = csv.DictWriter(
        buf,
        fieldnames=[
            "trade_id",
            "strategy",
            "token",
            "trading_mode",
            "entry_time",
            "exit_time",
            "hold_minutes",
            "entry_price",
            "exit_price",
            "quantity",
            "size_usd",
            "fees_usd",
            "gross_pnl_usd",
            "net_pnl_usd",
            "pnl_pct",
            "exit_reason",
            "partial_profit_taken",
            "max_favorable_excursion_pct",
            "max_adverse_excursion_pct",
            "peak_unrealized_pnl_pct",
            "profit_giveback_pct",
            "signal_confidence",
            "volume_confirmation_passed",
            "rejected_before_execution",
        ],
    )
    writer.writeheader()
    for row in report["trade_details"]:
        writer.writerow(row)
    return buf.getvalue()


def _load_trade_rows(
    db: Session,
    *,
    filters: TradingDiagnosticsFilters,
    window_start: datetime,
) -> list[tuple[TradeOutcomeFeature, ExecutionTradeRecord, StrategySignalSnapshot | None]]:
    stmt = (
        select(TradeOutcomeFeature, ExecutionTradeRecord, StrategySignalSnapshot)
        .join(ExecutionTradeRecord, TradeOutcomeFeature.trade_id == ExecutionTradeRecord.id)
        .outerjoin(StrategySignalSnapshot, TradeOutcomeFeature.signal_snapshot_id == StrategySignalSnapshot.id)
        .where(TradeOutcomeFeature.created_at >= window_start)
        .order_by(TradeOutcomeFeature.created_at.desc())
        .limit(max(1, min(filters.limit, 100)))
    )
    if filters.normalized_token:
        stmt = stmt.where(TradeOutcomeFeature.token_symbol == filters.normalized_token)
    if filters.normalized_strategy:
        stmt = stmt.where(TradeOutcomeFeature.strategy_name == filters.normalized_strategy)
    if filters.normalized_mode:
        stmt = stmt.where(TradeOutcomeFeature.trading_mode == filters.normalized_mode)
    return list(db.execute(stmt).all())


def _trade_row_to_payload(
    feat: TradeOutcomeFeature,
    trade: ExecutionTradeRecord,
    snapshot: StrategySignalSnapshot | None,
) -> dict[str, Any]:
    exit_at = _aware_iso(feat.created_at)
    hold_minutes = round(feat.hold_seconds / 60, 2) if feat.hold_seconds is not None else None
    entry_at = (
        _aware_iso(feat.created_at - timedelta(seconds=feat.hold_seconds))
        if feat.hold_seconds is not None
        else None
    )
    entry_price = _float_or_none(feat.entry_price)
    quantity = _float_or_none(feat.filled_size)
    fees_usd = _float_or_none(feat.fee_paid)
    net_pnl_usd = _float_or_none(feat.realized_pnl)
    gross_pnl_usd = (
        round((net_pnl_usd or 0.0) + (fees_usd or 0.0), 6)
        if net_pnl_usd is not None or fees_usd is not None
        else None
    )
    size_usd = (
        round((entry_price or 0.0) * (quantity or 0.0), 6)
        if entry_price is not None and quantity is not None
        else None
    )
    signal_confidence = (
        _float_or_none(snapshot.confidence_score) if snapshot is not None else None
    )

    return {
        "trade_id": str(feat.trade_id),
        "strategy": feat.strategy_name,
        "token": feat.token_symbol,
        "trading_mode": feat.trading_mode,
        "entry_time": entry_at,
        "exit_time": exit_at,
        "hold_minutes": hold_minutes,
        "entry_price": entry_price,
        "exit_price": _float_or_none(feat.exit_price),
        "quantity": quantity,
        "size_usd": size_usd,
        "fees_usd": fees_usd,
        "gross_pnl_usd": gross_pnl_usd,
        "net_pnl_usd": net_pnl_usd,
        "pnl_pct": _pct(feat.realized_return_pct),
        "exit_reason": feat.exit_reason,
        "partial_profit_taken": bool(feat.partial_profit_taken),
        "max_favorable_excursion_pct": _pct(feat.max_favorable_excursion_pct),
        "max_adverse_excursion_pct": _pct(feat.max_adverse_excursion_pct),
        "peak_unrealized_pnl_pct": None,
        "profit_giveback_pct": _pct(feat.profit_giveback_pct),
        "signal_confidence": signal_confidence,
        "volume_confirmation_passed": None,
        "rejected_before_execution": False,
    }


def _build_strategy_summary(trades: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for trade in trades:
        grouped[trade["strategy"]].append(trade)

    output: list[dict[str, Any]] = []
    for strategy, rows in sorted(grouped.items()):
        wins = [row for row in rows if (row["net_pnl_usd"] or 0) > 0]
        losses = [row for row in rows if (row["net_pnl_usd"] or 0) < 0]
        gross_win = sum(row["net_pnl_usd"] or 0 for row in wins)
        gross_loss = abs(sum(row["net_pnl_usd"] or 0 for row in losses))
        ordered_returns = [row["net_pnl_usd"] or 0 for row in sorted(rows, key=_trade_sort_key)]
        output.append(
            {
                "strategy": strategy,
                "total_trades": len(rows),
                "wins": len(wins),
                "losses": len(losses),
                "win_rate_pct": _ratio_pct(len(wins), len(rows)),
                "avg_win_pct": _avg([row["pnl_pct"] for row in wins]),
                "avg_loss_pct": _avg([row["pnl_pct"] for row in losses]),
                "profit_factor": round(gross_win / gross_loss, 4) if gross_loss else None,
                "total_net_pnl_usd": round(sum(row["net_pnl_usd"] or 0 for row in rows), 6),
                "total_net_pnl_pct": round(sum(row["pnl_pct"] or 0 for row in rows), 6),
                "max_drawdown_pct": _max_drawdown_pct(ordered_returns),
                "avg_hold_minutes": _avg([row["hold_minutes"] for row in rows]),
                "stop_loss_exits": _count_exit_reason(rows, "stop"),
                "take_profit_exits": _count_exit_reason(rows, "take"),
                "trailing_stop_exits": _count_exit_reason(rows, "trailing"),
                "partial_profit_exits": sum(1 for row in rows if row["partial_profit_taken"]),
                "max_hold_exits": _count_exit_reason(rows, "max_hold"),
                "bearish_signal_exits": _count_exit_reason(rows, "bearish"),
                "avg_profit_giveback_pct": _avg([row["profit_giveback_pct"] for row in rows]),
            }
        )
    return output


def _build_token_summary(trades: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for trade in trades:
        grouped[trade["token"]].append(trade)

    output: list[dict[str, Any]] = []
    for token, rows in sorted(grouped.items()):
        by_strategy: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in rows:
            by_strategy[row["strategy"]].append(row)
        strategy_scores = {
            strategy: sum(item["net_pnl_usd"] or 0 for item in items)
            for strategy, items in by_strategy.items()
        }
        best_strategy = max(strategy_scores, key=strategy_scores.get) if strategy_scores else None
        worst_strategy = min(strategy_scores, key=strategy_scores.get) if strategy_scores else None
        wins = sum(1 for row in rows if (row["net_pnl_usd"] or 0) > 0)
        output.append(
            {
                "token": token,
                "total_trades": len(rows),
                "win_rate_pct": _ratio_pct(wins, len(rows)),
                "total_net_pnl_usd": round(sum(row["net_pnl_usd"] or 0 for row in rows), 6),
                "total_net_pnl_pct": round(sum(row["pnl_pct"] or 0 for row in rows), 6),
                "avg_trade_return_pct": _avg([row["pnl_pct"] for row in rows]),
                "avg_hold_minutes": _avg([row["hold_minutes"] for row in rows]),
                "avg_profit_giveback_pct": _avg([row["profit_giveback_pct"] for row in rows]),
                "best_strategy": best_strategy,
                "worst_strategy": worst_strategy,
            }
        )
    return output


def _build_signal_funnel(
    db: Session,
    *,
    filters: TradingDiagnosticsFilters,
    window_start: datetime,
    trade_count: int,
) -> dict[str, Any]:
    runs = _load_rows(
        db,
        _apply_signal_filters(
            select(StrategyRun),
            filters=filters,
            window_start=window_start,
            token_column=StrategyRun.symbol,
            strategy_column=StrategyRun.strategy_name,
            mode_column=StrategyRun.trading_mode,
            timestamp_column=StrategyRun.started_at,
        ),
    )
    signals = _load_rows(
        db,
        _apply_signal_filters(
            select(StrategySignalRecord),
            filters=filters,
            window_start=window_start,
            token_column=StrategySignalRecord.symbol,
            strategy_column=StrategySignalRecord.strategy_name,
            mode_column=StrategySignalRecord.trading_mode,
            timestamp_column=StrategySignalRecord.timestamp,
        ),
    )
    risk_events = _load_rows(
        db,
        _apply_signal_filters(
            select(RiskEvent),
            filters=filters,
            window_start=window_start,
            token_column=RiskEvent.symbol,
            strategy_column=RiskEvent.strategy_name,
            mode_column=RiskEvent.trading_mode,
            timestamp_column=RiskEvent.created_at,
        ),
    )
    orders = _load_rows(
        db,
        _apply_signal_filters(
            select(ExecutionOrder),
            filters=filters,
            window_start=window_start,
            token_column=ExecutionOrder.symbol,
            strategy_column=ExecutionOrder.strategy_id,
            mode_column=ExecutionOrder.trading_mode,
            timestamp_column=ExecutionOrder.created_at,
        ),
    )

    suppressed_runs = []
    rejection_reasons: Counter[str] = Counter()
    for run in runs:
        metadata = run.run_metadata or {}
        if metadata.get("suppressed"):
            suppressed_runs.append(run)
            rejection_reasons[_bucket_rejection_reason(metadata.get("suppression_reason"))] += 1

    risk_rejects = []
    for event in risk_events:
        outcome = (event.outcome or "").lower()
        if outcome.startswith("reject"):
            risk_rejects.append(event)
            rejection_reasons[_bucket_rejection_reason(event.reason or event.detail)] += 1

    failed_orders = []
    for order in orders:
        state = (order.state or "").lower()
        if state in {"failed", "cancelled", "rejected"}:
            failed_orders.append(order)
            rejection_reasons[_bucket_rejection_reason(order.failure_code or order.failure_detail or state)] += 1

    signals_evaluated: int | None
    if runs:
        signals_evaluated = len(runs)
    elif trade_count > 0 or signals or risk_events or orders:
        signals_evaluated = None
    else:
        signals_evaluated = 0

    if signals:
        signals_emitted: int | None = len(signals)
    elif trade_count > 0 or runs or risk_events or orders:
        signals_emitted = None
    else:
        signals_emitted = 0

    rejected_count = len(suppressed_runs) + len(risk_rejects) + len(failed_orders)
    if rejected_count:
        signals_rejected: int | None = rejected_count
    elif runs or signals or risk_events:
        signals_rejected = 0
    elif trade_count > 0:
        signals_rejected = None
    else:
        signals_rejected = 0

    unavailable_metrics = [
        metric
        for metric, value in (
            ("signals_evaluated", signals_evaluated),
            ("signals_emitted", signals_emitted),
            ("signals_rejected", signals_rejected),
        )
        if value is None
    ]

    return {
        "signals_evaluated": signals_evaluated,
        "signals_emitted": signals_emitted,
        "signals_rejected": signals_rejected,
        "trades_executed": trade_count,
        "rejection_reasons": {
            "confidence": _counter_or_none(rejection_reasons, "confidence", signals_rejected),
            "volume": _counter_or_none(rejection_reasons, "volume", signals_rejected),
            "allocation": _counter_or_none(rejection_reasons, "allocation", signals_rejected),
            "risk_engine": _counter_or_none(rejection_reasons, "risk_engine", signals_rejected),
            "token_strategy_policy": _counter_or_none(
                rejection_reasons, "token_strategy_policy", signals_rejected
            ),
            "cooldown": _counter_or_none(rejection_reasons, "cooldown", signals_rejected),
            "liquidity_hours": _counter_or_none(rejection_reasons, "liquidity_hours", signals_rejected),
            "other": _counter_or_none(rejection_reasons, "other", signals_rejected),
        },
        "data_sources": {
            "signals_evaluated": "strategy_runs",
            "signals_emitted": "strategy_signal_records",
            "signals_rejected": "strategy_runs.run_metadata + risk_events + execution_orders",
            "trades_executed": "trade_outcome_features",
        },
        "unavailable_metrics": unavailable_metrics,
        "note": (
            "Signals were executed or completed, but earlier-stage telemetry is unavailable for some metrics."
            if unavailable_metrics
            else None
        ),
    }


def _build_capital_utilization(
    db: Session,
    *,
    filters: TradingDiagnosticsFilters,
    window_start: datetime,
) -> dict[str, Any]:
    bucket_stmt = select(StrategyCapitalBucket)
    ledger_stmt = select(StrategyCapitalLedger).where(StrategyCapitalLedger.created_at >= window_start)
    if filters.normalized_strategy:
        bucket_stmt = bucket_stmt.where(StrategyCapitalBucket.strategy_id == filters.normalized_strategy)
        ledger_stmt = ledger_stmt.where(StrategyCapitalLedger.strategy_id == filters.normalized_strategy)
    if filters.normalized_mode:
        bucket_stmt = bucket_stmt.where(StrategyCapitalBucket.trading_mode == filters.normalized_mode)
        ledger_stmt = ledger_stmt.where(StrategyCapitalLedger.trading_mode == filters.normalized_mode)
    buckets = list(db.scalars(bucket_stmt).all())
    ledgers = list(db.scalars(ledger_stmt).all())

    note_parts: list[str] = []
    if filters.normalized_token:
        note_parts.append("Capital buckets are strategy-scoped, so token filters do not change capital totals.")
    if not buckets:
        return {
            "total_account_value": None,
            "avg_capital_deployed_pct": None,
            "peak_capital_deployed_pct": None,
            "avg_cash_idle_pct": None,
            "capital_by_strategy": {
                "momentum": None,
                "day_trading": None,
                "reversion": None,
                "dca": None,
            },
            "note": " ".join(note_parts) if note_parts else "No capital bucket data matched the selected filters.",
        }

    total_account_value = sum(
        bucket.assigned_capital_cents + bucket.realized_pnl_cents + bucket.unrealized_pnl_cents
        for bucket in buckets
    ) / 100
    deployed_pcts = []
    idle_pcts = []
    capital_by_strategy = {
        "momentum": 0.0,
        "day_trading": 0.0,
        "reversion": 0.0,
        "dca": 0.0,
    }
    for bucket in buckets:
        bucket_value = (
            bucket.assigned_capital_cents + bucket.realized_pnl_cents + bucket.unrealized_pnl_cents
        )
        if bucket_value <= 0:
            continue
        deployed = bucket.locked_capital_cents + bucket.reserved_cash_cents
        deployed_pcts.append(deployed / bucket_value * 100)
        idle_pcts.append(bucket.available_cash_cents / bucket_value * 100)
        capital_by_strategy[bucket.strategy_id] = round(
            capital_by_strategy.get(bucket.strategy_id, 0.0) + bucket.assigned_capital_cents / 100,
            6,
        )

    peak_capital_deployed_pct = max(deployed_pcts) if deployed_pcts else None
    if ledgers:
        for ledger in ledgers:
            denominator = max(ledger.after_available_cash_cents + ledger.after_reserved_cash_cents + ledger.after_locked_capital_cents, 1)
            peak_capital_deployed_pct = max(
                peak_capital_deployed_pct or 0,
                ((ledger.after_locked_capital_cents + ledger.after_reserved_cash_cents) / denominator) * 100,
            )

    return {
        "total_account_value": round(total_account_value, 6),
        "avg_capital_deployed_pct": _avg(deployed_pcts),
        "peak_capital_deployed_pct": round(peak_capital_deployed_pct, 6)
        if peak_capital_deployed_pct is not None
        else None,
        "avg_cash_idle_pct": _avg(idle_pcts),
        "capital_by_strategy": capital_by_strategy,
        "note": " ".join(note_parts) if note_parts else None,
    }


def _build_exit_analysis(trades: list[dict[str, Any]]) -> dict[str, Any]:
    if not trades:
        return {
            "most_common_exit_reason": None,
            "stop_loss_rate_pct": None,
            "avg_profit_before_trailing_exit_pct": None,
            "avg_profit_before_reversal_pct": None,
            "partial_take_profit_effectiveness_pct": None,
            "trades_that_were_positive_before_loss_pct": None,
        }

    reasons = Counter((trade["exit_reason"] or "unknown") for trade in trades)
    trailing_rows = [trade for trade in trades if _reason_contains(trade["exit_reason"], "trailing")]
    reversal_rows = [
        trade for trade in trades if _reason_contains(trade["exit_reason"], "bearish") or _reason_contains(trade["exit_reason"], "reversal")
    ]
    partial_rows = [trade for trade in trades if trade["partial_profit_taken"]]
    losing_rows = [trade for trade in trades if (trade["net_pnl_usd"] or 0) < 0]
    losing_positive_before_loss = [
        trade
        for trade in losing_rows
        if (trade["max_favorable_excursion_pct"] or 0) > 0
    ]

    return {
        "most_common_exit_reason": reasons.most_common(1)[0][0] if reasons else None,
        "stop_loss_rate_pct": _ratio_pct(_count_exit_reason(trades, "stop"), len(trades)),
        "avg_profit_before_trailing_exit_pct": _avg(
            [trade["max_favorable_excursion_pct"] for trade in trailing_rows]
        ),
        "avg_profit_before_reversal_pct": _avg(
            [trade["max_favorable_excursion_pct"] for trade in reversal_rows]
        ),
        "partial_take_profit_effectiveness_pct": _avg([trade["pnl_pct"] for trade in partial_rows]),
        "trades_that_were_positive_before_loss_pct": _ratio_pct(
            len(losing_positive_before_loss), len(losing_rows)
        ),
    }


def _build_active_strategy_config(db: Session, settings: Settings) -> dict[str, Any]:
    strategies = {
        row.slug: row.config_schema or {}
        for row in db.scalars(
            select(PlatformStrategy).where(
                PlatformStrategy.slug.in_(["momentum", "day_trading", "reversion", "dca"])
            )
        ).all()
    }
    token_export = TokenPolicyService(db).export_token_matrix()
    signal_rules = {
        slug: (config or {}).get("signal_rules", {})
        for slug, config in strategies.items()
    }
    return {
        "momentum_config": strategies.get("momentum"),
        "day_trading_config": strategies.get("day_trading"),
        "reversion_config": strategies.get("reversion"),
        "dca_config": strategies.get("dca"),
        "signal_rules": signal_rules,
        "token_strategy_policy_matrix": token_export["matrix"],
        "default_missing_policy_behavior": normalize_missing_policy_behavior(
            os.environ.get("TOKEN_STRATEGY_POLICY_DEFAULT_BEHAVIOR")
        ),
    }


def _apply_signal_filters(
    stmt: Select[Any],
    *,
    filters: TradingDiagnosticsFilters,
    window_start: datetime,
    token_column: Any,
    strategy_column: Any,
    mode_column: Any,
    timestamp_column: Any,
) -> Select[Any]:
    stmt = stmt.where(timestamp_column >= window_start)
    if filters.normalized_token:
        stmt = stmt.where(token_column == filters.normalized_token)
    if filters.normalized_strategy:
        stmt = stmt.where(strategy_column == filters.normalized_strategy)
    if filters.normalized_mode:
        stmt = stmt.where(mode_column == filters.normalized_mode)
    return stmt


def _load_rows(db: Session, stmt: Select[Any]) -> list[Any]:
    return list(db.scalars(stmt).all())


def _bucket_rejection_reason(reason: str | None) -> str:
    text = (reason or "").lower()
    if "confidence" in text:
        return "confidence"
    if "volume" in text:
        return "volume"
    if "allocation" in text or "position_limit" in text or "capital" in text:
        return "allocation"
    if "token_strategy_policy" in text or "blocked token" in text:
        return "token_strategy_policy"
    if "cooldown" in text:
        return "cooldown"
    if "liquid" in text or "hours" in text:
        return "liquidity_hours"
    if "risk" in text or "fee_economics" in text:
        return "risk_engine"
    return "other"


def _counter_or_none(counter: Counter[str], key: str, signals_rejected: int | None) -> int | None:
    if signals_rejected is None:
        return None
    return int(counter.get(key, 0))


def _aware_iso(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.isoformat()


def _float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    return round(float(Decimal(str(value))), 6)


def _pct(value: Any) -> float | None:
    if value is None:
        return None
    return round(float(Decimal(str(value)) * Decimal("100")), 6)


def _avg(values: list[float | None]) -> float | None:
    cleaned = [value for value in values if value is not None]
    if not cleaned:
        return None
    return round(sum(cleaned) / len(cleaned), 6)


def _ratio_pct(numerator: int, denominator: int) -> float | None:
    if denominator <= 0:
        return None
    return round(numerator / denominator * 100, 6)


def _trade_sort_key(row: dict[str, Any]) -> str:
    return row["exit_time"] or ""


def _count_exit_reason(trades: list[dict[str, Any]], needle: str) -> int:
    return sum(1 for trade in trades if _reason_contains(trade["exit_reason"], needle))


def _reason_contains(reason: str | None, needle: str) -> bool:
    return needle in (reason or "").lower()


def _max_drawdown_pct(pnls: list[float]) -> float | None:
    if not pnls:
        return None
    cumulative = 0.0
    peak = 0.0
    max_drawdown = 0.0
    for pnl in pnls:
        cumulative += pnl
        peak = max(peak, cumulative)
        max_drawdown = min(max_drawdown, cumulative - peak)
    if peak <= 0 and max_drawdown == 0:
        return 0.0
    denominator = peak if peak > 0 else 1.0
    return round(abs(max_drawdown) / denominator * 100, 6)
