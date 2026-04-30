from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any

from sqlalchemy import Select, func, select
from sqlalchemy.orm import Session

from oziebot_common.fee_model import (
    SETTING_EXECUTION_FEE_MODEL,
    calculate_round_trip_cost_bps,
    default_fee_model_settings,
    resolve_fee_profile,
)
from oziebot_api.models.execution import ExecutionOrder, ExecutionTradeRecord
from oziebot_api.models.platform_setting import PlatformSetting
from oziebot_api.models.platform_strategy import PlatformStrategy
from oziebot_api.models.risk_event import RiskEvent
from oziebot_api.models.strategy_signal_pipeline import StrategyRun, StrategySignalRecord
from oziebot_api.models.trade_intelligence import StrategySignalSnapshot, TradeOutcomeFeature

ANALYTICS_DATASET_ROW_LIMIT = 1000
# Summary endpoint (/analytics/summary) uses tighter caps and skips signal-snapshot joins
# so aggregates stay fast enough for ALB timeouts under load.
ANALYTICS_SUMMARY_ROW_LIMIT = 400
ANALYTICS_GROUP_ROW_LIMIT = 50
ANALYTICS_REJECTION_ROW_LIMIT = 25
ANALYTICS_COMPARISON_STRATEGY_LIMIT = 25
LIVE_EQUIVALENT_MAX_DAILY_LOSS_CENTS = 3_000
LIVE_EQUIVALENT_COOLDOWN_LOSS_COUNT = 3
LIVE_EQUIVALENT_COOLDOWN_MINUTES = 45
LIVE_EQUIVALENT_MAX_SPREAD_PCT = Decimal("0.012")
LIVE_EQUIVALENT_MAX_SLIPPAGE_PCT = Decimal("0.02")


def _json_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    return {}


def _as_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _to_decimal(value: Any) -> Decimal:
    if value in (None, ""):
        return Decimal("0")
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return Decimal("0")


def _to_float(value: Decimal | float | int) -> float:
    return float(value)


def _percent(numerator: int | float, denominator: int | float) -> float:
    if not denominator:
        return 0.0
    return round((float(numerator) / float(denominator)) * 100, 2)


def _avg_decimal(values: list[Decimal]) -> float:
    if not values:
        return 0.0
    return round(_to_float(sum(values, Decimal("0")) / Decimal(len(values))), 4)


def _avg_seconds_to_minutes(values: list[int]) -> float:
    if not values:
        return 0.0
    return round((sum(values) / len(values)) / 60, 2)


@dataclass(slots=True)
class AnalyticsFilters:
    user_id: Any
    trading_mode: str | None = None
    strategy_name: str | None = None
    symbol: str | None = None
    start_at: datetime | None = None
    end_at: datetime | None = None

    def matches(
        self,
        *,
        strategy_name: str | None,
        symbol: str | None,
        trading_mode: str | None,
        timestamp: datetime | None,
    ) -> bool:
        if self.trading_mode and trading_mode != self.trading_mode:
            return False
        if self.strategy_name and strategy_name != self.strategy_name:
            return False
        if self.symbol and symbol != self.symbol:
            return False
        if timestamp is None:
            return True
        current = _as_utc(timestamp)
        if current is None:
            return True
        if self.start_at and current < _as_utc(self.start_at):
            return False
        if self.end_at and current > _as_utc(self.end_at):
            return False
        return True


@dataclass(slots=True)
class AnalyticsBudgetState:
    dataset_row_limit: int
    group_row_limit: int
    rejection_row_limit: int
    comparison_strategy_limit: int
    datasets: dict[str, dict[str, Any]]
    sections: dict[str, dict[str, Any]]


class TradeReviewAnalyticsService:
    def __init__(self, db: Session):
        self._db = db
        self._budget = AnalyticsBudgetState(
            dataset_row_limit=ANALYTICS_DATASET_ROW_LIMIT,
            group_row_limit=ANALYTICS_GROUP_ROW_LIMIT,
            rejection_row_limit=ANALYTICS_REJECTION_ROW_LIMIT,
            comparison_strategy_limit=ANALYTICS_COMPARISON_STRATEGY_LIMIT,
            datasets={},
            sections={},
        )

    def build_overview(self, filters: AnalyticsFilters) -> dict[str, Any]:
        dataset = self._load_dataset(filters)
        strategy_rows = self._group_rows(dataset, grouping="strategy")
        token_rows = self._group_rows(dataset, grouping="token")
        pair_rows = self._group_rows(dataset, grouping="pair")
        return {
            "filters": self.filters_payload(filters),
            "summary": self._summary_payload(dataset),
            "budget": self.budget_payload(),
            "signalFunnel": [
                {
                    "strategyName": row["strategyName"],
                    "tradingMode": row["tradingMode"],
                    "evaluated": row["evaluated"],
                    "emitted": row["emitted"],
                    "suppressed": row["suppressed"],
                    "riskRejected": row["riskRejected"],
                    "executionRejected": row["executionRejected"],
                    "reduced": row["reduced"],
                    "rejected": row["rejected"],
                    "executed": row["executed"],
                    "closedProfitable": row["closedProfitable"],
                    "closedUnprofitable": row["closedUnprofitable"],
                    "profitable": row["profitable"],
                    "rejectionRatePct": row["rejectionRatePct"],
                    "executionRatePct": row["executionRatePct"],
                    "profitabilityRatePct": row["profitabilityRatePct"],
                    "overFilteringFlag": row["overFilteringFlag"],
                }
                for row in strategy_rows
            ],
            "strategyPerformance": strategy_rows,
            "tokenPerformance": token_rows,
            "pairPerformance": pair_rows,
            "rejectionBreakdown": self._rejection_breakdown(dataset),
            "paperLiveComparison": self._paper_live_comparison(self._comparison_dataset(filters)),
            "outcomes": self._outcome_rows(dataset),
            "paperLiveValidation": self._paper_live_validation(dataset),
            "availableStrategies": self._available_strategies(dataset),
            "availableSymbols": self._available_symbols(dataset),
        }

    def build_summary(self, filters: AnalyticsFilters) -> dict[str, Any]:
        dataset = self._load_summary_dataset(filters)
        return {
            "filters": self.filters_payload(filters),
            "summary": self._summary_payload(dataset),
            "budget": self.budget_payload(),
            "availableStrategies": self._available_strategies(dataset),
            "availableSymbols": self._available_symbols(dataset),
        }

    def build_strategy_rows(self, filters: AnalyticsFilters) -> list[dict[str, Any]]:
        return self._group_rows(self._load_dataset(filters), grouping="strategy")

    def build_token_rows(self, filters: AnalyticsFilters) -> list[dict[str, Any]]:
        return self._group_rows(self._load_dataset(filters), grouping="token")

    def build_pair_rows(self, filters: AnalyticsFilters) -> list[dict[str, Any]]:
        return self._group_rows(self._load_dataset(filters), grouping="pair")

    def build_rejection_breakdown(self, filters: AnalyticsFilters) -> dict[str, Any]:
        return self._rejection_breakdown(self._load_dataset(filters))

    def build_paper_live_comparison(self, filters: AnalyticsFilters) -> dict[str, Any]:
        return self._paper_live_comparison(self._comparison_dataset(filters))

    def build_outcome_rows(self, filters: AnalyticsFilters) -> list[dict[str, Any]]:
        return self._outcome_rows(self._load_dataset(filters))

    def build_paper_live_validation(self, filters: AnalyticsFilters) -> dict[str, Any]:
        return self._paper_live_validation(self._load_dataset(filters))

    def filters_payload(self, filters: AnalyticsFilters) -> dict[str, Any]:
        return {
            "tradingMode": filters.trading_mode or "all",
            "strategyName": filters.strategy_name,
            "symbol": filters.symbol,
            "startAt": _as_utc(filters.start_at).isoformat() if filters.start_at else None,
            "endAt": _as_utc(filters.end_at).isoformat() if filters.end_at else None,
        }

    def budget_payload(self) -> dict[str, Any]:
        truncated_datasets = sorted(
            key for key, value in self._budget.datasets.items() if bool(value.get("truncated"))
        )
        degraded_sections = sorted(
            key for key, value in self._budget.sections.items() if bool(value.get("degraded"))
        )
        return {
            "datasetRowLimit": self._budget.dataset_row_limit,
            "groupRowLimit": self._budget.group_row_limit,
            "rejectionRowLimit": self._budget.rejection_row_limit,
            "comparisonStrategyLimit": self._budget.comparison_strategy_limit,
            "datasets": self._budget.datasets,
            "sections": self._budget.sections,
            "truncatedDatasets": truncated_datasets,
            "degradedSections": degraded_sections,
            "degraded": bool(truncated_datasets or degraded_sections),
        }

    def _load_dataset(self, filters: AnalyticsFilters) -> dict[str, list[dict[str, Any]]]:
        return {
            "runs": self._load_runs(filters),
            "signals": self._load_signals(filters),
            "risk_events": self._load_risk_events(filters),
            "orders": self._load_orders(filters),
            "outcomes": self._load_outcomes(filters),
        }

    def _load_summary_dataset(self, filters: AnalyticsFilters) -> dict[str, list[dict[str, Any]]]:
        lim = ANALYTICS_SUMMARY_ROW_LIMIT
        return {
            "runs": self._load_runs(filters, row_limit=lim),
            "signals": self._load_signals(filters, row_limit=lim),
            "risk_events": self._load_risk_events(filters, row_limit=lim),
            "orders": self._load_orders(filters, row_limit=lim),
            "outcomes": self._load_outcomes(filters, row_limit=lim, include_signal_snapshot=False),
        }

    def _load_runs(
        self, filters: AnalyticsFilters, *, row_limit: int | None = None
    ) -> list[dict[str, Any]]:
        timestamp_expr = func.coalesce(StrategyRun.completed_at, StrategyRun.started_at)
        rows = self._limited_scalars(
            self._apply_filters(
                select(StrategyRun),
                filters=filters,
                strategy_column=StrategyRun.strategy_name,
                symbol_column=StrategyRun.symbol,
                trading_mode_column=StrategyRun.trading_mode,
                timestamp_column=timestamp_expr,
                user_column=StrategyRun.user_id,
            ).order_by(timestamp_expr.desc()),
            dataset_name="runs",
            row_limit=row_limit,
        )
        payload: list[dict[str, Any]] = []
        for row in rows:
            timestamp = row.completed_at or row.started_at
            metadata = _json_dict(row.run_metadata)
            payload.append(
                {
                    "strategy_name": row.strategy_name,
                    "symbol": row.symbol,
                    "trading_mode": row.trading_mode,
                    "status": row.status,
                    "timestamp": timestamp,
                    "suppressed": bool(metadata.get("suppressed")),
                    "suppression_reason": metadata.get("suppression_reason"),
                }
            )
        return payload

    def _load_signals(
        self, filters: AnalyticsFilters, *, row_limit: int | None = None
    ) -> list[dict[str, Any]]:
        rows = self._limited_scalars(
            self._apply_filters(
                select(StrategySignalRecord),
                filters=filters,
                strategy_column=StrategySignalRecord.strategy_name,
                symbol_column=StrategySignalRecord.symbol,
                trading_mode_column=StrategySignalRecord.trading_mode,
                timestamp_column=StrategySignalRecord.timestamp,
                user_column=StrategySignalRecord.user_id,
            ).order_by(StrategySignalRecord.timestamp.desc()),
            dataset_name="signals",
            row_limit=row_limit,
        )
        payload: list[dict[str, Any]] = []
        for row in rows:
            payload.append(
                {
                    "strategy_name": row.strategy_name,
                    "symbol": row.symbol,
                    "trading_mode": row.trading_mode,
                    "action": row.action.lower(),
                    "timestamp": row.timestamp,
                }
            )
        return payload

    def _load_risk_events(
        self, filters: AnalyticsFilters, *, row_limit: int | None = None
    ) -> list[dict[str, Any]]:
        rows = self._limited_scalars(
            self._apply_filters(
                select(RiskEvent),
                filters=filters,
                strategy_column=RiskEvent.strategy_name,
                symbol_column=RiskEvent.symbol,
                trading_mode_column=RiskEvent.trading_mode,
                timestamp_column=RiskEvent.created_at,
                user_column=RiskEvent.user_id,
            ).order_by(RiskEvent.created_at.desc()),
            dataset_name="risk_events",
            row_limit=row_limit,
        )
        payload: list[dict[str, Any]] = []
        for row in rows:
            payload.append(
                {
                    "strategy_name": row.strategy_name,
                    "symbol": row.symbol,
                    "trading_mode": row.trading_mode,
                    "outcome": (row.outcome or "").lower(),
                    "reason": row.reason,
                    "detail": row.detail,
                    "original_size": _to_decimal(row.original_size),
                    "final_size": _to_decimal(row.final_size),
                    "timestamp": row.created_at,
                }
            )
        return payload

    def _load_orders(
        self, filters: AnalyticsFilters, *, row_limit: int | None = None
    ) -> list[dict[str, Any]]:
        timestamp_expr = func.coalesce(
            ExecutionOrder.completed_at,
            ExecutionOrder.failed_at,
            ExecutionOrder.cancelled_at,
            ExecutionOrder.created_at,
        )
        rows = self._limited_scalars(
            self._apply_filters(
                select(ExecutionOrder),
                filters=filters,
                strategy_column=ExecutionOrder.strategy_id,
                symbol_column=ExecutionOrder.symbol,
                trading_mode_column=ExecutionOrder.trading_mode,
                timestamp_column=timestamp_expr,
                user_column=ExecutionOrder.user_id,
            ).order_by(timestamp_expr.desc()),
            dataset_name="orders",
            row_limit=row_limit,
        )
        payload: list[dict[str, Any]] = []
        for row in rows:
            timestamp = row.completed_at or row.failed_at or row.cancelled_at or row.created_at
            payload.append(
                {
                    "strategy_name": row.strategy_id,
                    "symbol": row.symbol,
                    "trading_mode": row.trading_mode,
                    "state": row.state,
                    "failure_code": row.failure_code,
                    "failure_detail": row.failure_detail,
                    "fees": Decimal(row.fees_cents or 0) / Decimal("100"),
                    "estimated_slippage_bps": Decimal(row.estimated_slippage_bps or 0),
                    "estimated_total_cost_bps": Decimal(row.estimated_total_cost_bps or 0),
                    "expected_net_edge_bps": Decimal(row.expected_net_edge_bps or 0),
                    "timestamp": timestamp,
                }
            )
        return payload

    def _load_outcomes(
        self,
        filters: AnalyticsFilters,
        *,
        row_limit: int | None = None,
        include_signal_snapshot: bool = True,
    ) -> list[dict[str, Any]]:
        if include_signal_snapshot:
            base_query = (
                select(
                    TradeOutcomeFeature,
                    ExecutionTradeRecord,
                    StrategySignalSnapshot,
                )
                .join(
                    ExecutionTradeRecord,
                    TradeOutcomeFeature.trade_id == ExecutionTradeRecord.id,
                )
                .join(
                    StrategySignalSnapshot,
                    TradeOutcomeFeature.signal_snapshot_id == StrategySignalSnapshot.id,
                    isouter=True,
                )
            )
        else:
            base_query = select(TradeOutcomeFeature, ExecutionTradeRecord).join(
                ExecutionTradeRecord,
                TradeOutcomeFeature.trade_id == ExecutionTradeRecord.id,
            )
        rows = self._limited_execute(
            self._apply_filters(
                base_query,
                filters=filters,
                strategy_column=TradeOutcomeFeature.strategy_name,
                symbol_column=TradeOutcomeFeature.token_symbol,
                trading_mode_column=TradeOutcomeFeature.trading_mode,
                timestamp_column=TradeOutcomeFeature.created_at,
                user_column=ExecutionTradeRecord.user_id,
            ).order_by(TradeOutcomeFeature.created_at.desc()),
            dataset_name="outcomes",
            row_limit=row_limit,
        )
        payload: list[dict[str, Any]] = []
        if include_signal_snapshot:
            for outcome, trade, snapshot in rows:
                self._append_outcome_payload_row(
                    payload,
                    outcome,
                    trade,
                    snapshot=snapshot,
                )
        else:
            for outcome, trade in rows:
                self._append_outcome_payload_row(payload, outcome, trade, snapshot=None)
        return payload

    def _append_outcome_payload_row(
        self,
        payload: list[dict[str, Any]],
        outcome: TradeOutcomeFeature,
        trade: ExecutionTradeRecord,
        *,
        snapshot: StrategySignalSnapshot | None,
    ) -> None:
        mfe_pct = _to_decimal(outcome.max_favorable_excursion_pct) * Decimal("100")
        mae_pct = _to_decimal(outcome.max_adverse_excursion_pct) * Decimal("100")
        realized_return_pct = _to_decimal(outcome.realized_return_pct) * Decimal("100")
        giveback_pct = _to_decimal(outcome.profit_giveback_pct) * Decimal("100")
        signal_timestamp = snapshot.timestamp if snapshot is not None else None
        signal_spread_pct = (
            _to_decimal(snapshot.spread_pct) if snapshot is not None else Decimal("0")
        )
        signal_slippage_pct = (
            _to_decimal(snapshot.estimated_slippage_pct) if snapshot is not None else Decimal("0")
        )
        confidence_score = (
            float(snapshot.confidence_score)
            if snapshot is not None and snapshot.confidence_score is not None
            else None
        )
        payload.append(
            {
                "outcome_id": str(outcome.id),
                "trade_id": str(outcome.trade_id) if outcome.trade_id else None,
                "signal_snapshot_id": (
                    str(outcome.signal_snapshot_id) if outcome.signal_snapshot_id else None
                ),
                "strategy_name": outcome.strategy_name,
                "symbol": outcome.token_symbol,
                "trading_mode": outcome.trading_mode,
                "side": trade.side,
                "entry_price": _to_decimal(outcome.entry_price),
                "exit_price": _to_decimal(outcome.exit_price),
                "filled_size": _to_decimal(outcome.filled_size),
                "fee_paid": _to_decimal(outcome.fee_paid),
                "slippage_realized": _to_decimal(outcome.slippage_realized),
                "hold_seconds": int(outcome.hold_seconds or 0),
                "realized_pnl": _to_decimal(outcome.realized_pnl),
                "realized_return_pct": realized_return_pct,
                "max_favorable_excursion_pct": mfe_pct,
                "max_adverse_excursion_pct": mae_pct,
                "profit_giveback_pct": giveback_pct,
                "partial_profit_taken": bool(outcome.partial_profit_taken),
                "remaining_position_outcome": outcome.remaining_position_outcome,
                "exit_reason": outcome.exit_reason,
                "win_loss_label": (outcome.win_loss_label or "").lower(),
                "profitable_after_fees_label": (outcome.profitable_after_fees_label or "").lower(),
                "signal_timestamp": signal_timestamp,
                "signal_spread_pct": signal_spread_pct * Decimal("100"),
                "signal_estimated_slippage_pct": signal_slippage_pct * Decimal("100"),
                "signal_confidence_score": confidence_score,
                "timestamp": outcome.created_at,
            }
        )

    def _effective_row_limit(self, row_limit: int | None) -> int:
        return row_limit if row_limit is not None else self._budget.dataset_row_limit

    def _limited_scalars(
        self,
        query: Select[Any],
        *,
        dataset_name: str,
        row_limit: int | None = None,
    ) -> list[Any]:
        eff = self._effective_row_limit(row_limit)
        rows = self._db.scalars(query.limit(eff + 1)).all()
        truncated = len(rows) > eff
        limited_rows = rows[:eff]
        self._budget.datasets[dataset_name] = {
            "limit": eff,
            "returned": len(limited_rows),
            "truncated": truncated,
        }
        return limited_rows

    def _limited_execute(
        self,
        query: Select[Any],
        *,
        dataset_name: str,
        row_limit: int | None = None,
    ) -> list[Any]:
        eff = self._effective_row_limit(row_limit)
        rows = self._db.execute(query.limit(eff + 1)).all()
        truncated = len(rows) > eff
        limited_rows = rows[:eff]
        self._budget.datasets[dataset_name] = {
            "limit": eff,
            "returned": len(limited_rows),
            "truncated": truncated,
        }
        return limited_rows

    def _apply_filters(
        self,
        query: Select[Any],
        *,
        filters: AnalyticsFilters,
        strategy_column,
        symbol_column,
        trading_mode_column,
        timestamp_column,
        user_column,
    ) -> Select[Any]:
        query = query.where(user_column == filters.user_id)
        if filters.trading_mode:
            query = query.where(trading_mode_column == filters.trading_mode)
        if filters.strategy_name:
            query = query.where(strategy_column == filters.strategy_name)
        if filters.symbol:
            query = query.where(symbol_column == filters.symbol)
        if filters.start_at:
            query = query.where(
                timestamp_column.is_not(None), timestamp_column >= _as_utc(filters.start_at)
            )
        if filters.end_at:
            query = query.where(
                timestamp_column.is_not(None), timestamp_column <= _as_utc(filters.end_at)
            )
        return query

    def _summary_payload(self, dataset: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
        runs = dataset["runs"]
        signals = [row for row in dataset["signals"] if row["action"] != "hold"]
        suppressed = [row for row in runs if row["suppressed"]]
        reduced = [row for row in dataset["risk_events"] if row["outcome"] == "reduce_size"]
        risk_rejected = [row for row in dataset["risk_events"] if row["outcome"] == "reject"]
        execution_rejected = [
            row for row in dataset["orders"] if row["state"] in {"failed", "cancelled"}
        ]
        rejected = [*suppressed, *risk_rejected, *execution_rejected]
        executed = [row for row in dataset["orders"] if row["state"] == "filled"]
        outcomes = dataset["outcomes"]
        profitable = [row for row in outcomes if row["profitable_after_fees_label"] == "profitable"]
        unprofitable = [
            row for row in outcomes if row["profitable_after_fees_label"] != "profitable"
        ]
        total_fees = sum((row["fee_paid"] for row in outcomes), Decimal("0"))
        slippages = [row["slippage_realized"] * Decimal("100") for row in outcomes]
        holds = [row["hold_seconds"] for row in outcomes if row["hold_seconds"] > 0]
        givebacks = [
            row["profit_giveback_pct"]
            for row in outcomes
            if row["max_favorable_excursion_pct"] > 0 or row["profit_giveback_pct"] != 0
        ]
        win_pnls = [row["realized_pnl"] for row in outcomes if row["realized_pnl"] > 0]
        loss_pnls = [row["realized_pnl"] for row in outcomes if row["realized_pnl"] < 0]
        return {
            "evaluated": len(runs),
            "emitted": len(signals),
            "suppressed": len(suppressed),
            "riskRejected": len(risk_rejected),
            "executionRejected": len(execution_rejected),
            "reduced": len(reduced),
            "rejected": len(rejected),
            "executed": len(executed),
            "tradeCount": len(outcomes),
            "closedProfitable": len(profitable),
            "closedUnprofitable": len(unprofitable),
            "profitable": len(profitable),
            "rejectionRatePct": _percent(len(rejected), len(runs)),
            "executionRatePct": _percent(len(executed), len(signals)),
            "profitabilityRatePct": _percent(len(profitable), len(outcomes)),
            "winRatePct": _percent(len(win_pnls), len(outcomes)),
            "avgWin": round(_avg_decimal(win_pnls), 2),
            "avgLoss": round(_avg_decimal(loss_pnls), 2),
            "totalRealizedPnl": round(
                _to_float(sum((row["realized_pnl"] for row in outcomes), Decimal("0"))), 2
            ),
            "totalFees": round(_to_float(total_fees), 2),
            "avgSlippagePct": round(_avg_decimal(slippages), 4),
            "avgHoldMinutes": _avg_seconds_to_minutes(holds),
            "avgGivebackPct": round(_avg_decimal(givebacks), 2),
            "maxDrawdownEstimate": self._max_drawdown_estimate(outcomes),
            "partialProfitCount": sum(
                1 for row in outcomes if row["exit_reason"] == "partial_take_profit"
            ),
            "stopLossCount": sum(1 for row in outcomes if row["exit_reason"] == "stop_loss"),
            "takeProfitCount": sum(1 for row in outcomes if row["exit_reason"] == "take_profit"),
            "trailingStopCount": sum(
                1 for row in outcomes if row["exit_reason"] == "trailing_stop"
            ),
            "maxAgeExitCount": sum(
                1
                for row in outcomes
                if str(row["exit_reason"] or "")
                in {"max_hold_time", "max_position_age_exceeded", "max_age_exit"}
            ),
            "overFilteringFlag": _percent(len(rejected), len(runs)) >= 55.0,
        }

    def _available_strategies(self, dataset: dict[str, list[dict[str, Any]]]) -> list[str]:
        strategies = {
            str(row["strategy_name"])
            for rows in dataset.values()
            for row in rows
            if row.get("strategy_name")
        }
        return sorted(strategies)

    def _available_symbols(self, dataset: dict[str, list[dict[str, Any]]]) -> list[str]:
        symbols = {
            str(row["symbol"]) for rows in dataset.values() for row in rows if row.get("symbol")
        }
        return sorted(symbols)

    def _comparison_dataset(self, filters: AnalyticsFilters) -> dict[str, list[dict[str, Any]]]:
        return self._load_dataset(
            AnalyticsFilters(
                user_id=filters.user_id,
                trading_mode=None,
                strategy_name=filters.strategy_name,
                symbol=filters.symbol,
                start_at=filters.start_at,
                end_at=filters.end_at,
            )
        )

    def _group_rows(
        self, dataset: dict[str, list[dict[str, Any]]], *, grouping: str
    ) -> list[dict[str, Any]]:
        groups: dict[tuple[Any, ...], dict[str, Any]] = {}
        for row in dataset["runs"]:
            key = self._group_key(
                grouping, row["strategy_name"], row["symbol"], row["trading_mode"]
            )
            entry = groups.setdefault(key, self._base_group_payload(row, grouping))
            entry["evaluated"] += 1
            if row["suppressed"]:
                entry["suppressed"] += 1
                entry["rejected"] += 1

        for row in dataset["signals"]:
            if row["action"] == "hold":
                continue
            key = self._group_key(
                grouping, row["strategy_name"], row["symbol"], row["trading_mode"]
            )
            entry = groups.setdefault(key, self._base_group_payload(row, grouping))
            entry["emitted"] += 1

        for row in dataset["risk_events"]:
            key = self._group_key(
                grouping, row["strategy_name"], row["symbol"], row["trading_mode"]
            )
            entry = groups.setdefault(key, self._base_group_payload(row, grouping))
            if row["outcome"] == "reduce_size":
                entry["reduced"] += 1
            elif row["outcome"] == "reject":
                entry["riskRejected"] += 1
                entry["rejected"] += 1

        for row in dataset["orders"]:
            key = self._group_key(
                grouping, row["strategy_name"], row["symbol"], row["trading_mode"]
            )
            entry = groups.setdefault(key, self._base_group_payload(row, grouping))
            if row["state"] == "filled":
                entry["executed"] += 1
            if row["state"] in {"failed", "cancelled"}:
                entry["executionRejected"] += 1
                entry["executionFailures"] += 1
                entry["rejected"] += 1

        for row in dataset["outcomes"]:
            key = self._group_key(
                grouping, row["strategy_name"], row["symbol"], row["trading_mode"]
            )
            entry = groups.setdefault(key, self._base_group_payload(row, grouping))
            entry["tradeCount"] += 1
            entry["totalRealizedPnl"] += row["realized_pnl"]
            entry["realizedReturns"].append(row["realized_return_pct"])
            entry["fees"].append(row["fee_paid"])
            entry["slippages"].append(row["slippage_realized"] * Decimal("100"))
            entry["givebacks"].append(row["profit_giveback_pct"])
            if row["hold_seconds"] > 0:
                entry["holds"].append(row["hold_seconds"])
            entry["timeline"].append(
                {"timestamp": row["timestamp"], "realized_pnl": row["realized_pnl"]}
            )
            if row["realized_pnl"] > 0:
                entry["wins"].append(row["realized_pnl"])
            elif row["realized_pnl"] < 0:
                entry["losses"].append(row["realized_pnl"])
            if row["profitable_after_fees_label"] == "profitable":
                entry["profitable"] += 1
                entry["closedProfitable"] += 1
            else:
                entry["closedUnprofitable"] += 1
            if row["exit_reason"] == "partial_take_profit":
                entry["partialProfitCount"] += 1
            elif row["exit_reason"] == "stop_loss":
                entry["stopLossCount"] += 1
            elif row["exit_reason"] == "take_profit":
                entry["takeProfitCount"] += 1
            elif row["exit_reason"] == "trailing_stop":
                entry["trailingStopCount"] += 1
            elif str(row["exit_reason"] or "") in {
                "max_hold_time",
                "max_position_age_exceeded",
                "max_age_exit",
            }:
                entry["maxAgeExitCount"] += 1

        rows: list[dict[str, Any]] = []
        for entry in groups.values():
            trade_count = int(entry["tradeCount"])
            evaluated = int(entry["evaluated"])
            emitted = int(entry["emitted"])
            rejected = int(entry["rejected"])
            executed = int(entry["executed"])
            profitable = int(entry["profitable"])
            total_realized_pnl = Decimal(entry["totalRealizedPnl"])
            avg_win = _avg_decimal(entry["wins"])
            avg_loss = _avg_decimal(entry["losses"])
            realized_return_pct = _avg_decimal(entry["realizedReturns"])
            total_fees = sum(entry["fees"], Decimal("0"))
            avg_fee = _avg_decimal(entry["fees"])
            avg_slippage_pct = _avg_decimal(entry["slippages"])
            avg_giveback_pct = _avg_decimal(entry["givebacks"])
            avg_hold_minutes = _avg_seconds_to_minutes(entry["holds"])
            win_rate_pct = _percent(len(entry["wins"]), trade_count)
            rejection_rate_pct = _percent(rejected, evaluated)
            execution_rate_pct = _percent(executed, emitted)
            profitability_rate_pct = _percent(profitable, trade_count)
            rows.append(
                {
                    "strategyName": entry["strategyName"],
                    "symbol": entry["symbol"],
                    "tradingMode": entry["tradingMode"],
                    "evaluated": evaluated,
                    "emitted": emitted,
                    "suppressed": int(entry["suppressed"]),
                    "riskRejected": int(entry["riskRejected"]),
                    "executionRejected": int(entry["executionRejected"]),
                    "reduced": int(entry["reduced"]),
                    "rejected": rejected,
                    "executed": executed,
                    "closedProfitable": int(entry["closedProfitable"]),
                    "closedUnprofitable": int(entry["closedUnprofitable"]),
                    "profitable": profitable,
                    "tradeCount": trade_count,
                    "winRatePct": win_rate_pct,
                    "avgWin": round(avg_win, 2),
                    "avgLoss": round(avg_loss, 2),
                    "realizedReturnPct": round(realized_return_pct, 2),
                    "totalRealizedPnl": round(_to_float(total_realized_pnl), 2),
                    "totalFees": round(_to_float(total_fees), 2),
                    "avgFeePerTrade": round(avg_fee, 2),
                    "avgSlippagePct": round(avg_slippage_pct, 4),
                    "avgGivebackPct": round(avg_giveback_pct, 2),
                    "avgHoldMinutes": avg_hold_minutes,
                    "maxDrawdownEstimate": self._max_drawdown_from_timeline(entry["timeline"]),
                    "partialProfitCount": int(entry["partialProfitCount"]),
                    "stopLossCount": int(entry["stopLossCount"]),
                    "takeProfitCount": int(entry["takeProfitCount"]),
                    "trailingStopCount": int(entry["trailingStopCount"]),
                    "maxAgeExitCount": int(entry["maxAgeExitCount"]),
                    "rejectionRatePct": rejection_rate_pct,
                    "executionRatePct": execution_rate_pct,
                    "profitabilityRatePct": profitability_rate_pct,
                    "executionFailures": int(entry["executionFailures"]),
                    "overFilteringFlag": rejection_rate_pct >= 55.0 and execution_rate_pct <= 60.0,
                    "needsReview": (
                        trade_count >= 3
                        and (win_rate_pct < 45.0 or _to_float(total_realized_pnl) < 0)
                    )
                    or (rejection_rate_pct >= 60.0 and evaluated >= 5),
                }
            )
        sorted_rows = sorted(
            rows,
            key=lambda row: (
                row["tradingMode"],
                row["strategyName"] or "",
                row["symbol"] or "",
            ),
        )
        section_name = {
            "strategy": "strategyPerformance",
            "token": "tokenPerformance",
            "pair": "pairPerformance",
        }[grouping]
        limited_rows = sorted_rows[: self._budget.group_row_limit]
        self._budget.sections[section_name] = {
            "limit": self._budget.group_row_limit,
            "returned": len(limited_rows),
            "degraded": len(sorted_rows) > self._budget.group_row_limit,
        }
        return limited_rows

    def _group_key(
        self,
        grouping: str,
        strategy_name: str | None,
        symbol: str | None,
        trading_mode: str | None,
    ) -> tuple[Any, ...]:
        if grouping == "strategy":
            return (strategy_name, trading_mode)
        if grouping == "token":
            return (symbol, trading_mode)
        return (strategy_name, symbol, trading_mode)

    def _base_group_payload(self, row: dict[str, Any], grouping: str) -> dict[str, Any]:
        strategy_name = row["strategy_name"] if grouping != "token" else None
        symbol = row["symbol"] if grouping != "strategy" else None
        return {
            "strategyName": strategy_name,
            "symbol": symbol,
            "tradingMode": row["trading_mode"],
            "evaluated": 0,
            "emitted": 0,
            "suppressed": 0,
            "riskRejected": 0,
            "executionRejected": 0,
            "reduced": 0,
            "rejected": 0,
            "executed": 0,
            "closedProfitable": 0,
            "closedUnprofitable": 0,
            "profitable": 0,
            "tradeCount": 0,
            "totalRealizedPnl": Decimal("0"),
            "wins": [],
            "losses": [],
            "realizedReturns": [],
            "fees": [],
            "slippages": [],
            "givebacks": [],
            "holds": [],
            "timeline": [],
            "partialProfitCount": 0,
            "stopLossCount": 0,
            "takeProfitCount": 0,
            "trailingStopCount": 0,
            "maxAgeExitCount": 0,
            "executionFailures": 0,
        }

    def _max_drawdown_estimate(self, outcomes: list[dict[str, Any]]) -> float:
        timeline = [
            {"timestamp": row["timestamp"], "realized_pnl": row["realized_pnl"]} for row in outcomes
        ]
        return self._max_drawdown_from_timeline(timeline)

    def _max_drawdown_from_timeline(self, timeline: list[dict[str, Any]]) -> float:
        if not timeline:
            return 0.0
        equity = Decimal("0")
        peak = Decimal("0")
        max_drawdown = Decimal("0")
        for row in sorted(
            timeline,
            key=lambda item: _as_utc(item.get("timestamp")) or datetime.min.replace(tzinfo=UTC),
        ):
            equity += _to_decimal(row.get("realized_pnl"))
            if equity > peak:
                peak = equity
            drawdown = peak - equity
            if drawdown > max_drawdown:
                max_drawdown = drawdown
        return round(_to_float(max_drawdown), 2)

    def _outcome_rows(self, dataset: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
        rows = [
            {
                "outcomeId": row["outcome_id"],
                "tradeId": row["trade_id"],
                "strategyName": row["strategy_name"],
                "symbol": row["symbol"],
                "tradingMode": row["trading_mode"],
                "timestamp": row["timestamp"],
                "holdMinutes": round(row["hold_seconds"] / 60, 2),
                "realizedPnl": round(_to_float(row["realized_pnl"]), 2),
                "realizedReturnPct": round(_to_float(row["realized_return_pct"]), 2),
                "maxFavorableExcursionPct": round(_to_float(row["max_favorable_excursion_pct"]), 2),
                "maxAdverseExcursionPct": round(_to_float(row["max_adverse_excursion_pct"]), 2),
                "profitGivebackPct": round(_to_float(row["profit_giveback_pct"]), 2),
                "partialProfitTaken": bool(row["partial_profit_taken"]),
                "remainingPositionOutcome": row["remaining_position_outcome"],
                "exitReason": row["exit_reason"],
                "entryPrice": round(_to_float(row["entry_price"]), 8),
                "exitPrice": round(_to_float(row["exit_price"]), 8),
                "filledSize": round(_to_float(row["filled_size"]), 8),
            }
            for row in dataset["outcomes"]
        ]
        limited_rows = rows[: self._budget.group_row_limit]
        self._budget.sections["outcomes"] = {
            "limit": self._budget.group_row_limit,
            "returned": len(limited_rows),
            "degraded": len(rows) > self._budget.group_row_limit,
        }
        return limited_rows

    def _paper_live_validation(self, dataset: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
        paper_outcomes = [row for row in dataset["outcomes"] if row["trading_mode"] == "paper"]
        strategy_configs = self._platform_strategy_configs()
        fee_settings = self._execution_fee_settings()
        rows: list[dict[str, Any]] = []
        for row in paper_outcomes:
            reasons = self._live_equivalent_rejections(
                outcome=row,
                all_outcomes=paper_outcomes,
                strategy_configs=strategy_configs,
                fee_settings=fee_settings,
            )
            rows.append(
                {
                    "outcomeId": row["outcome_id"],
                    "strategyName": row["strategy_name"],
                    "symbol": row["symbol"],
                    "signalTimestamp": row["signal_timestamp"],
                    "executedAt": row["timestamp"],
                    "realizedPnl": round(_to_float(row["realized_pnl"]), 2),
                    "realizedReturnPct": round(_to_float(row["realized_return_pct"]), 2),
                    "profitGivebackPct": round(_to_float(row["profit_giveback_pct"]), 2),
                    "liveEquivalentRejected": bool(reasons),
                    "rejectedReasonCodes": reasons,
                    "signalSpreadPct": round(_to_float(row["signal_spread_pct"]), 3),
                    "signalEstimatedSlippagePct": round(
                        _to_float(row["signal_estimated_slippage_pct"]), 3
                    ),
                }
            )
        limited_rows = rows[: self._budget.group_row_limit]
        rejected_rows = [row for row in rows if row["liveEquivalentRejected"]]
        self._budget.sections["paperLiveValidation"] = {
            "limit": self._budget.group_row_limit,
            "returned": len(limited_rows),
            "degraded": len(rows) > self._budget.group_row_limit,
        }
        by_reason: defaultdict[str, int] = defaultdict(int)
        for row in rejected_rows:
            for reason in row["rejectedReasonCodes"]:
                by_reason[reason] += 1
        return {
            "overview": {
                "paperTradesReviewed": len(rows),
                "wouldPassLiveEquivalent": len(rows) - len(rejected_rows),
                "wouldRejectLiveEquivalent": len(rejected_rows),
                "rejectionRatePct": _percent(len(rejected_rows), len(rows)),
            },
            "reasonBreakdown": [
                {"reasonCode": reason, "count": count}
                for reason, count in sorted(by_reason.items(), key=lambda item: (-item[1], item[0]))
            ],
            "rows": limited_rows,
        }

    def _platform_strategy_configs(self) -> dict[str, dict[str, Any]]:
        rows = self._db.scalars(select(PlatformStrategy)).all()
        return {row.slug: _json_dict(row.config_schema) for row in rows}

    def _execution_fee_settings(self) -> dict[str, Any]:
        row = self._db.get(PlatformSetting, SETTING_EXECUTION_FEE_MODEL)
        return _json_dict(row.value if row is not None else default_fee_model_settings())

    def _live_equivalent_rejections(
        self,
        *,
        outcome: dict[str, Any],
        all_outcomes: list[dict[str, Any]],
        strategy_configs: dict[str, dict[str, Any]],
        fee_settings: dict[str, Any],
    ) -> list[str]:
        signal_timestamp = _as_utc(outcome.get("signal_timestamp"))
        if signal_timestamp is None:
            return ["missing_signal_snapshot"]
        strategy_config = strategy_configs.get(str(outcome["strategy_name"]), {})
        risk_caps = _json_dict(strategy_config.get("risk_caps"))
        reasons: list[str] = []
        daily_loss_cents = self._paper_daily_loss_cents_before(
            all_outcomes=all_outcomes, before=signal_timestamp
        )
        if daily_loss_cents >= LIVE_EQUIVALENT_MAX_DAILY_LOSS_CENTS:
            reasons.append("max_daily_loss")
        loss_threshold = int(
            risk_caps.get("max_consecutive_losses") or LIVE_EQUIVALENT_COOLDOWN_LOSS_COUNT
        )
        cooldown_minutes = int(
            risk_caps.get("loss_cooldown_minutes") or LIVE_EQUIVALENT_COOLDOWN_MINUTES
        )
        if self._paper_strategy_cooldown_active(
            all_outcomes=all_outcomes,
            strategy_name=str(outcome["strategy_name"]),
            before=signal_timestamp,
            loss_threshold=loss_threshold,
            cooldown_minutes=cooldown_minutes,
        ):
            reasons.append("cooldown_after_losses")

        fee_profile = resolve_fee_profile(
            fee_settings,
            trading_mode="live",
            strategy_id=str(outcome["strategy_name"]),
            symbol=str(outcome["symbol"]),
        )
        notional = _to_decimal(outcome["filled_size"]) * _to_decimal(outcome["entry_price"])
        signal_spread_pct = _to_decimal(outcome["signal_spread_pct"]) / Decimal("100")
        signal_slippage_pct = _to_decimal(outcome["signal_estimated_slippage_pct"]) / Decimal("100")
        estimated_slippage_bps = int(
            (
                signal_slippage_pct * Decimal("10000")
                if signal_slippage_pct > 0
                else _to_decimal(fee_profile.get("estimated_slippage_bps", 0))
            ).quantize(Decimal("1"))
        )
        estimated_total_cost_bps = calculate_round_trip_cost_bps(
            fee_profile.get("entry_fill_type", "maker"),
            fee_profile.get("exit_fill_type", "taker"),
            estimated_slippage_bps,
            fee_profile.get("spread_buffer_bps", 0),
            fee_profile.get("safety_buffer_bps", 0),
            fee_profile.get("coinbase_one_rebate_percent", 0),
            maker_fee_bps=fee_profile.get("maker_fee_bps", 0),
            taker_fee_bps=fee_profile.get("taker_fee_bps", 0),
        )
        expected_gross_edge_bps = int(
            _to_decimal(fee_profile.get("expected_gross_edge_bps", 0)).quantize(Decimal("1"))
        )
        expected_net_edge_bps = expected_gross_edge_bps - estimated_total_cost_bps
        expected_gross_profit = notional * (Decimal(expected_gross_edge_bps) / Decimal("10000"))
        estimated_cost = notional * (Decimal(estimated_total_cost_bps) / Decimal("10000"))
        expected_net_profit = expected_gross_profit - estimated_cost
        allowed_cost = expected_gross_profit * _to_decimal(
            fee_profile.get("max_fee_percent_of_expected_profit", 0)
        )
        if signal_spread_pct > LIVE_EQUIVALENT_MAX_SPREAD_PCT:
            reasons.append("execution_quality_spread")
        if signal_slippage_pct > LIVE_EQUIVALENT_MAX_SLIPPAGE_PCT:
            reasons.append("execution_quality_slippage")
        min_notional = _to_decimal(fee_profile.get("min_notional_per_trade", 0))
        if min_notional > 0 and notional < min_notional:
            reasons.append("fee_economics_min_notional")
        max_slippage_bps = int(_to_decimal(fee_profile.get("max_slippage_bps", 0)))
        if max_slippage_bps > 0 and estimated_slippage_bps > max_slippage_bps:
            reasons.append("fee_economics_slippage_cap")
        min_expected_edge_bps = int(_to_decimal(fee_profile.get("min_expected_edge_bps", 0)))
        if expected_net_edge_bps < min_expected_edge_bps:
            reasons.append("fee_economics_min_edge")
        min_expected_net_profit = _to_decimal(fee_profile.get("min_expected_net_profit_dollars", 0))
        if min_expected_net_profit > 0 and expected_net_profit < min_expected_net_profit:
            reasons.append("fee_economics_min_net_profit")
        if (
            expected_gross_profit > 0
            and _to_decimal(fee_profile.get("max_fee_percent_of_expected_profit", 0)) > 0
            and estimated_cost > allowed_cost
        ):
            reasons.append("fee_economics_cost_ratio")
        return reasons

    def _paper_daily_loss_cents_before(
        self, *, all_outcomes: list[dict[str, Any]], before: datetime
    ) -> int:
        day_start = before.replace(hour=0, minute=0, second=0, microsecond=0)
        total = Decimal("0")
        for row in all_outcomes:
            timestamp = _as_utc(row.get("timestamp"))
            if timestamp is None or timestamp < day_start or timestamp >= before:
                continue
            pnl = _to_decimal(row["realized_pnl"])
            if pnl < 0:
                total += abs(pnl)
        return int((total * Decimal("100")).quantize(Decimal("1")))

    def _paper_strategy_cooldown_active(
        self,
        *,
        all_outcomes: list[dict[str, Any]],
        strategy_name: str,
        before: datetime,
        loss_threshold: int,
        cooldown_minutes: int,
    ) -> bool:
        recent_losses: list[dict[str, Any]] = []
        ordered = sorted(
            (
                row
                for row in all_outcomes
                if row["strategy_name"] == strategy_name
                and (_as_utc(row.get("timestamp")) or before) < before
            ),
            key=lambda row: _as_utc(row.get("timestamp")) or datetime.min.replace(tzinfo=UTC),
            reverse=True,
        )
        for row in ordered:
            pnl = _to_decimal(row["realized_pnl"])
            if pnl >= 0:
                break
            recent_losses.append(row)
        if len(recent_losses) < loss_threshold or not recent_losses:
            return False
        latest_loss_at = _as_utc(recent_losses[0].get("timestamp"))
        if latest_loss_at is None:
            return False
        return before < latest_loss_at + timedelta(minutes=cooldown_minutes)

    def _rejection_breakdown(self, dataset: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
        breakdown: dict[tuple[str, str], dict[str, Any]] = {}
        stage_counts: defaultdict[str, int] = defaultdict(int)

        def record(stage: str, reason_code: str, strategy: str | None, symbol: str | None) -> None:
            stage_counts[stage] += 1
            key = (stage, reason_code)
            entry = breakdown.setdefault(
                key,
                {
                    "stage": stage,
                    "reasonCode": reason_code,
                    "count": 0,
                    "strategies": set(),
                    "symbols": set(),
                },
            )
            entry["count"] += 1
            if strategy:
                entry["strategies"].add(strategy)
            if symbol:
                entry["symbols"].add(symbol)

        for row in dataset["runs"]:
            if row["suppressed"]:
                record(
                    "suppression",
                    str(row["suppression_reason"] or "unspecified"),
                    row["strategy_name"],
                    row["symbol"],
                )

        for row in dataset["risk_events"]:
            if row["outcome"] == "reject":
                record(
                    "risk", str(row["reason"] or "unspecified"), row["strategy_name"], row["symbol"]
                )

        for row in dataset["orders"]:
            if row["state"] in {"failed", "cancelled"}:
                record(
                    "execution",
                    str(row["failure_code"] or row["state"] or "unspecified"),
                    row["strategy_name"],
                    row["symbol"],
                )

        breakdown_rows = sorted(
            [
                {
                    "stage": row["stage"],
                    "reasonCode": row["reasonCode"],
                    "count": row["count"],
                    "strategies": sorted(row["strategies"]),
                    "symbols": sorted(row["symbols"]),
                }
                for row in breakdown.values()
            ],
            key=lambda row: (-row["count"], row["stage"], row["reasonCode"]),
        )
        by_stage = [
            {"stage": stage, "count": count}
            for stage, count in sorted(stage_counts.items(), key=lambda item: (-item[1], item[0]))
        ]
        rows = breakdown_rows[: self._budget.rejection_row_limit]
        self._budget.sections["rejectionBreakdown"] = {
            "limit": self._budget.rejection_row_limit,
            "returned": len(rows),
            "degraded": len(breakdown_rows) > self._budget.rejection_row_limit,
        }
        return {
            "totalRejected": sum(stage_counts.values()),
            "byStage": by_stage,
            "rows": rows,
        }

    def _paper_live_comparison(self, dataset: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
        strategy_rows = self._group_rows(dataset, grouping="strategy")
        mode_overview = self._group_rows(dataset, grouping="pair")
        by_mode: dict[str, dict[str, Any]] = {}
        for row in mode_overview:
            mode = row["tradingMode"]
            entry = by_mode.setdefault(
                mode,
                {
                    "tradingMode": mode,
                    "evaluated": 0,
                    "emitted": 0,
                    "reduced": 0,
                    "rejected": 0,
                    "executed": 0,
                    "profitable": 0,
                    "tradeCount": 0,
                    "totalRealizedPnl": 0.0,
                    "totalFees": 0.0,
                    "avgSlippagePctValues": [],
                    "avgHoldMinutesValues": [],
                    "winRatePctValues": [],
                },
            )
            entry["evaluated"] += row["evaluated"]
            entry["emitted"] += row["emitted"]
            entry["reduced"] += row["reduced"]
            entry["rejected"] += row["rejected"]
            entry["executed"] += row["executed"]
            entry["profitable"] += row["profitable"]
            entry["tradeCount"] += row["tradeCount"]
            entry["totalRealizedPnl"] += row["totalRealizedPnl"]
            entry["totalFees"] += row["totalFees"]
            entry["avgSlippagePctValues"].append(row["avgSlippagePct"])
            entry["avgHoldMinutesValues"].append(row["avgHoldMinutes"])
            entry["winRatePctValues"].append(row["winRatePct"])

        overview = []
        for row in by_mode.values():
            overview.append(
                {
                    "tradingMode": row["tradingMode"],
                    "evaluated": row["evaluated"],
                    "emitted": row["emitted"],
                    "reduced": row["reduced"],
                    "rejected": row["rejected"],
                    "executed": row["executed"],
                    "profitable": row["profitable"],
                    "tradeCount": row["tradeCount"],
                    "totalRealizedPnl": round(row["totalRealizedPnl"], 2),
                    "totalFees": round(row["totalFees"], 2),
                    "winRatePct": round(
                        sum(row["winRatePctValues"]) / max(1, len(row["winRatePctValues"])), 2
                    ),
                    "avgSlippagePct": round(
                        sum(row["avgSlippagePctValues"]) / max(1, len(row["avgSlippagePctValues"])),
                        4,
                    ),
                    "avgHoldMinutes": round(
                        sum(row["avgHoldMinutesValues"]) / max(1, len(row["avgHoldMinutesValues"])),
                        2,
                    ),
                }
            )

        strategy_modes: defaultdict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
        for row in strategy_rows:
            strategy_modes[str(row["strategyName"] or "")][row["tradingMode"]] = row

        strategy_comparison: list[dict[str, Any]] = []
        for strategy_name, row_map in sorted(strategy_modes.items()):
            paper = row_map.get("paper")
            live = row_map.get("live")
            strategy_comparison.append(
                {
                    "strategyName": strategy_name,
                    "paper": paper,
                    "live": live,
                    "deltas": {
                        "winRatePct": round(
                            float((live or {}).get("winRatePct", 0.0))
                            - float((paper or {}).get("winRatePct", 0.0)),
                            2,
                        ),
                        "realizedReturnPct": round(
                            float((live or {}).get("realizedReturnPct", 0.0))
                            - float((paper or {}).get("realizedReturnPct", 0.0)),
                            2,
                        ),
                        "totalFees": round(
                            float((live or {}).get("totalFees", 0.0))
                            - float((paper or {}).get("totalFees", 0.0)),
                            2,
                        ),
                        "avgSlippagePct": round(
                            float((live or {}).get("avgSlippagePct", 0.0))
                            - float((paper or {}).get("avgSlippagePct", 0.0)),
                            4,
                        ),
                    },
                }
            )
        strategies = strategy_comparison[: self._budget.comparison_strategy_limit]
        self._budget.sections["paperLiveComparison"] = {
            "limit": self._budget.comparison_strategy_limit,
            "returned": len(strategies),
            "degraded": len(strategy_comparison) > self._budget.comparison_strategy_limit,
        }
        return {
            "overview": sorted(overview, key=lambda row: row["tradingMode"]),
            "strategies": strategies,
        }
