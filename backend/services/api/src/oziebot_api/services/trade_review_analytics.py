from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from sqlalchemy import Select, func, select
from sqlalchemy.orm import Session

from oziebot_api.models.execution import ExecutionOrder, ExecutionTradeRecord
from oziebot_api.models.risk_event import RiskEvent
from oziebot_api.models.strategy_signal_pipeline import StrategyRun, StrategySignalRecord
from oziebot_api.models.trade_intelligence import TradeOutcomeFeature


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


class TradeReviewAnalyticsService:
    def __init__(self, db: Session):
        self._db = db

    def build_overview(self, filters: AnalyticsFilters) -> dict[str, Any]:
        dataset = self._load_dataset(filters)
        comparison_dataset = self._load_dataset(
            AnalyticsFilters(
                user_id=filters.user_id,
                trading_mode=None,
                strategy_name=filters.strategy_name,
                symbol=filters.symbol,
                start_at=filters.start_at,
                end_at=filters.end_at,
            )
        )
        strategy_rows = self._group_rows(dataset, grouping="strategy")
        token_rows = self._group_rows(dataset, grouping="token")
        pair_rows = self._group_rows(dataset, grouping="pair")
        return {
            "filters": self.filters_payload(filters),
            "summary": self._summary_payload(dataset),
            "signalFunnel": [
                {
                    "strategyName": row["strategyName"],
                    "tradingMode": row["tradingMode"],
                    "evaluated": row["evaluated"],
                    "emitted": row["emitted"],
                    "reduced": row["reduced"],
                    "rejected": row["rejected"],
                    "executed": row["executed"],
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
            "paperLiveComparison": self._paper_live_comparison(comparison_dataset),
            "availableStrategies": sorted(
                {row["strategyName"] for row in strategy_rows if row["strategyName"]}
            ),
            "availableSymbols": sorted({row["symbol"] for row in pair_rows if row["symbol"]}),
        }

    def build_strategy_rows(self, filters: AnalyticsFilters) -> list[dict[str, Any]]:
        return self._group_rows(self._load_dataset(filters), grouping="strategy")

    def build_token_rows(self, filters: AnalyticsFilters) -> list[dict[str, Any]]:
        return self._group_rows(self._load_dataset(filters), grouping="token")

    def build_pair_rows(self, filters: AnalyticsFilters) -> list[dict[str, Any]]:
        return self._group_rows(self._load_dataset(filters), grouping="pair")

    def filters_payload(self, filters: AnalyticsFilters) -> dict[str, Any]:
        return {
            "tradingMode": filters.trading_mode or "all",
            "strategyName": filters.strategy_name,
            "symbol": filters.symbol,
            "startAt": _as_utc(filters.start_at).isoformat() if filters.start_at else None,
            "endAt": _as_utc(filters.end_at).isoformat() if filters.end_at else None,
        }

    def _load_dataset(self, filters: AnalyticsFilters) -> dict[str, list[dict[str, Any]]]:
        return {
            "runs": self._load_runs(filters),
            "signals": self._load_signals(filters),
            "risk_events": self._load_risk_events(filters),
            "orders": self._load_orders(filters),
            "outcomes": self._load_outcomes(filters),
        }

    def _load_runs(self, filters: AnalyticsFilters) -> list[dict[str, Any]]:
        timestamp_expr = func.coalesce(StrategyRun.completed_at, StrategyRun.started_at)
        rows = self._db.scalars(
            self._apply_filters(
                select(StrategyRun),
                filters=filters,
                strategy_column=StrategyRun.strategy_name,
                symbol_column=StrategyRun.symbol,
                trading_mode_column=StrategyRun.trading_mode,
                timestamp_column=timestamp_expr,
                user_column=StrategyRun.user_id,
            )
        ).all()
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

    def _load_signals(self, filters: AnalyticsFilters) -> list[dict[str, Any]]:
        rows = self._db.scalars(
            self._apply_filters(
                select(StrategySignalRecord),
                filters=filters,
                strategy_column=StrategySignalRecord.strategy_name,
                symbol_column=StrategySignalRecord.symbol,
                trading_mode_column=StrategySignalRecord.trading_mode,
                timestamp_column=StrategySignalRecord.timestamp,
                user_column=StrategySignalRecord.user_id,
            )
        ).all()
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

    def _load_risk_events(self, filters: AnalyticsFilters) -> list[dict[str, Any]]:
        rows = self._db.scalars(
            self._apply_filters(
                select(RiskEvent),
                filters=filters,
                strategy_column=RiskEvent.strategy_name,
                symbol_column=RiskEvent.symbol,
                trading_mode_column=RiskEvent.trading_mode,
                timestamp_column=RiskEvent.created_at,
                user_column=RiskEvent.user_id,
            )
        ).all()
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

    def _load_orders(self, filters: AnalyticsFilters) -> list[dict[str, Any]]:
        timestamp_expr = func.coalesce(
            ExecutionOrder.completed_at,
            ExecutionOrder.failed_at,
            ExecutionOrder.cancelled_at,
            ExecutionOrder.created_at,
        )
        rows = self._db.scalars(
            self._apply_filters(
                select(ExecutionOrder),
                filters=filters,
                strategy_column=ExecutionOrder.strategy_id,
                symbol_column=ExecutionOrder.symbol,
                trading_mode_column=ExecutionOrder.trading_mode,
                timestamp_column=timestamp_expr,
                user_column=ExecutionOrder.user_id,
            )
        ).all()
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

    def _load_outcomes(self, filters: AnalyticsFilters) -> list[dict[str, Any]]:
        rows = self._db.execute(
            self._apply_filters(
                select(TradeOutcomeFeature, ExecutionTradeRecord).join(
                    ExecutionTradeRecord,
                    TradeOutcomeFeature.trade_id == ExecutionTradeRecord.id,
                ),
                filters=filters,
                strategy_column=TradeOutcomeFeature.strategy_name,
                symbol_column=TradeOutcomeFeature.token_symbol,
                trading_mode_column=TradeOutcomeFeature.trading_mode,
                timestamp_column=TradeOutcomeFeature.created_at,
                user_column=ExecutionTradeRecord.user_id,
            )
        ).all()
        payload: list[dict[str, Any]] = []
        for outcome, _trade in rows:
            payload.append(
                {
                    "strategy_name": outcome.strategy_name,
                    "symbol": outcome.token_symbol,
                    "trading_mode": outcome.trading_mode,
                    "fee_paid": _to_decimal(outcome.fee_paid),
                    "slippage_realized": _to_decimal(outcome.slippage_realized),
                    "hold_seconds": int(outcome.hold_seconds or 0),
                    "realized_pnl": _to_decimal(outcome.realized_pnl),
                    "realized_return_pct": _to_decimal(outcome.realized_return_pct)
                    * Decimal("100"),
                    "win_loss_label": (outcome.win_loss_label or "").lower(),
                    "profitable_after_fees_label": (
                        outcome.profitable_after_fees_label or ""
                    ).lower(),
                    "timestamp": outcome.created_at,
                }
            )
        return payload

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
                (timestamp_column.is_(None)) | (timestamp_column >= _as_utc(filters.start_at))
            )
        if filters.end_at:
            query = query.where(
                (timestamp_column.is_(None)) | (timestamp_column <= _as_utc(filters.end_at))
            )
        return query

    def _summary_payload(self, dataset: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
        runs = dataset["runs"]
        signals = [row for row in dataset["signals"] if row["action"] != "hold"]
        reduced = [row for row in dataset["risk_events"] if row["outcome"] == "reduce_size"]
        rejected = [
            *[row for row in runs if row["suppressed"]],
            *[row for row in dataset["risk_events"] if row["outcome"] == "reject"],
        ]
        executed = [row for row in dataset["orders"] if row["state"] == "filled"]
        outcomes = dataset["outcomes"]
        profitable = [row for row in outcomes if row["profitable_after_fees_label"] == "profitable"]
        total_fees = sum((row["fee_paid"] for row in outcomes), Decimal("0"))
        slippages = [row["slippage_realized"] * Decimal("100") for row in outcomes]
        holds = [row["hold_seconds"] for row in outcomes if row["hold_seconds"] > 0]
        return {
            "evaluated": len(runs),
            "emitted": len(signals),
            "reduced": len(reduced),
            "rejected": len(rejected),
            "executed": len(executed),
            "profitable": len(profitable),
            "rejectionRatePct": _percent(len(rejected), len(runs)),
            "executionRatePct": _percent(len(executed), len(signals)),
            "profitabilityRatePct": _percent(len(profitable), len(outcomes)),
            "totalRealizedPnl": round(
                _to_float(sum((row["realized_pnl"] for row in outcomes), Decimal("0"))), 2
            ),
            "totalFees": round(_to_float(total_fees), 2),
            "avgSlippagePct": round(_avg_decimal(slippages), 4),
            "avgHoldMinutes": _avg_seconds_to_minutes(holds),
            "overFilteringFlag": _percent(len(rejected), len(runs)) >= 55.0,
        }

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
                entry["rejected"] += 1

        for row in dataset["orders"]:
            key = self._group_key(
                grouping, row["strategy_name"], row["symbol"], row["trading_mode"]
            )
            entry = groups.setdefault(key, self._base_group_payload(row, grouping))
            if row["state"] == "filled":
                entry["executed"] += 1
            if row["state"] in {"failed", "cancelled"}:
                entry["executionFailures"] += 1

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
            if row["hold_seconds"] > 0:
                entry["holds"].append(row["hold_seconds"])
            if row["realized_pnl"] > 0:
                entry["wins"].append(row["realized_pnl"])
            elif row["realized_pnl"] < 0:
                entry["losses"].append(row["realized_pnl"])
            if row["profitable_after_fees_label"] == "profitable":
                entry["profitable"] += 1

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
                    "reduced": int(entry["reduced"]),
                    "rejected": rejected,
                    "executed": executed,
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
                    "avgHoldMinutes": avg_hold_minutes,
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
        return sorted(
            rows,
            key=lambda row: (
                row["tradingMode"],
                row["strategyName"] or "",
                row["symbol"] or "",
            ),
        )

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
            "reduced": 0,
            "rejected": 0,
            "executed": 0,
            "profitable": 0,
            "tradeCount": 0,
            "totalRealizedPnl": Decimal("0"),
            "wins": [],
            "losses": [],
            "realizedReturns": [],
            "fees": [],
            "slippages": [],
            "holds": [],
            "executionFailures": 0,
        }

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
        return {
            "totalRejected": sum(stage_counts.values()),
            "byStage": by_stage,
            "rows": breakdown_rows,
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
        return {
            "overview": sorted(overview, key=lambda row: row["tradingMode"]),
            "strategies": strategy_comparison,
        }
