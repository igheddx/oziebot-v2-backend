from __future__ import annotations

import csv
import io
import os
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

from sqlalchemy import Numeric, Select, case, cast, func, select
from sqlalchemy.orm import Session

from oziebot_common.reason_codes import (
    normalize_reason_code,
    summarize_rejection_reason,
    top_reason_rows,
)
from oziebot_common.token_policy import normalize_missing_policy_behavior
from oziebot_api.config import Settings
from oziebot_api.models.execution import ExecutionOrder, ExecutionPosition, ExecutionTradeRecord
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
    trade_details = [
        _trade_row_to_payload(feat, trade, snapshot) for feat, trade, snapshot in trade_rows
    ]
    execution_activity = _build_execution_activity(
        db,
        filters=filters,
        window_start=window_start,
    )
    open_positions = _build_open_positions(
        db,
        filters=filters,
    )

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
        "execution_activity": execution_activity,
        "open_positions": open_positions,
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
            "section",
            "label",
            "id",
            "strategy",
            "token",
            "trading_mode",
            "side",
            "event_time",
            "opened_at",
            "updated_at",
            "entry_time",
            "exit_time",
            "hold_minutes",
            "price_usd",
            "avg_entry_price_usd",
            "entry_price",
            "exit_price",
            "quantity",
            "size_usd",
            "notional_usd",
            "fees_usd",
            "gross_pnl_usd",
            "net_pnl_usd",
            "realized_pnl_usd",
            "unrealized_pnl_usd",
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
            "position_closed",
            "value_1",
            "value_2",
            "value_3",
            "value_4",
            "note",
        ],
    )
    writer.writeheader()
    writer.writerow(
        {
            "section": "overview",
            "label": "report_overview",
            "value_1": report.get("trade_count"),
            "value_2": report.get("execution_activity", {}).get("execution_count"),
            "value_3": report.get("open_positions", {}).get("position_count"),
            "note": "value_1=closed_trade_count,value_2=execution_count,value_3=open_position_count",
        }
    )
    for row in report["trade_details"]:
        writer.writerow(
            {
                "section": "closed_trade_detail",
                "label": "closed_trade",
                "id": row.get("trade_id"),
                "strategy": row.get("strategy"),
                "token": row.get("token"),
                "trading_mode": row.get("trading_mode"),
                "entry_time": row.get("entry_time"),
                "exit_time": row.get("exit_time"),
                "hold_minutes": row.get("hold_minutes"),
                "entry_price": row.get("entry_price"),
                "exit_price": row.get("exit_price"),
                "quantity": row.get("quantity"),
                "size_usd": row.get("size_usd"),
                "fees_usd": row.get("fees_usd"),
                "gross_pnl_usd": row.get("gross_pnl_usd"),
                "net_pnl_usd": row.get("net_pnl_usd"),
                "pnl_pct": row.get("pnl_pct"),
                "exit_reason": row.get("exit_reason"),
                "partial_profit_taken": row.get("partial_profit_taken"),
                "max_favorable_excursion_pct": row.get("max_favorable_excursion_pct"),
                "max_adverse_excursion_pct": row.get("max_adverse_excursion_pct"),
                "peak_unrealized_pnl_pct": row.get("peak_unrealized_pnl_pct"),
                "profit_giveback_pct": row.get("profit_giveback_pct"),
                "signal_confidence": row.get("signal_confidence"),
                "volume_confirmation_passed": row.get("volume_confirmation_passed"),
                "rejected_before_execution": row.get("rejected_before_execution"),
            }
        )
    for row in report.get("strategy_summary", []):
        writer.writerow(
            {
                "section": "closed_trade_strategy_summary",
                "label": row.get("strategy"),
                "strategy": row.get("strategy"),
                "value_1": row.get("total_trades"),
                "value_2": row.get("win_rate_pct"),
                "value_3": row.get("total_net_pnl_usd"),
                "value_4": row.get("profit_factor"),
                "note": "value_1=total_trades,value_2=win_rate_pct,value_3=total_net_pnl_usd,value_4=profit_factor",
            }
        )
    for row in report.get("token_summary", []):
        writer.writerow(
            {
                "section": "closed_trade_token_summary",
                "label": row.get("token"),
                "token": row.get("token"),
                "value_1": row.get("total_trades"),
                "value_2": row.get("win_rate_pct"),
                "value_3": row.get("total_net_pnl_usd"),
                "note": "value_1=total_trades,value_2=win_rate_pct,value_3=total_net_pnl_usd",
            }
        )
    for row in report.get("execution_activity", {}).get("execution_details", []):
        writer.writerow(
            {
                "section": "execution_detail",
                "label": "execution_trade",
                "id": row.get("execution_trade_id"),
                "strategy": row.get("strategy"),
                "token": row.get("token"),
                "trading_mode": row.get("trading_mode"),
                "side": row.get("side"),
                "event_time": row.get("executed_at"),
                "quantity": row.get("quantity"),
                "price_usd": row.get("price_usd"),
                "notional_usd": row.get("notional_usd"),
                "fees_usd": row.get("fees_usd"),
                "realized_pnl_usd": row.get("realized_pnl_usd"),
                "position_closed": row.get("position_closed"),
            }
        )
    for row in report.get("execution_activity", {}).get("strategy_summary", []):
        writer.writerow(
            {
                "section": "execution_strategy_summary",
                "label": row.get("strategy"),
                "strategy": row.get("strategy"),
                "trading_mode": row.get("trading_mode"),
                "value_1": row.get("total_executions"),
                "value_2": row.get("flattened_executions"),
                "value_3": row.get("total_notional_usd"),
                "value_4": row.get("total_realized_pnl_usd"),
                "note": "value_1=total_executions,value_2=flattened_executions,value_3=total_notional_usd,value_4=total_realized_pnl_usd",
            }
        )
    for row in report.get("execution_activity", {}).get("token_summary", []):
        writer.writerow(
            {
                "section": "execution_token_summary",
                "label": row.get("token"),
                "token": row.get("token"),
                "trading_mode": row.get("trading_mode"),
                "value_1": row.get("total_executions"),
                "value_2": row.get("flattened_executions"),
                "value_3": row.get("total_notional_usd"),
                "value_4": row.get("total_realized_pnl_usd"),
                "note": "value_1=total_executions,value_2=flattened_executions,value_3=total_notional_usd,value_4=total_realized_pnl_usd",
            }
        )
    for row in report.get("open_positions", {}).get("positions", []):
        writer.writerow(
            {
                "section": "open_position",
                "label": "current_open_position",
                "id": row.get("position_id"),
                "strategy": row.get("strategy"),
                "token": row.get("token"),
                "trading_mode": row.get("trading_mode"),
                "opened_at": row.get("opened_at"),
                "updated_at": row.get("updated_at"),
                "quantity": row.get("quantity"),
                "avg_entry_price_usd": row.get("avg_entry_price"),
                "notional_usd": row.get("position_notional_usd"),
                "realized_pnl_usd": row.get("realized_pnl_usd"),
                "note": row.get("last_trade_at"),
            }
        )
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
        .outerjoin(
            StrategySignalSnapshot,
            TradeOutcomeFeature.signal_snapshot_id == StrategySignalSnapshot.id,
        )
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
    signal_confidence = _float_or_none(snapshot.confidence_score) if snapshot is not None else None

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


def _build_execution_activity(
    db: Session,
    *,
    filters: TradingDiagnosticsFilters,
    window_start: datetime,
) -> dict[str, Any]:
    numeric_position_after = _numeric_string_expr(ExecutionTradeRecord.position_quantity_after)
    base_count_stmt = _apply_signal_filters(
        select(func.count()).select_from(ExecutionTradeRecord),
        filters=filters,
        window_start=window_start,
        token_column=ExecutionTradeRecord.symbol,
        strategy_column=ExecutionTradeRecord.strategy_id,
        mode_column=ExecutionTradeRecord.trading_mode,
        timestamp_column=ExecutionTradeRecord.executed_at,
    )
    execution_count = _count_rows(db, base_count_stmt)
    flattened_count = _count_rows(
        db,
        _apply_signal_filters(
            select(func.count())
            .select_from(ExecutionTradeRecord)
            .where(numeric_position_after == 0),
            filters=filters,
            window_start=window_start,
            token_column=ExecutionTradeRecord.symbol,
            strategy_column=ExecutionTradeRecord.strategy_id,
            mode_column=ExecutionTradeRecord.trading_mode,
            timestamp_column=ExecutionTradeRecord.executed_at,
        ),
    )
    aggregate_row = db.execute(
        _apply_signal_filters(
            select(
                func.count(func.distinct(ExecutionTradeRecord.symbol)),
                func.sum(case((ExecutionTradeRecord.side == "buy", 1), else_=0)),
                func.sum(case((ExecutionTradeRecord.side == "sell", 1), else_=0)),
                func.sum(ExecutionTradeRecord.gross_notional_cents),
                func.sum(ExecutionTradeRecord.fee_cents),
                func.sum(ExecutionTradeRecord.realized_pnl_cents),
            ).select_from(ExecutionTradeRecord),
            filters=filters,
            window_start=window_start,
            token_column=ExecutionTradeRecord.symbol,
            strategy_column=ExecutionTradeRecord.strategy_id,
            mode_column=ExecutionTradeRecord.trading_mode,
            timestamp_column=ExecutionTradeRecord.executed_at,
        )
    ).one()
    detail_rows = list(
        db.scalars(
            _apply_signal_filters(
                select(ExecutionTradeRecord)
                .order_by(ExecutionTradeRecord.executed_at.desc())
                .limit(max(1, min(filters.limit, 100))),
                filters=filters,
                window_start=window_start,
                token_column=ExecutionTradeRecord.symbol,
                strategy_column=ExecutionTradeRecord.strategy_id,
                mode_column=ExecutionTradeRecord.trading_mode,
                timestamp_column=ExecutionTradeRecord.executed_at,
            )
        ).all()
    )
    strategy_rows = db.execute(
        _apply_signal_filters(
            select(
                ExecutionTradeRecord.strategy_id,
                ExecutionTradeRecord.trading_mode,
                func.count(),
                func.sum(case((ExecutionTradeRecord.side == "buy", 1), else_=0)),
                func.sum(case((ExecutionTradeRecord.side == "sell", 1), else_=0)),
                func.sum(case((numeric_position_after == 0, 1), else_=0)),
                func.sum(ExecutionTradeRecord.gross_notional_cents),
                func.sum(ExecutionTradeRecord.fee_cents),
                func.sum(ExecutionTradeRecord.realized_pnl_cents),
                func.max(ExecutionTradeRecord.executed_at),
            )
            .select_from(ExecutionTradeRecord)
            .group_by(ExecutionTradeRecord.strategy_id, ExecutionTradeRecord.trading_mode),
            filters=filters,
            window_start=window_start,
            token_column=ExecutionTradeRecord.symbol,
            strategy_column=ExecutionTradeRecord.strategy_id,
            mode_column=ExecutionTradeRecord.trading_mode,
            timestamp_column=ExecutionTradeRecord.executed_at,
        )
    ).all()
    token_rows = db.execute(
        _apply_signal_filters(
            select(
                ExecutionTradeRecord.symbol,
                ExecutionTradeRecord.trading_mode,
                func.count(),
                func.sum(case((ExecutionTradeRecord.side == "buy", 1), else_=0)),
                func.sum(case((ExecutionTradeRecord.side == "sell", 1), else_=0)),
                func.sum(case((numeric_position_after == 0, 1), else_=0)),
                func.sum(ExecutionTradeRecord.gross_notional_cents),
                func.sum(ExecutionTradeRecord.fee_cents),
                func.sum(ExecutionTradeRecord.realized_pnl_cents),
                func.max(ExecutionTradeRecord.executed_at),
            )
            .select_from(ExecutionTradeRecord)
            .group_by(ExecutionTradeRecord.symbol, ExecutionTradeRecord.trading_mode),
            filters=filters,
            window_start=window_start,
            token_column=ExecutionTradeRecord.symbol,
            strategy_column=ExecutionTradeRecord.strategy_id,
            mode_column=ExecutionTradeRecord.trading_mode,
            timestamp_column=ExecutionTradeRecord.executed_at,
        )
    ).all()

    note_parts: list[str] = []
    if execution_count > len(detail_rows):
        note_parts.append(
            "Execution details are capped by the selected limit, while execution summary metrics cover the full filtered window."
        )

    return {
        "execution_count": execution_count,
        "flattened_trade_count": flattened_count,
        "buy_count": int(aggregate_row[1] or 0),
        "sell_count": int(aggregate_row[2] or 0),
        "unique_tokens": int(aggregate_row[0] or 0),
        "total_notional_usd": _cents_to_usd(aggregate_row[3]),
        "total_fees_usd": _cents_to_usd(aggregate_row[4]),
        "total_realized_pnl_usd": _cents_to_usd(aggregate_row[5]),
        "data_source": "execution_trades",
        "note": " ".join(note_parts) if note_parts else None,
        "strategy_summary": [
            {
                "strategy": strategy,
                "trading_mode": trading_mode,
                "total_executions": int(total or 0),
                "buy_executions": int(buys or 0),
                "sell_executions": int(sells or 0),
                "flattened_executions": int(flattened or 0),
                "total_notional_usd": _cents_to_usd(notional_cents),
                "total_fees_usd": _cents_to_usd(fee_cents),
                "total_realized_pnl_usd": _cents_to_usd(realized_pnl_cents),
                "last_executed_at": _aware_iso(last_executed_at),
            }
            for strategy, trading_mode, total, buys, sells, flattened, notional_cents, fee_cents, realized_pnl_cents, last_executed_at in sorted(
                strategy_rows,
                key=lambda row: (
                    row[9] if row[9] is not None else datetime.min.replace(tzinfo=UTC),
                    row[0],
                    row[1],
                ),
                reverse=True,
            )
        ],
        "token_summary": [
            {
                "token": token,
                "trading_mode": trading_mode,
                "total_executions": int(total or 0),
                "buy_executions": int(buys or 0),
                "sell_executions": int(sells or 0),
                "flattened_executions": int(flattened or 0),
                "total_notional_usd": _cents_to_usd(notional_cents),
                "total_fees_usd": _cents_to_usd(fee_cents),
                "total_realized_pnl_usd": _cents_to_usd(realized_pnl_cents),
                "last_executed_at": _aware_iso(last_executed_at),
            }
            for token, trading_mode, total, buys, sells, flattened, notional_cents, fee_cents, realized_pnl_cents, last_executed_at in sorted(
                token_rows,
                key=lambda row: (
                    row[9] if row[9] is not None else datetime.min.replace(tzinfo=UTC),
                    row[0],
                    row[1],
                ),
                reverse=True,
            )
        ],
        "execution_details": [_execution_trade_to_payload(row) for row in detail_rows],
    }


def _execution_trade_to_payload(trade: ExecutionTradeRecord) -> dict[str, Any]:
    quantity = _float_or_none(trade.quantity)
    price_usd = _float_or_none(trade.price)
    return {
        "execution_trade_id": str(trade.id),
        "order_id": str(trade.order_id),
        "strategy": trade.strategy_id,
        "token": trade.symbol,
        "trading_mode": trade.trading_mode,
        "side": trade.side,
        "executed_at": _aware_iso(trade.executed_at),
        "quantity": quantity,
        "price_usd": price_usd,
        "notional_usd": _cents_to_usd(trade.gross_notional_cents),
        "fees_usd": _cents_to_usd(trade.fee_cents),
        "realized_pnl_usd": _cents_to_usd(trade.realized_pnl_cents),
        "position_quantity_after": _float_or_none(trade.position_quantity_after),
        "position_closed": _string_numeric_is_zero(trade.position_quantity_after),
    }


def _build_open_positions(
    db: Session,
    *,
    filters: TradingDiagnosticsFilters,
) -> dict[str, Any]:
    stmt = select(ExecutionPosition).where(_numeric_string_expr(ExecutionPosition.quantity) > 0)
    if filters.normalized_token:
        stmt = stmt.where(ExecutionPosition.symbol == filters.normalized_token)
    if filters.normalized_strategy:
        stmt = stmt.where(ExecutionPosition.strategy_id == filters.normalized_strategy)
    if filters.normalized_mode:
        stmt = stmt.where(ExecutionPosition.trading_mode == filters.normalized_mode)
    positions = list(db.scalars(stmt.order_by(ExecutionPosition.updated_at.desc())).all())
    exposure_by_strategy = {
        "momentum": 0.0,
        "day_trading": 0.0,
        "reversion": 0.0,
        "dca": 0.0,
    }
    position_rows = [_open_position_to_payload(row) for row in positions]
    total_notional = 0.0
    total_realized_pnl = 0.0
    for row in position_rows:
        total_notional += row["position_notional_usd"] or 0.0
        total_realized_pnl += row["realized_pnl_usd"] or 0.0
        exposure_by_strategy[row["strategy"]] = round(
            exposure_by_strategy.get(row["strategy"], 0.0) + (row["position_notional_usd"] or 0.0),
            6,
        )

    note = "Open positions are a current snapshot from execution_positions; the days filter does not narrow this section."
    if not position_rows:
        note = "No current open positions matched the selected token, strategy, and trading-mode filters."

    return {
        "position_count": len(position_rows),
        "unique_tokens": len({row["token"] for row in position_rows}),
        "total_position_notional_usd": round(total_notional, 6),
        "total_realized_pnl_usd": round(total_realized_pnl, 6),
        "exposure_by_strategy": exposure_by_strategy,
        "data_source": "execution_positions",
        "note": note,
        "positions": position_rows,
    }


def _open_position_to_payload(position: ExecutionPosition) -> dict[str, Any]:
    quantity = _float_or_none(position.quantity)
    avg_entry_price = _float_or_none(position.avg_entry_price)
    notional = (
        round((quantity or 0.0) * (avg_entry_price or 0.0), 6)
        if quantity is not None and avg_entry_price is not None
        else None
    )
    return {
        "position_id": str(position.id),
        "strategy": position.strategy_id,
        "token": position.symbol,
        "trading_mode": position.trading_mode,
        "quantity": quantity,
        "avg_entry_price": avg_entry_price,
        "position_notional_usd": notional,
        "realized_pnl_usd": _cents_to_usd(position.realized_pnl_cents),
        "opened_at": _aware_iso(position.opened_at),
        "last_trade_at": _aware_iso(position.last_trade_at),
        "updated_at": _aware_iso(position.updated_at),
        "closed_at": _aware_iso(position.closed_at),
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
    runs_count = _count_rows(
        db,
        _apply_signal_filters(
            select(func.count()).select_from(StrategyRun),
            filters=filters,
            window_start=window_start,
            token_column=StrategyRun.symbol,
            strategy_column=StrategyRun.strategy_name,
            mode_column=StrategyRun.trading_mode,
            timestamp_column=StrategyRun.started_at,
        ),
    )
    signals_count = _count_rows(
        db,
        _apply_signal_filters(
            select(func.count()).select_from(StrategySignalRecord),
            filters=filters,
            window_start=window_start,
            token_column=StrategySignalRecord.symbol,
            strategy_column=StrategySignalRecord.strategy_name,
            mode_column=StrategySignalRecord.trading_mode,
            timestamp_column=StrategySignalRecord.timestamp,
        ),
    )
    risk_events_count = _count_rows(
        db,
        _apply_signal_filters(
            select(func.count()).select_from(RiskEvent),
            filters=filters,
            window_start=window_start,
            token_column=RiskEvent.symbol,
            strategy_column=RiskEvent.strategy_name,
            mode_column=RiskEvent.trading_mode,
            timestamp_column=RiskEvent.created_at,
        ),
    )
    orders_count = _count_rows(
        db,
        _apply_signal_filters(
            select(func.count()).select_from(ExecutionOrder),
            filters=filters,
            window_start=window_start,
            token_column=ExecutionOrder.symbol,
            strategy_column=ExecutionOrder.strategy_id,
            mode_column=ExecutionOrder.trading_mode,
            timestamp_column=ExecutionOrder.created_at,
        ),
    )

    signal_actions: Counter[str] = Counter()
    rejection_reason_codes: Counter[str] = Counter()
    strategy_breakdown: dict[str, dict[str, Any]] = defaultdict(_empty_strategy_breakdown)

    evaluated_rows = db.execute(
        _apply_signal_filters(
            select(
                StrategyRun.strategy_name,
                func.count(),
            )
            .select_from(StrategyRun)
            .group_by(StrategyRun.strategy_name),
            filters=filters,
            window_start=window_start,
            token_column=StrategyRun.symbol,
            strategy_column=StrategyRun.strategy_name,
            mode_column=StrategyRun.trading_mode,
            timestamp_column=StrategyRun.started_at,
        )
    ).all()
    for strategy_name, count in evaluated_rows:
        bucket = strategy_breakdown[str(strategy_name)]
        bucket["signals_evaluated"] = int(count or 0)

    signal_rows = db.execute(
        _apply_signal_filters(
            select(
                StrategySignalRecord.strategy_name,
                StrategySignalRecord.action,
                StrategySignalRecord.reasoning_metadata["reason"].as_string(),
                func.count(),
            )
            .select_from(StrategySignalRecord)
            .group_by(
                StrategySignalRecord.strategy_name,
                StrategySignalRecord.action,
                StrategySignalRecord.reasoning_metadata["reason"].as_string(),
            ),
            filters=filters,
            window_start=window_start,
            token_column=StrategySignalRecord.symbol,
            strategy_column=StrategySignalRecord.strategy_name,
            mode_column=StrategySignalRecord.trading_mode,
            timestamp_column=StrategySignalRecord.timestamp,
        )
    ).all()
    for strategy_name, action, reason, count in signal_rows:
        bucket = strategy_breakdown[str(strategy_name)]
        action_name = _bucket_signal_action(action)
        bucket["signal_actions"][action_name] += int(count or 0)
        signal_actions[action_name] += int(count or 0)
        if action_name == "hold":
            bucket["hold_reason_counts"][_reason_label(reason)] += int(count or 0)
        else:
            bucket["non_hold_reason_counts"][_reason_label(reason)] += int(count or 0)

    suppressed_count = 0
    suppressed_rows = db.execute(
        _apply_signal_filters(
            select(
                StrategyRun.strategy_name,
                StrategyRun.run_metadata["suppression_reason"].as_string(),
            )
            .select_from(StrategyRun)
            .where(StrategyRun.run_metadata["suppressed"].as_boolean().is_(True)),
            filters=filters,
            window_start=window_start,
            token_column=StrategyRun.symbol,
            strategy_column=StrategyRun.strategy_name,
            mode_column=StrategyRun.trading_mode,
            timestamp_column=StrategyRun.started_at,
        )
    ).all()
    for strategy_name, reason in suppressed_rows:
        suppressed_count += 1
        normalized_reason = normalize_reason_code(reason)
        rejection_reason_codes[normalized_reason] += 1
        strategy_breakdown[str(strategy_name)]["rejection_reasons"][normalized_reason] += 1

    risk_rejects_count = 0
    risk_reject_rows = db.execute(
        _apply_signal_filters(
            select(RiskEvent.strategy_name, RiskEvent.reason, RiskEvent.detail)
            .select_from(RiskEvent)
            .where(RiskEvent.outcome.ilike("reject%")),
            filters=filters,
            window_start=window_start,
            token_column=RiskEvent.symbol,
            strategy_column=RiskEvent.strategy_name,
            mode_column=RiskEvent.trading_mode,
            timestamp_column=RiskEvent.created_at,
        )
    ).all()
    for strategy_name, reason, detail in risk_reject_rows:
        risk_rejects_count += 1
        normalized_reason = normalize_reason_code(reason, reason_detail=detail)
        rejection_reason_codes[normalized_reason] += 1
        strategy_breakdown[str(strategy_name)]["rejection_reasons"][normalized_reason] += 1

    failed_orders_count = 0
    failed_order_rows = db.execute(
        _apply_signal_filters(
            select(
                ExecutionOrder.strategy_id,
                ExecutionOrder.failure_code,
                ExecutionOrder.failure_detail,
                ExecutionOrder.state,
            )
            .select_from(ExecutionOrder)
            .where(ExecutionOrder.state.in_(("failed", "cancelled", "rejected"))),
            filters=filters,
            window_start=window_start,
            token_column=ExecutionOrder.symbol,
            strategy_column=ExecutionOrder.strategy_id,
            mode_column=ExecutionOrder.trading_mode,
            timestamp_column=ExecutionOrder.created_at,
        )
    ).all()
    for strategy_name, failure_code, failure_detail, state in failed_order_rows:
        failed_orders_count += 1
        normalized_reason = normalize_reason_code(
            failure_code or state,
            reason_detail=failure_detail,
        )
        rejection_reason_codes[normalized_reason] += 1
        strategy_breakdown[str(strategy_name)]["rejection_reasons"][normalized_reason] += 1

    signals_evaluated: int | None
    if runs_count:
        signals_evaluated = runs_count
    elif trade_count > 0 or signals_count or risk_events_count or orders_count:
        signals_evaluated = None
    else:
        signals_evaluated = 0

    if signals_count:
        signals_emitted: int | None = signals_count
    elif trade_count > 0 or runs_count or risk_events_count or orders_count:
        signals_emitted = None
    else:
        signals_emitted = 0

    rejected_count = suppressed_count + risk_rejects_count + failed_orders_count
    if rejected_count:
        signals_rejected: int | None = rejected_count
    elif runs_count or signals_count or risk_events_count:
        signals_rejected = 0
    elif trade_count > 0:
        signals_rejected = None
    else:
        signals_rejected = 0

    for bucket in strategy_breakdown.values():
        bucket["signals_rejected"] = int(sum(bucket["rejection_reasons"].values()))

    unavailable_metrics = [
        metric
        for metric, value in (
            ("signals_evaluated", signals_evaluated),
            ("signals_emitted", signals_emitted),
            ("signals_rejected", signals_rejected),
        )
        if value is None
    ]

    signal_actions_payload = _serialize_signal_actions(signal_actions)
    rejection_reasons = Counter(
        {
            summarize_rejection_reason(reason): count
            for reason, count in rejection_reason_codes.items()
        }
    )
    strategy_breakdown_payload = {
        strategy: {
            "signals_evaluated": bucket["signals_evaluated"],
            "signals_rejected": bucket["signals_rejected"],
            "signal_actions": _serialize_signal_actions(bucket["signal_actions"]),
            "rejection_reasons": dict(sorted(bucket["rejection_reasons"].items())),
            "top_hold_reasons": _top_counter_rows(bucket["hold_reason_counts"]),
            "top_non_hold_reasons": _top_counter_rows(bucket["non_hold_reason_counts"]),
            "top_rejection_reasons": top_reason_rows(bucket["rejection_reasons"]),
        }
        for strategy, bucket in sorted(strategy_breakdown.items())
    }

    return {
        "signals_evaluated": signals_evaluated,
        "signals_emitted": signals_emitted,
        "signals_rejected": signals_rejected,
        "non_hold_signals_emitted": signal_actions_payload["non_hold"],
        "trades_executed": trade_count,
        "signal_actions": signal_actions_payload,
        "rejection_reasons": {
            "confidence": _counter_or_none(rejection_reasons, "confidence", signals_rejected),
            "volume": _counter_or_none(rejection_reasons, "volume", signals_rejected),
            "allocation": _counter_or_none(rejection_reasons, "allocation", signals_rejected),
            "risk_engine": _counter_or_none(rejection_reasons, "risk_engine", signals_rejected),
            "token_strategy_policy": _counter_or_none(
                rejection_reasons, "token_strategy_policy", signals_rejected
            ),
            "cooldown": _counter_or_none(rejection_reasons, "cooldown", signals_rejected),
            "liquidity_hours": _counter_or_none(
                rejection_reasons, "liquidity_hours", signals_rejected
            ),
            "other": None,
        },
        "top_rejection_reasons": top_reason_rows(rejection_reason_codes),
        "data_sources": {
            "signals_evaluated": "strategy_runs",
            "signals_emitted": "strategy_signal_records",
            "signals_rejected": "strategy_runs.run_metadata + risk_events + execution_orders",
            "trades_executed": "trade_outcome_features (closed trade outcomes)",
        },
        "strategy_breakdown": strategy_breakdown_payload,
        "unavailable_metrics": unavailable_metrics,
        "note": (
            "signals_emitted counts stored strategy signal records and may include HOLD decisions; "
            "use non_hold_signals_emitted and signal_actions for actionable emissions."
            if not unavailable_metrics
            else "Signals were executed or completed, but earlier-stage telemetry is unavailable for some metrics."
        ),
        "telemetry_note": (
            "signals_emitted counts stored strategy signal records and may include HOLD decisions; "
            "use non_hold_signals_emitted and signal_actions for actionable emissions."
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
    ledger_stmt = select(StrategyCapitalLedger).where(
        StrategyCapitalLedger.created_at >= window_start
    )
    if filters.normalized_strategy:
        bucket_stmt = bucket_stmt.where(
            StrategyCapitalBucket.strategy_id == filters.normalized_strategy
        )
        ledger_stmt = ledger_stmt.where(
            StrategyCapitalLedger.strategy_id == filters.normalized_strategy
        )
    if filters.normalized_mode:
        bucket_stmt = bucket_stmt.where(
            StrategyCapitalBucket.trading_mode == filters.normalized_mode
        )
        ledger_stmt = ledger_stmt.where(
            StrategyCapitalLedger.trading_mode == filters.normalized_mode
        )
    buckets = list(db.scalars(bucket_stmt).all())
    peak_ledger_deployed_pct = db.scalar(
        ledger_stmt.with_only_columns(
            func.max(
                (
                    (
                        StrategyCapitalLedger.after_locked_capital_cents
                        + StrategyCapitalLedger.after_reserved_cash_cents
                    )
                    * 100.0
                )
                / func.nullif(
                    (
                        StrategyCapitalLedger.after_available_cash_cents
                        + StrategyCapitalLedger.after_reserved_cash_cents
                        + StrategyCapitalLedger.after_locked_capital_cents
                    ),
                    0,
                )
            )
        )
    )

    note_parts: list[str] = []
    if filters.normalized_token:
        note_parts.append(
            "Capital buckets are strategy-scoped, so token filters do not change capital totals."
        )
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
            "note": " ".join(note_parts)
            if note_parts
            else "No capital bucket data matched the selected filters.",
        }

    total_account_value = (
        sum(
            bucket.assigned_capital_cents + bucket.realized_pnl_cents + bucket.unrealized_pnl_cents
            for bucket in buckets
        )
        / 100
    )
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
    if peak_ledger_deployed_pct is not None:
        peak_capital_deployed_pct = max(
            peak_capital_deployed_pct or 0,
            float(peak_ledger_deployed_pct),
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
    trailing_rows = [
        trade for trade in trades if _reason_contains(trade["exit_reason"], "trailing")
    ]
    reversal_rows = [
        trade
        for trade in trades
        if _reason_contains(trade["exit_reason"], "bearish")
        or _reason_contains(trade["exit_reason"], "reversal")
    ]
    partial_rows = [trade for trade in trades if trade["partial_profit_taken"]]
    losing_rows = [trade for trade in trades if (trade["net_pnl_usd"] or 0) < 0]
    losing_positive_before_loss = [
        trade for trade in losing_rows if (trade["max_favorable_excursion_pct"] or 0) > 0
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
        slug: (config or {}).get("signal_rules", {}) for slug, config in strategies.items()
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


def _count_rows(db: Session, stmt: Select[Any]) -> int:
    return int(db.scalar(stmt) or 0)


def _bucket_signal_action(action: str | None) -> str:
    normalized = (action or "").strip().lower()
    if normalized in {"buy", "sell", "close", "hold"}:
        return normalized
    return "other"


def _reason_label(reason: str | None) -> str:
    value = (reason or "").strip()
    return value or "unspecified"


def _empty_strategy_breakdown() -> dict[str, Any]:
    return {
        "signals_evaluated": 0,
        "signals_rejected": 0,
        "signal_actions": Counter(),
        "rejection_reasons": Counter(),
        "hold_reason_counts": Counter(),
        "non_hold_reason_counts": Counter(),
    }


def _serialize_signal_actions(counter: Counter[str]) -> dict[str, int]:
    counts = {
        "buy": int(counter.get("buy", 0)),
        "sell": int(counter.get("sell", 0)),
        "close": int(counter.get("close", 0)),
        "hold": int(counter.get("hold", 0)),
        "other": int(counter.get("other", 0)),
    }
    counts["non_hold"] = counts["buy"] + counts["sell"] + counts["close"] + counts["other"]
    return counts


def _top_counter_rows(counter: Counter[str], *, limit: int = 3) -> list[dict[str, Any]]:
    return [{"reason": reason, "count": int(count)} for reason, count in counter.most_common(limit)]


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


def _cents_to_usd(value: Any) -> float | None:
    if value is None:
        return None
    return round(float(Decimal(str(value)) / Decimal("100")), 6)


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


def _numeric_string_expr(column: Any) -> Any:
    return func.coalesce(cast(func.nullif(column, ""), Numeric(28, 10)), 0)


def _string_numeric_is_zero(value: str | None) -> bool:
    if value is None:
        return True
    return Decimal(str(value or "0")) == 0
