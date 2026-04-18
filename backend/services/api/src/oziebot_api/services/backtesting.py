from __future__ import annotations

import hashlib
import json
import statistics
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from math import sqrt

from sqlalchemy.orm import Session

from oziebot_api.models.backtesting import (
    BacktestPerformanceSnapshot,
    BacktestRun,
    BacktestTradeResult,
    StrategyAnalyticsArtifactRecord,
)
from oziebot_domain.backtesting import (
    BacktestCandle,
    BacktestConfig,
    BacktestRunRequest,
    BacktestSnapshotScope,
    BacktestTrade,
)
from oziebot_domain.strategy import SignalType

try:
    from oziebot_strategy_engine.registry import StrategyRegistry
    from oziebot_strategy_engine.strategy import (
        MarketSnapshot,
        PositionState,
        StrategyContext,
    )

    _HAS_STRATEGY_ENGINE = True
except Exception:
    StrategyRegistry = None
    MarketSnapshot = None
    PositionState = None
    StrategyContext = None
    _HAS_STRATEGY_ENGINE = False


@dataclass
class _OpenPosition:
    symbol: str
    entry_ts: datetime
    entry_price: float
    entry_idx: int


def run_and_store_backtest(
    db: Session,
    *,
    user_id: uuid.UUID,
    tenant_id: uuid.UUID,
    request: BacktestRunRequest,
) -> BacktestRun:
    checksum = _request_checksum(request)
    deterministic_fingerprint = None
    if request.config.benchmark_mode:
        deterministic_fingerprint = f"{request.config.benchmark_namespace}:{checksum}"
        existing = (
            db.query(BacktestRun)
            .filter(
                BacktestRun.user_id == user_id,
                BacktestRun.deterministic_fingerprint == deterministic_fingerprint,
            )
            .first()
        )
        if existing is not None:
            return existing

    now = datetime.now(UTC)
    run_id = (
        uuid.uuid5(uuid.NAMESPACE_URL, f"{user_id}:{deterministic_fingerprint}")
        if deterministic_fingerprint
        else uuid.uuid4()
    )
    run = BacktestRun(
        id=run_id,
        user_id=user_id,
        tenant_id=tenant_id,
        strategy_id=request.strategy_id,
        trading_mode=request.trading_mode.value,
        benchmark_mode=request.config.benchmark_mode,
        deterministic_fingerprint=deterministic_fingerprint,
        dataset_name=request.dataset_name,
        timeframe=request.timeframe,
        status="running",
        params_json=request.config.model_dump(mode="json"),
        summary_json={},
        started_at=now,
        completed_at=None,
        created_at=now,
        updated_at=now,
    )
    db.add(run)
    db.flush()

    trades, execution_engine = _simulate_trades(
        request=request,
        tenant_id=tenant_id,
    )
    summary = _metrics(trades, request.config.initial_capital_cents)
    summary["checksum"] = checksum
    summary["execution_engine"] = execution_engine

    snapshots = _build_snapshots(
        trades=trades,
        user_id=user_id,
        strategy_id=request.strategy_id,
        trading_mode=request.trading_mode.value,
        symbols=sorted({c.symbol for c in request.candles}),
        initial_capital_cents=request.config.initial_capital_cents,
        created_at=now,
    )
    artifacts = _build_analytics_artifacts(
        trades=trades,
        strategy_id=request.strategy_id,
        trading_mode=request.trading_mode.value,
        dataset_name=request.dataset_name,
        timeframe=request.timeframe,
        checksum=checksum,
        execution_engine=execution_engine,
        created_at=now,
    )

    for trade in trades:
        db.add(
            BacktestTradeResult(
                run_id=run.id,
                symbol=trade.symbol,
                side=trade.side,
                entry_ts=trade.entry_ts,
                exit_ts=trade.exit_ts,
                quantity=f"{trade.quantity:.10f}",
                entry_price=f"{trade.entry_price:.10f}",
                exit_price=f"{trade.exit_price:.10f}",
                gross_return_bps=trade.gross_return_bps,
                net_return_bps=trade.net_return_bps,
                fee_bps_total=trade.fee_bps_total,
                slippage_bps_total=trade.slippage_bps_total,
                fee_impact_cents=trade.fee_impact_cents,
                slippage_impact_cents=trade.slippage_impact_cents,
                pnl_cents=trade.pnl_cents,
                holding_seconds=trade.holding_seconds,
                created_at=now,
            )
        )

    for snapshot in snapshots:
        db.add(
            BacktestPerformanceSnapshot(
                run_id=run.id,
                user_id=user_id,
                strategy_id=request.strategy_id,
                trading_mode=request.trading_mode.value,
                token_symbol=snapshot["token_symbol"],
                scope=snapshot["scope"],
                scope_key=snapshot["scope_key"],
                total_trades=snapshot["total_trades"],
                win_rate=snapshot["win_rate"],
                avg_return_bps=snapshot["avg_return_bps"],
                max_drawdown=snapshot["max_drawdown"],
                sharpe_like=snapshot["sharpe_like"],
                avg_slippage_bps=snapshot["avg_slippage_bps"],
                fee_impact_cents=snapshot["fee_impact_cents"],
                avg_holding_seconds=snapshot["avg_holding_seconds"],
                created_at=now,
            )
        )

    for artifact in artifacts:
        db.add(
            StrategyAnalyticsArtifactRecord(
                run_id=run.id,
                user_id=user_id,
                strategy_id=request.strategy_id,
                trading_mode=request.trading_mode.value,
                token_symbol=artifact["token_symbol"],
                feature_vector=artifact["feature_vector"],
                labels=artifact["labels"],
                metadata_json=artifact["metadata_json"],
                created_at=now,
            )
        )

    run.status = "completed"
    run.summary_json = summary
    run.completed_at = datetime.now(UTC)
    run.updated_at = run.completed_at
    db.add(run)
    db.commit()
    db.refresh(run)
    return run


def _simulate_trades(
    *,
    request: BacktestRunRequest,
    tenant_id: uuid.UUID,
) -> tuple[list[BacktestTrade], str]:
    if _HAS_STRATEGY_ENGINE:
        try:
            strategy = StrategyRegistry.get_strategy(request.strategy_id)
            strategy.validate_config(request.config.model_dump(mode="json"))
            return _simulate_trades_via_strategy(
                candles=request.candles,
                cfg=request.config,
                tenant_id=tenant_id,
                strategy=strategy,
                request=request,
            ), "strategy_plugin"
        except Exception:
            pass

    return _simulate_trades_heuristic(request.candles, request.config), "heuristic_fallback"


def _simulate_trades_via_strategy(
    *,
    candles: list[BacktestCandle],
    cfg: BacktestConfig,
    tenant_id: uuid.UUID,
    strategy,
    request: BacktestRunRequest,
) -> list[BacktestTrade]:
    by_symbol: dict[str, list[BacktestCandle]] = {}
    for c in candles:
        by_symbol.setdefault(c.symbol, []).append(c)

    out: list[BacktestTrade] = []
    for symbol, rows in by_symbol.items():
        rows = sorted(rows, key=lambda x: x.ts)
        open_pos: _OpenPosition | None = None
        strategy_cfg = request.config.model_dump(mode="json")

        for idx, cur in enumerate(rows):
            if idx == 0:
                continue
            prev = rows[idx - 1]

            context = StrategyContext(
                tenant_id=tenant_id,
                trading_mode=request.trading_mode,
                market_snapshot=MarketSnapshot(
                    timestamp=cur.ts,
                    symbol=symbol,
                    current_price=Decimal(str(cur.close)),
                    bid_price=Decimal(str(cur.close * (1 - cfg.slippage_bps / 10_000))),
                    ask_price=Decimal(str(cur.close * (1 + cfg.slippage_bps / 10_000))),
                    volume_24h=Decimal(str(cur.volume)),
                    open_price=Decimal(str(cur.open)),
                    high_price=Decimal(str(cur.high)),
                    low_price=Decimal(str(cur.low)),
                    close_price=Decimal(str(cur.close)),
                ),
                position_state=PositionState(
                    symbol=symbol,
                    quantity=Decimal("1") if open_pos else Decimal("0"),
                    entry_price=Decimal(str(open_pos.entry_price)) if open_pos else None,
                ),
            )

            signal = strategy.generate_signal(
                context=context,
                config=strategy_cfg,
                signal_id=uuid.uuid4(),
                correlation_id=uuid.uuid4(),
            )

            if signal.signal_type == SignalType.BUY and open_pos is None:
                open_pos = _OpenPosition(
                    symbol=symbol,
                    entry_ts=cur.ts,
                    entry_price=float(context.market_snapshot.ask_price),
                    entry_idx=idx,
                )
                continue

            if open_pos is None:
                momentum_bps = ((cur.close - prev.close) / prev.close) * 10_000
                if momentum_bps >= cfg.entry_threshold_bps:
                    open_pos = _OpenPosition(
                        symbol=symbol,
                        entry_ts=cur.ts,
                        entry_price=float(context.market_snapshot.ask_price),
                        entry_idx=idx,
                    )
                    continue

            if signal.signal_type in {SignalType.CLOSE, SignalType.SELL} and open_pos is not None:
                out.append(_close_trade(open_pos, cur, cfg))
                open_pos = None
                continue

            if open_pos is None:
                continue

            bars_held = idx - open_pos.entry_idx
            move_from_entry_bps = (
                (cur.close - open_pos.entry_price) / open_pos.entry_price
            ) * 10_000
            should_exit = (
                move_from_entry_bps >= cfg.take_profit_bps
                or move_from_entry_bps <= -cfg.stop_loss_bps
                or bars_held >= cfg.max_holding_bars
            )
            if not should_exit:
                continue

            out.append(_close_trade(open_pos, cur, cfg))
            open_pos = None

        if open_pos is not None:
            out.append(_close_trade(open_pos, rows[-1], cfg))

    return out


def _simulate_trades_heuristic(
    candles: list[BacktestCandle], cfg: BacktestConfig
) -> list[BacktestTrade]:
    by_symbol: dict[str, list[BacktestCandle]] = {}
    for c in candles:
        by_symbol.setdefault(c.symbol, []).append(c)

    out: list[BacktestTrade] = []
    for symbol, rows in by_symbol.items():
        rows = sorted(rows, key=lambda x: x.ts)
        open_pos: _OpenPosition | None = None

        for idx in range(1, len(rows)):
            prev = rows[idx - 1]
            cur = rows[idx]
            momentum_bps = ((cur.close - prev.close) / prev.close) * 10_000

            if open_pos is None and momentum_bps >= cfg.entry_threshold_bps:
                open_pos = _OpenPosition(
                    symbol=symbol,
                    entry_ts=cur.ts,
                    entry_price=cur.close * (1 + cfg.slippage_bps / 10_000),
                    entry_idx=idx,
                )
                continue

            if open_pos is None:
                continue

            bars_held = idx - open_pos.entry_idx
            move_from_entry_bps = (
                (cur.close - open_pos.entry_price) / open_pos.entry_price
            ) * 10_000
            should_exit = (
                move_from_entry_bps >= cfg.take_profit_bps
                or move_from_entry_bps <= -cfg.stop_loss_bps
                or bars_held >= cfg.max_holding_bars
            )
            if not should_exit:
                continue

            out.append(_close_trade(open_pos, cur, cfg))
            open_pos = None

        if open_pos is not None:
            out.append(_close_trade(open_pos, rows[-1], cfg))

    return out


def _close_trade(
    open_pos: _OpenPosition, candle: BacktestCandle, cfg: BacktestConfig
) -> BacktestTrade:
    exit_price = candle.close * (1 - cfg.slippage_bps / 10_000)
    gross_return_bps = ((exit_price - open_pos.entry_price) / open_pos.entry_price) * 10_000
    fee_bps_total = cfg.fee_bps * 2
    slippage_bps_total = cfg.slippage_bps * 2
    net_return_bps = gross_return_bps - fee_bps_total - slippage_bps_total

    fee_impact_cents = int(round(cfg.per_trade_notional_cents * (fee_bps_total / 10_000)))
    slippage_impact_cents = int(round(cfg.per_trade_notional_cents * (slippage_bps_total / 10_000)))
    pnl_cents = int(round(cfg.per_trade_notional_cents * (net_return_bps / 10_000)))

    dollars = cfg.per_trade_notional_cents / 100
    quantity = dollars / max(open_pos.entry_price, 1e-12)
    holding_seconds = max(0, int((candle.ts - open_pos.entry_ts).total_seconds()))

    return BacktestTrade(
        symbol=open_pos.symbol,
        entry_ts=open_pos.entry_ts,
        exit_ts=candle.ts,
        side="long",
        quantity=quantity,
        entry_price=open_pos.entry_price,
        exit_price=exit_price,
        gross_return_bps=gross_return_bps,
        net_return_bps=net_return_bps,
        fee_bps_total=fee_bps_total,
        slippage_bps_total=slippage_bps_total,
        fee_impact_cents=fee_impact_cents,
        slippage_impact_cents=slippage_impact_cents,
        pnl_cents=pnl_cents,
        holding_seconds=holding_seconds,
    )


def _request_checksum(request: BacktestRunRequest) -> str:
    candles = sorted(
        [
            {
                "ts": c.ts.isoformat(),
                "symbol": c.symbol,
                "open": c.open,
                "high": c.high,
                "low": c.low,
                "close": c.close,
                "volume": c.volume,
            }
            for c in request.candles
        ],
        key=lambda r: (r["symbol"], r["ts"]),
    )
    payload = {
        "strategy_id": request.strategy_id,
        "trading_mode": request.trading_mode.value,
        "dataset_name": request.dataset_name,
        "timeframe": request.timeframe,
        "config": request.config.model_dump(mode="json"),
        "candles": candles,
    }
    normalized = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def _metrics(trades: list[BacktestTrade], initial_capital_cents: int) -> dict[str, float | int]:
    if not trades:
        return {
            "total_trades": 0,
            "win_rate": 0.0,
            "avg_return_bps": 0.0,
            "max_drawdown": 0.0,
            "sharpe_like": 0.0,
            "avg_slippage_bps": 0.0,
            "fee_impact_cents": 0,
            "avg_holding_seconds": 0,
            "total_pnl_cents": 0,
        }

    returns = [t.net_return_bps for t in trades]
    wins = [t for t in trades if t.net_return_bps > 0]
    total_pnl = sum(t.pnl_cents for t in trades)
    fee_impact = sum(t.fee_impact_cents for t in trades)
    avg_holding = int(round(sum(t.holding_seconds for t in trades) / len(trades)))
    avg_slippage = float(sum(t.slippage_bps_total for t in trades) / len(trades))

    equity = float(initial_capital_cents)
    peak = equity
    max_dd = 0.0
    for t in trades:
        equity += t.pnl_cents
        peak = max(peak, equity)
        if peak > 0:
            max_dd = max(max_dd, (peak - equity) / peak)

    avg_return = float(sum(returns) / len(returns))
    sharpe_like = _sharpe_like(returns)

    return {
        "total_trades": len(trades),
        "win_rate": float(len(wins) / len(trades)),
        "avg_return_bps": avg_return,
        "max_drawdown": float(max_dd),
        "sharpe_like": sharpe_like,
        "avg_slippage_bps": avg_slippage,
        "fee_impact_cents": fee_impact,
        "avg_holding_seconds": avg_holding,
        "total_pnl_cents": total_pnl,
    }


def _sharpe_like(returns_bps: list[float]) -> float:
    if len(returns_bps) < 2:
        return 0.0
    values = [r / 10_000 for r in returns_bps]
    mean = statistics.fmean(values)
    stdev = statistics.stdev(values)
    if stdev <= 1e-12:
        return 0.0
    return float((mean / stdev) * sqrt(len(values)))


def _build_snapshots(
    *,
    trades: list[BacktestTrade],
    user_id: uuid.UUID,
    strategy_id: str,
    trading_mode: str,
    symbols: list[str],
    initial_capital_cents: int,
    created_at: datetime,
) -> list[dict]:
    all_metrics = _metrics(trades, initial_capital_cents)
    out = [
        {
            "scope": BacktestSnapshotScope.USER.value,
            "scope_key": str(user_id),
            "strategy_id": strategy_id,
            "trading_mode": trading_mode,
            "token_symbol": None,
            **all_metrics,
            "created_at": created_at,
        },
        {
            "scope": BacktestSnapshotScope.STRATEGY.value,
            "scope_key": strategy_id,
            "strategy_id": strategy_id,
            "trading_mode": trading_mode,
            "token_symbol": None,
            **all_metrics,
            "created_at": created_at,
        },
    ]

    by_token: dict[str, list[BacktestTrade]] = {}
    for t in trades:
        by_token.setdefault(t.symbol, []).append(t)

    for symbol in symbols:
        token_trades = by_token.get(symbol, [])
        out.append(
            {
                "scope": BacktestSnapshotScope.TOKEN.value,
                "scope_key": symbol,
                "strategy_id": strategy_id,
                "trading_mode": trading_mode,
                "token_symbol": symbol,
                **_metrics(token_trades, initial_capital_cents),
                "created_at": created_at,
            }
        )
    return out


def _build_analytics_artifacts(
    *,
    trades: list[BacktestTrade],
    strategy_id: str,
    trading_mode: str,
    dataset_name: str,
    timeframe: str,
    checksum: str,
    execution_engine: str,
    created_at: datetime,
) -> list[dict]:
    returns = [t.net_return_bps for t in trades]
    avg_return = float(sum(returns) / len(returns)) if returns else 0.0
    win_rate = float(len([r for r in returns if r > 0]) / len(returns)) if returns else 0.0

    out: list[dict] = [
        {
            "strategy_id": strategy_id,
            "trading_mode": trading_mode,
            "token_symbol": None,
            "feature_vector": {
                "trade_count": float(len(trades)),
                "avg_return_bps": avg_return,
                "win_rate": win_rate,
                "avg_holding_seconds": float(sum(t.holding_seconds for t in trades) / len(trades))
                if trades
                else 0.0,
            },
            "labels": {
                "target_next_run_return_bps": avg_return,
                "target_next_run_win_rate": win_rate,
            },
            "metadata_json": {
                "dataset": dataset_name,
                "timeframe": timeframe,
                "artifact_type": "strategy_level",
                "checksum": checksum,
                "execution_engine": execution_engine,
                "created_at": created_at.isoformat(),
            },
        }
    ]

    by_token: dict[str, list[BacktestTrade]] = {}
    for t in trades:
        by_token.setdefault(t.symbol, []).append(t)

    for symbol, token_trades in by_token.items():
        token_returns = [t.net_return_bps for t in token_trades]
        token_avg_return = float(sum(token_returns) / len(token_returns)) if token_returns else 0.0
        token_win_rate = (
            float(len([r for r in token_returns if r > 0]) / len(token_returns))
            if token_returns
            else 0.0
        )
        out.append(
            {
                "strategy_id": strategy_id,
                "trading_mode": trading_mode,
                "token_symbol": symbol,
                "feature_vector": {
                    "trade_count": float(len(token_trades)),
                    "avg_return_bps": token_avg_return,
                    "win_rate": token_win_rate,
                    "avg_fee_impact_cents": float(
                        sum(t.fee_impact_cents for t in token_trades) / len(token_trades)
                    )
                    if token_trades
                    else 0.0,
                },
                "labels": {
                    "target_next_run_return_bps": token_avg_return,
                    "target_next_run_win_rate": token_win_rate,
                },
                "metadata_json": {
                    "dataset": dataset_name,
                    "timeframe": timeframe,
                    "artifact_type": "token_level",
                    "checksum": checksum,
                    "execution_engine": execution_engine,
                    "created_at": created_at.isoformat(),
                },
            }
        )

    return out
