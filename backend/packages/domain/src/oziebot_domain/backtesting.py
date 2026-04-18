from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from uuid import UUID

from pydantic import Field

from oziebot_domain.trading_mode import TradingMode
from oziebot_domain.types import OziebotModel


class BacktestSnapshotScope(StrEnum):
    USER = "user"
    STRATEGY = "strategy"
    TOKEN = "token"


class BacktestCandle(OziebotModel):
    ts: datetime
    symbol: str = Field(min_length=3, max_length=32)
    open: float = Field(gt=0)
    high: float = Field(gt=0)
    low: float = Field(gt=0)
    close: float = Field(gt=0)
    volume: float = Field(ge=0)


class BacktestConfig(OziebotModel):
    initial_capital_cents: int = 100_000
    benchmark_mode: bool = False
    benchmark_namespace: str = "oziebot-backtest-v1"
    entry_threshold_bps: float = 20.0
    take_profit_bps: float = 120.0
    stop_loss_bps: float = 80.0
    max_holding_bars: int = 12
    fee_bps: float = 10.0
    slippage_bps: float = 5.0
    per_trade_notional_cents: int = 10_000


class BacktestTrade(OziebotModel):
    symbol: str
    entry_ts: datetime
    exit_ts: datetime
    side: str = "long"
    quantity: float
    entry_price: float
    exit_price: float
    gross_return_bps: float
    net_return_bps: float
    fee_bps_total: float
    slippage_bps_total: float
    fee_impact_cents: int
    slippage_impact_cents: int
    pnl_cents: int
    holding_seconds: int


class PerformanceSnapshot(OziebotModel):
    scope: BacktestSnapshotScope
    scope_key: str
    strategy_id: str
    token: str | None = None
    trading_mode: TradingMode
    total_trades: int
    win_rate: float
    avg_return_bps: float
    max_drawdown: float
    sharpe_like: float
    avg_slippage_bps: float
    fee_impact_cents: int
    avg_holding_seconds: int


class StrategyAnalyticsArtifact(OziebotModel):
    strategy_id: str
    token: str | None = None
    trading_mode: TradingMode
    feature_vector: dict[str, float]
    labels: dict[str, float]
    metadata: dict[str, str | float | int] = Field(default_factory=dict)


class BacktestRunRequest(OziebotModel):
    strategy_id: str = Field(min_length=1, max_length=128)
    trading_mode: TradingMode
    dataset_name: str = Field(min_length=1, max_length=128)
    timeframe: str = Field(min_length=1, max_length=32)
    candles: list[BacktestCandle] = Field(min_length=3)
    config: BacktestConfig = Field(default_factory=BacktestConfig)


class BacktestRunResult(OziebotModel):
    run_id: UUID
    user_id: UUID
    checksum: str
    strategy_id: str
    trading_mode: TradingMode
    started_at: datetime
    completed_at: datetime
    total_trades: int
    win_rate: float
    avg_return_bps: float
    max_drawdown: float
    sharpe_like: float
    avg_slippage_bps: float
    fee_impact_cents: int
    avg_holding_seconds: int
    snapshots: list[PerformanceSnapshot] = Field(default_factory=list)
    analytics: list[StrategyAnalyticsArtifact] = Field(default_factory=list)