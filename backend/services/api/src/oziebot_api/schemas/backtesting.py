from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field


class BacktestCandleIn(BaseModel):
    ts: datetime
    symbol: str = Field(min_length=3, max_length=32)
    open: float = Field(gt=0)
    high: float = Field(gt=0)
    low: float = Field(gt=0)
    close: float = Field(gt=0)
    volume: float = Field(ge=0)


class BacktestConfigIn(BaseModel):
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


class BacktestRunCreate(BaseModel):
    strategy_id: str = Field(min_length=1, max_length=128)
    trading_mode: str = Field(pattern="^(paper|live)$")
    dataset_name: str = Field(min_length=1, max_length=128)
    timeframe: str = Field(min_length=1, max_length=32)
    candles: list[BacktestCandleIn] = Field(min_length=3)
    config: BacktestConfigIn = Field(default_factory=BacktestConfigIn)


class BacktestTradeOut(BaseModel):
    symbol: str
    side: str
    entry_ts: datetime
    exit_ts: datetime
    quantity: str
    entry_price: str
    exit_price: str
    gross_return_bps: float
    net_return_bps: float
    fee_bps_total: float
    slippage_bps_total: float
    fee_impact_cents: int
    slippage_impact_cents: int
    pnl_cents: int
    holding_seconds: int

    model_config = {"from_attributes": True}


class BacktestPerformanceSnapshotOut(BaseModel):
    scope: str
    scope_key: str
    strategy_id: str
    trading_mode: str
    token_symbol: str | None
    total_trades: int
    win_rate: float
    avg_return_bps: float
    max_drawdown: float
    sharpe_like: float
    avg_slippage_bps: float
    fee_impact_cents: int
    avg_holding_seconds: int
    created_at: datetime

    model_config = {"from_attributes": True}


class StrategyAnalyticsArtifactOut(BaseModel):
    strategy_id: str
    trading_mode: str
    token_symbol: str | None
    feature_vector: dict[str, Any]
    labels: dict[str, Any]
    metadata_json: dict[str, Any]
    created_at: datetime

    model_config = {"from_attributes": True}


class BacktestRunOut(BaseModel):
    id: UUID
    strategy_id: str
    trading_mode: str
    benchmark_mode: bool
    deterministic_fingerprint: str | None
    dataset_name: str
    timeframe: str
    status: str
    params_json: dict[str, Any]
    summary_json: dict[str, Any]
    started_at: datetime
    completed_at: datetime | None

    model_config = {"from_attributes": True}


class BacktestRunDetailOut(BacktestRunOut):
    trades: list[BacktestTradeOut]
    snapshots: list[BacktestPerformanceSnapshotOut]
    analytics_artifacts: list[StrategyAnalyticsArtifactOut]


class BacktestRunListOut(BaseModel):
    total: int
    runs: list[BacktestRunOut]


class HistoricalPerformanceOut(BaseModel):
    total: int
    snapshots: list[BacktestPerformanceSnapshotOut]


class StrategyAnalyticsListOut(BaseModel):
    total: int
    artifacts: list[StrategyAnalyticsArtifactOut]
