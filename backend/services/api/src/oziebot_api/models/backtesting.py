from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import Boolean, DateTime, Float, ForeignKey, Integer, JSON, String, Uuid, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from oziebot_api.db.base import Base


class BacktestRun(Base):
    __tablename__ = "backtest_runs"
    __table_args__ = (
        UniqueConstraint("user_id", "deterministic_fingerprint", name="uq_backtest_run_user_fingerprint"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True
    )
    tenant_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False, index=True
    )
    strategy_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    trading_mode: Mapped[str] = mapped_column(String(16), nullable=False, index=True)
    benchmark_mode: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="false")
    deterministic_fingerprint: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    dataset_name: Mapped[str] = mapped_column(String(128), nullable=False)
    timeframe: Mapped[str] = mapped_column(String(32), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="completed")
    params_json: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    summary_json: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    trades: Mapped[list["BacktestTradeResult"]] = relationship(
        "BacktestTradeResult", back_populates="run", cascade="all, delete-orphan"
    )
    snapshots: Mapped[list["BacktestPerformanceSnapshot"]] = relationship(
        "BacktestPerformanceSnapshot", back_populates="run", cascade="all, delete-orphan"
    )
    analytics_artifacts: Mapped[list["StrategyAnalyticsArtifactRecord"]] = relationship(
        "StrategyAnalyticsArtifactRecord", back_populates="run", cascade="all, delete-orphan"
    )


class BacktestTradeResult(Base):
    __tablename__ = "backtest_trade_results"

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    run_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("backtest_runs.id", ondelete="CASCADE"), nullable=False, index=True
    )
    symbol: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    side: Mapped[str] = mapped_column(String(16), nullable=False, default="long")
    entry_ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    exit_ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    quantity: Mapped[str] = mapped_column(String(64), nullable=False)
    entry_price: Mapped[str] = mapped_column(String(64), nullable=False)
    exit_price: Mapped[str] = mapped_column(String(64), nullable=False)
    gross_return_bps: Mapped[float] = mapped_column(Float, nullable=False)
    net_return_bps: Mapped[float] = mapped_column(Float, nullable=False)
    fee_bps_total: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    slippage_bps_total: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    fee_impact_cents: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    slippage_impact_cents: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    pnl_cents: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    holding_seconds: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    run: Mapped[BacktestRun] = relationship("BacktestRun", back_populates="trades")


class BacktestPerformanceSnapshot(Base):
    __tablename__ = "backtest_performance_snapshots"

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    run_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("backtest_runs.id", ondelete="CASCADE"), nullable=False, index=True
    )
    user_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True
    )
    strategy_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    trading_mode: Mapped[str] = mapped_column(String(16), nullable=False, index=True)
    token_symbol: Mapped[str | None] = mapped_column(String(32), nullable=True, index=True)
    scope: Mapped[str] = mapped_column(String(16), nullable=False, index=True)
    scope_key: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    total_trades: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    win_rate: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    avg_return_bps: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    max_drawdown: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    sharpe_like: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    avg_slippage_bps: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    fee_impact_cents: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    avg_holding_seconds: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)

    run: Mapped[BacktestRun] = relationship("BacktestRun", back_populates="snapshots")


class StrategyAnalyticsArtifactRecord(Base):
    __tablename__ = "strategy_analytics_artifacts"

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    run_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("backtest_runs.id", ondelete="CASCADE"), nullable=False, index=True
    )
    user_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True
    )
    strategy_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    trading_mode: Mapped[str] = mapped_column(String(16), nullable=False, index=True)
    token_symbol: Mapped[str | None] = mapped_column(String(32), nullable=True, index=True)
    feature_vector: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    labels: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    metadata_json: Mapped[dict[str, Any]] = mapped_column("metadata", JSON, nullable=False, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)

    run: Mapped[BacktestRun] = relationship("BacktestRun", back_populates="analytics_artifacts")
