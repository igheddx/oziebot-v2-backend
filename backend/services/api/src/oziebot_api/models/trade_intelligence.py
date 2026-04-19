from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import DateTime, Float, ForeignKey, Integer, JSON, Numeric, String, Uuid
from sqlalchemy.orm import Mapped, mapped_column, relationship

from oziebot_api.db.base import Base


class StrategySignalSnapshot(Base):
    __tablename__ = "strategy_signal_snapshots"

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True
    )
    tenant_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False, index=True
    )
    trading_mode: Mapped[str] = mapped_column(String(16), nullable=False, index=True)
    strategy_name: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    token_symbol: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    current_price: Mapped[float] = mapped_column(Numeric(28, 10), nullable=False)
    best_bid: Mapped[float] = mapped_column(Numeric(28, 10), nullable=False)
    best_ask: Mapped[float] = mapped_column(Numeric(28, 10), nullable=False)
    spread_pct: Mapped[float] = mapped_column(Numeric(18, 10), nullable=False)
    estimated_slippage_pct: Mapped[float] = mapped_column(Numeric(18, 10), nullable=False)
    volume: Mapped[float] = mapped_column(Numeric(28, 10), nullable=False)
    volatility: Mapped[float | None] = mapped_column(Numeric(18, 10), nullable=True)
    confidence_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    raw_feature_json: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    token_policy_status: Mapped[str | None] = mapped_column(String(32), nullable=True, index=True)
    token_policy_multiplier: Mapped[float | None] = mapped_column(Numeric(18, 10), nullable=True)

    decision_audits: Mapped[list["StrategyDecisionAudit"]] = relationship(
        "StrategyDecisionAudit",
        back_populates="signal_snapshot",
        cascade="all, delete-orphan",
    )
    trade_outcomes: Mapped[list["TradeOutcomeFeature"]] = relationship(
        "TradeOutcomeFeature",
        back_populates="signal_snapshot",
    )
    ai_inference_records: Mapped[list["AIInferenceRecord"]] = relationship(
        "AIInferenceRecord",
        back_populates="signal_snapshot",
        cascade="all, delete-orphan",
    )


class StrategyDecisionAudit(Base):
    __tablename__ = "strategy_decision_audits"

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    signal_snapshot_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("strategy_signal_snapshots.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    stage: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    decision: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    reason_code: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    reason_detail: Mapped[str | None] = mapped_column(String(512), nullable=True)
    size_before: Mapped[float | None] = mapped_column(Numeric(28, 10), nullable=True)
    size_after: Mapped[float | None] = mapped_column(Numeric(28, 10), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, index=True
    )

    signal_snapshot: Mapped[StrategySignalSnapshot | None] = relationship(
        "StrategySignalSnapshot", back_populates="decision_audits"
    )


class TradeOutcomeFeature(Base):
    __tablename__ = "trade_outcome_features"

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    trade_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("execution_trades.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    signal_snapshot_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("strategy_signal_snapshots.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    trading_mode: Mapped[str] = mapped_column(String(16), nullable=False, index=True)
    strategy_name: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    token_symbol: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    entry_price: Mapped[float] = mapped_column(Numeric(28, 10), nullable=False)
    exit_price: Mapped[float] = mapped_column(Numeric(28, 10), nullable=False)
    filled_size: Mapped[float] = mapped_column(Numeric(28, 10), nullable=False)
    fee_paid: Mapped[float] = mapped_column(Numeric(28, 10), nullable=False)
    slippage_realized: Mapped[float | None] = mapped_column(Numeric(18, 10), nullable=True)
    hold_seconds: Mapped[int | None] = mapped_column(Integer, nullable=True)
    realized_pnl: Mapped[float] = mapped_column(Numeric(28, 10), nullable=False)
    realized_return_pct: Mapped[float | None] = mapped_column(Numeric(18, 10), nullable=True)
    max_favorable_excursion_pct: Mapped[float | None] = mapped_column(
        Numeric(18, 10), nullable=True
    )
    max_adverse_excursion_pct: Mapped[float | None] = mapped_column(Numeric(18, 10), nullable=True)
    exit_reason: Mapped[str | None] = mapped_column(String(128), nullable=True)
    win_loss_label: Mapped[str] = mapped_column(String(16), nullable=False, index=True)
    profitable_after_fees_label: Mapped[str] = mapped_column(String(16), nullable=False, index=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, index=True
    )

    signal_snapshot: Mapped[StrategySignalSnapshot | None] = relationship(
        "StrategySignalSnapshot", back_populates="trade_outcomes"
    )


class AIInferenceRecord(Base):
    __tablename__ = "ai_inference_records"

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    signal_snapshot_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("strategy_signal_snapshots.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    model_name: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    model_version: Mapped[str] = mapped_column(String(64), nullable=False)
    recommendation: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    confidence_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    explanation_json: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, index=True
    )

    signal_snapshot: Mapped[StrategySignalSnapshot] = relationship(
        "StrategySignalSnapshot", back_populates="ai_inference_records"
    )
