from __future__ import annotations

from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, Field


class StrategicAggressiveAllocationConfigPayload(BaseModel):
    trading_mode: Literal["paper", "live"]
    enabled: bool = False
    total_allocated_amount_usd: dict[str, Any] = Field(default_factory=dict)
    bucket_allocations: list[dict[str, Any]] = Field(default_factory=list)
    selected_tokens: dict[str, list[str]] = Field(default_factory=dict)
    max_allocation_per_token: dict[str, float] = Field(default_factory=dict)
    profit_taking_rules: dict[str, Any] = Field(default_factory=dict)
    stop_loss_rules: dict[str, Any] = Field(default_factory=dict)
    trailing_stop_rules: dict[str, Any] = Field(default_factory=dict)
    rebalance_settings: dict[str, Any] = Field(default_factory=dict)
    mode_settings: dict[str, Any] = Field(default_factory=dict)


class StrategicAggressiveAllocationEnablePayload(BaseModel):
    trading_mode: Literal["paper", "live"]


class StrategicAggressiveAllocationRebalancePayload(BaseModel):
    trading_mode: Literal["paper", "live"]
    aggressive_rebalance: bool | None = None
    execute: bool = False


class StrategicAggressiveAllocationTokenOption(BaseModel):
    symbol: str
    display_name: str | None = None
    bucket_ids: list[str] = Field(default_factory=list)
    ecosystem: str | None = None
    strategy_policy_status: str
    strategy_policy_reason: str | None = None
    user_enabled: bool
    admin_enabled: bool
    trend_score: float | None = None
    liquidity_score: float | None = None


class StrategicAggressiveAllocationProfitEventOut(BaseModel):
    id: UUID
    symbol: str
    bucket_id: str
    event_type: str
    status: str
    quantity: str
    trigger_price: str | None = None
    realized_pnl_cents: int | None = None
    signal_id: UUID | None = None
    correlation_id: UUID | None = None
    metadata: dict[str, Any] | None = None
    occurred_at: datetime


class StrategicAggressiveAllocationSignalPreview(BaseModel):
    signal_id: UUID
    symbol: str
    action: str
    bucket_id: str
    reason: str
    suggested_size: float
    confidence: float
