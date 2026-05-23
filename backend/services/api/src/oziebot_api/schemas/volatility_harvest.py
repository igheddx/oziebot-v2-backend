from __future__ import annotations

from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, Field


class VolatilityHarvestConfigPayload(BaseModel):
    trading_mode: Literal["paper", "live"]
    enabled: bool = False
    selected_tokens: list[str] = Field(default_factory=list)
    total_allocated_amount_usd: dict[str, Any] = Field(default_factory=dict)
    core_position_percentage: float = 70.0
    trading_position_percentage: float = 30.0
    entry_layers: list[dict[str, Any]] = Field(default_factory=list)
    harvest_bands: list[dict[str, Any]] = Field(default_factory=list)
    rebuy_bands: list[dict[str, Any]] = Field(default_factory=list)
    volatility_settings: dict[str, Any] = Field(default_factory=dict)
    risk_controls: dict[str, Any] = Field(default_factory=dict)
    fee_settings: dict[str, Any] = Field(default_factory=dict)
    mode_settings: dict[str, Any] = Field(default_factory=dict)


class VolatilityHarvestTogglePayload(BaseModel):
    trading_mode: Literal["paper", "live"]


class VolatilityHarvestCyclePayload(BaseModel):
    trading_mode: Literal["paper", "live"]
    execute: bool = False


class VolatilityHarvestAdminDefaultsPayload(BaseModel):
    max_volatility_pct: float = 12.0
    default_harvest_bands: list[dict[str, Any]] = Field(default_factory=list)
    fee_assumptions: dict[str, Any] = Field(default_factory=dict)
    emergency_disable: bool = False
    suspend_rebuys_on_btc_breakdown: bool = True


class VolatilityHarvestTokenOption(BaseModel):
    symbol: str
    display_name: str | None = None
    ecosystem: str | None = None
    strategy_policy_status: str
    strategy_policy_reason: str | None = None
    user_enabled: bool
    admin_enabled: bool
    volatility_score: float | None = None
    liquidity_score: float | None = None


class VolatilityHarvestPositionOut(BaseModel):
    symbol: str
    core_quantity: str
    trading_quantity: str
    avg_core_entry_price: str
    avg_trading_entry_price: str
    harvested_cash_cents: int
    realized_gains_cents: int
    unrealized_gains_cents: int
    total_harvested_gains_cents: int
    token_accumulation_quantity: str
    token_accumulation_pct: str
    last_harvest_at: datetime | None = None
    last_rebuy_at: datetime | None = None


class VolatilityHarvestTransactionOut(BaseModel):
    id: UUID
    symbol: str
    transaction_type: str
    bucket_type: str | None = None
    band_code: str | None = None
    quantity: str
    price: str
    gross_notional_cents: int
    fee_cents: int
    slippage_cents: int
    net_profit_cents: int
    harvested_cash_balance_cents: int
    token_quantity_after: str
    occurred_at: datetime
    metadata: dict[str, Any] | None = None
