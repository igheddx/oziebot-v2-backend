from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field


class AllocationItemInput(BaseModel):
    strategy_id: str = Field(min_length=1, max_length=64)
    allocation_bps: int = Field(ge=0, le=10_000)


class ManualAllocationUpsert(BaseModel):
    total_capital_cents: int = Field(ge=0)
    allocations: list[AllocationItemInput]


class GuidedAllocationUpsert(BaseModel):
    total_capital_cents: int = Field(ge=0)
    preset_name: Literal["conservative", "balanced", "aggressive"]


class AllocationItemResponse(BaseModel):
    strategy_id: str
    allocation_bps: int
    assigned_capital_cents: int


class AllocationPlanResponse(BaseModel):
    trading_mode: str
    allocation_mode: str
    preset_name: str | None
    total_capital_cents: int
    items: list[AllocationItemResponse]


class StrategyBucketResponse(BaseModel):
    strategy_id: str
    trading_mode: str
    assigned_capital_cents: int
    available_cash_cents: int
    reserved_cash_cents: int
    locked_capital_cents: int
    realized_pnl_cents: int
    unrealized_pnl_cents: int
    available_buying_power_cents: int
    version: int
    updated_at: datetime


class StrategyBucketsResponse(BaseModel):
    trading_mode: str
    buckets: list[StrategyBucketResponse]


class ReserveCapitalRequest(BaseModel):
    strategy_id: str = Field(min_length=1, max_length=64)
    amount_cents: int = Field(gt=0)
    reference_id: str = Field(min_length=1, max_length=128)


class ReleaseCapitalRequest(BaseModel):
    strategy_id: str = Field(min_length=1, max_length=64)
    amount_cents: int = Field(gt=0)
    reference_id: str = Field(min_length=1, max_length=128)


class LockCapitalRequest(BaseModel):
    strategy_id: str = Field(min_length=1, max_length=64)
    amount_cents: int = Field(gt=0)
    reference_id: str = Field(min_length=1, max_length=128)


class SettleCapitalRequest(BaseModel):
    strategy_id: str = Field(min_length=1, max_length=64)
    released_locked_cents: int = Field(ge=0)
    realized_pnl_delta_cents: int
    reference_id: str = Field(min_length=1, max_length=128)


class UnrealizedPnlRequest(BaseModel):
    strategy_id: str = Field(min_length=1, max_length=64)
    unrealized_pnl_cents: int
    reference_id: str = Field(min_length=1, max_length=128)
