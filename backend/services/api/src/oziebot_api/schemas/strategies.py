"""Schemas for user strategy configuration."""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field


# ============================================================================
# Strategy List and Info
# ============================================================================


class StrategyMetadata(BaseModel):
    """Metadata about an available strategy."""

    strategy_id: str
    display_name: str
    description: str
    version: str
    config_schema: dict[str, Any]


class AvailableStrategiesResponse(BaseModel):
    """List of available strategies."""

    total: int
    strategies: list[StrategyMetadata]


# ============================================================================
# User Strategy Configuration
# ============================================================================


class UserStrategyCreate(BaseModel):
    """Request to create/add a strategy for a user."""

    strategy_id: str = Field(min_length=1, max_length=64)
    is_enabled: bool = Field(default=True)
    config: dict[str, Any] = Field(default_factory=dict)


class UserStrategyUpdate(BaseModel):
    """Request to update strategy configuration."""

    is_enabled: bool | None = None
    config: dict[str, Any] | None = None


class UserStrategyResponse(BaseModel):
    """User's strategy configuration."""

    id: UUID
    strategy_id: str
    is_enabled: bool
    config: dict[str, Any]
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class UserStrategiesListResponse(BaseModel):
    """List of user's strategies."""

    total: int
    enabled_count: int
    strategies: list[UserStrategyResponse]


# ============================================================================
# Strategy Performance
# ============================================================================


class StrategySignalResponse(BaseModel):
    """Individual strategy signal log entry."""

    id: UUID
    strategy_id: str
    signal_type: str
    trading_mode: str
    symbol: str | None
    confidence: float
    reason: str
    created_at: datetime

    model_config = {"from_attributes": True}


class StrategyPerformanceResponse(BaseModel):
    """Strategy performance metrics."""

    strategy_id: str
    trading_mode: str
    total_signals: int
    buy_signals: int
    sell_signals: int
    hold_signals: int
    close_signals: int
    avg_confidence: float
    last_signal_at: datetime | None

    model_config = {"from_attributes": True}


class StrategyPerformanceListResponse(BaseModel):
    """List of strategy performance metrics."""

    strategies: list[StrategyPerformanceResponse]


class UserStrategyStateUpsert(BaseModel):
    """Upsert runtime state for a strategy in a specific trading mode."""

    trading_mode: str = Field(pattern="^(paper|live)$")
    state: dict[str, Any] = Field(default_factory=dict)


class UserStrategyStateResponse(BaseModel):
    """Runtime state for a user's strategy."""

    strategy_id: str
    trading_mode: str
    state: dict[str, Any]
    updated_at: datetime

    model_config = {"from_attributes": True}
