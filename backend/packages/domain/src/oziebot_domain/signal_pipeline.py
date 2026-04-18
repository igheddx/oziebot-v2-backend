from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from uuid import UUID

from pydantic import Field

from oziebot_domain.strategy import SignalType
from oziebot_domain.trading_mode import TradingMode
from oziebot_domain.types import OziebotModel


class StrategySignalEvent(OziebotModel):
    """Queue/persistence representation of a strategy-generated trade signal."""

    signal_id: UUID
    run_id: UUID
    user_id: UUID
    strategy_name: str = Field(min_length=1, max_length=128)
    symbol: str = Field(min_length=3, max_length=32)
    action: SignalType
    confidence: float = Field(ge=0.0, le=1.0)
    suggested_size: Decimal = Field(ge=0)
    reasoning_metadata: dict = Field(default_factory=dict)
    trading_mode: TradingMode
    timestamp: datetime
