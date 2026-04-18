"""Strategy signals - what strategies emit to indicate trading decisions."""

from decimal import Decimal
from enum import StrEnum
from uuid import UUID

from pydantic import Field

from oziebot_domain.tenant import TenantId
from oziebot_domain.trading import Instrument, OrderType, Quantity, Side
from oziebot_domain.trading_mode import TradingMode
from oziebot_domain.types import OziebotModel


class SignalType(StrEnum):
    """Signal types that strategies can emit."""

    BUY = "buy"
    SELL = "sell"
    HOLD = "hold"  # No action
    CLOSE = "close"  # Close position


class StrategySignal(OziebotModel):
    """
    Signal emitted by a strategy.
    
    Strategies evaluate market state and generate signals that recommend
    actions. Signals are then passed through risk/compliance checks before
    execution.
    
    Key design: Strategies do NOT directly interact with trading or execution.
    They only analyze state and emit signals.
    """

    signal_id: UUID = Field(description="Unique signal identifier")
    correlation_id: UUID = Field(
        description="Correlation ID to link related signals/execution"
    )
    tenant_id: TenantId = Field(description="User's tenant ID")
    strategy_id: str = Field(
        min_length=1, max_length=128, description="Strategy identifier (e.g. 'momentum')"
    )
    strategy_version: str = Field(
        default="1.0", description="Strategy version for tracking updates"
    )
    trading_mode: TradingMode = Field(
        description="PAPER (simulated) or LIVE (real money)"
    )
    signal_type: SignalType = Field(description="Type of signal: BUY, SELL, HOLD, CLOSE")
    
    # Optional trading details (populated for BUY/SELL signals)
    instrument: Instrument | None = Field(default=None, description="Trading pair")
    side: Side | None = Field(default=None, description="BUY or SELL")
    order_type: OrderType | None = Field(default=None, description="MARKET or LIMIT")
    quantity: Quantity | None = Field(default=None, description="Amount to trade")
    limit_price: Decimal | None = Field(
        default=None, description="Optional limit price for LIMIT orders"
    )
    
    # Strategy metadata
    confidence: float = Field(
        ge=0.0, le=1.0, default=0.5, description="Confidence in signal (0-1)"
    )
    reason: str = Field(default="", description="Human-readable reason for signal")
    metadata: dict | None = Field(
        default=None, description="Strategy-specific metadata (e.g. indicators)"
    )


class StrategyPerformanceMetrics(OziebotModel):
    """Track strategy performance over time."""

    strategy_id: str
    total_signals: int = 0
    buy_signals: int = 0
    sell_signals: int = 0
    hold_signals: int = 0
    close_signals: int = 0
    last_signal_at: str | None = None
    avg_confidence: float = 0.0
