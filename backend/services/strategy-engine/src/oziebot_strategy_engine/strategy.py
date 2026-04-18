"""Base strategy class - all trading strategies must inherit from this."""

from abc import ABC, abstractmethod
from datetime import datetime
from decimal import Decimal
from uuid import UUID

from oziebot_domain.strategy import StrategySignal
from oziebot_domain.tenant import TenantId
from oziebot_domain.trading_mode import TradingMode


class MarketSnapshot:
    """Current market state passed to strategies."""

    def __init__(
        self,
        timestamp: datetime,
        symbol: str,
        current_price: Decimal,
        bid_price: Decimal,
        ask_price: Decimal,
        volume_24h: Decimal,
        open_price: Decimal,
        high_price: Decimal,
        low_price: Decimal,
        close_price: Decimal,
        **metadata,
    ):
        self.timestamp = timestamp
        self.symbol = symbol
        self.current_price = current_price
        self.bid_price = bid_price
        self.ask_price = ask_price
        self.volume_24h = volume_24h
        self.open_price = open_price
        self.high_price = high_price
        self.low_price = low_price
        self.close_price = close_price
        self.metadata = metadata


class PositionState:
    """Current position data for strategy."""

    def __init__(
        self,
        symbol: str,
        quantity: Decimal = Decimal(0),
        entry_price: Decimal | None = None,
        peak_price: Decimal | None = None,
        opened_at: datetime | None = None,
    ):
        self.symbol = symbol
        self.quantity = quantity  # Can be negative for short positions
        self.entry_price = entry_price
        self.peak_price = peak_price
        self.opened_at = opened_at


class StrategyContext:
    """Context passed to strategy when generating signals."""

    def __init__(
        self,
        tenant_id: TenantId,
        trading_mode: TradingMode,
        market_snapshot: MarketSnapshot,
        position_state: PositionState,
        **kwargs,
    ):
        self.tenant_id = tenant_id
        self.trading_mode = trading_mode
        self.market_snapshot = market_snapshot
        self.position_state = position_state
        self.extra = kwargs  # For passing additional data


class TradingStrategy(ABC):
    """
    Base class for all trading strategies.

    Strategies are stateless signal generators. They evaluate market conditions
    and generate signals (BUY, SELL, HOLD, CLOSE) but do NOT execute trades.

    Execution, risk management, and compliance are handled by separate services.
    """

    # Subclasses must define these
    strategy_id: str
    display_name: str
    description: str
    version: str = "1.0"

    def __init__(self):
        """Initialize strategy."""
        pass

    @abstractmethod
    def validate_config(self, config: dict) -> bool:
        """
        Validate strategy configuration.

        Args:
            config: Strategy-specific configuration

        Returns:
            True if config is valid, raises ValueError otherwise
        """
        pass

    @abstractmethod
    def generate_signal(
        self,
        context: StrategyContext,
        config: dict,
        signal_id: UUID,
        correlation_id: UUID,
    ) -> StrategySignal:
        """
        Generate trading signal based on current market state.

        Args:
            context: Market and position data
            config: Strategy configuration
            signal_id: Unique ID for this signal
            correlation_id: Correlation ID to track signal chain

        Returns:
            StrategySignal with recommendation (BUY/SELL/HOLD/CLOSE)

        Must work identically in both PAPER and LIVE modes - only execution differs.
        """
        pass

    def get_default_config(self) -> dict:
        """Return default configuration for this strategy."""
        return {}

    def get_config_schema(self) -> dict:
        """
        Return JSON schema describing config parameters.

        Used by frontend to generate dynamic UI for strategy settings.

        Returns:
            Dict with schema for strategy parameters
        """
        return {}
