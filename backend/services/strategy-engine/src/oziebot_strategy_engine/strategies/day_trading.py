"""Day trading strategy - intraday trading with same-day exit."""

from decimal import Decimal
from uuid import UUID

from oziebot_domain.strategy import SignalType, StrategySignal
from oziebot_domain.trading import Instrument, OrderType, Side
from oziebot_strategy_engine.strategy import StrategyContext, TradingStrategy


class DayTradingStrategy(TradingStrategy):
    """
    Day trading strategy - enters and exits positions within the same day.
    
    Configuration:
    - entry_threshold: Price vs low to trigger entry (default: 0.01, i.e. 1% above day low)
    - exit_threshold: Profit target percentage (default: 0.02, i.e. 2% profit)
    - stop_loss: Stop loss percentage (default: 0.01, i.e. 1% max loss)
    - max_position_age_hours: Force exit after N hours (default: 4 hours)
    """

    strategy_id = "day_trading"
    display_name = "Day Trading"
    description = "Intraday trading with same-day entry and exit"
    version = "1.0"

    def validate_config(self, config: dict) -> bool:
        """Validate day trading config."""
        entry_threshold = config.get("entry_threshold", 0.01)
        exit_threshold = config.get("exit_threshold", 0.02)
        stop_loss = config.get("stop_loss", 0.01)

        if not (0.0 <= entry_threshold <= 0.5):
            raise ValueError(f"entry_threshold must be 0-0.5, got {entry_threshold}")
        if not (0.0 < exit_threshold <= 1.0):
            raise ValueError(f"exit_threshold must be >0 and <=1, got {exit_threshold}")
        if not (0.0 < stop_loss <= 1.0):
            raise ValueError(f"stop_loss must be >0 and <=1, got {stop_loss}")

        return True

    def generate_signal(
        self,
        context: StrategyContext,
        config: dict,
        signal_id: UUID,
        correlation_id: UUID,
    ) -> StrategySignal:
        """Generate day trading signal using session high/low from candle history."""
        entry_threshold = float(config.get("entry_threshold", 0.008))
        exit_threshold = float(config.get("exit_threshold", 0.015))
        stop_loss = float(config.get("stop_loss", 0.007))

        market = context.market_snapshot
        position = context.position_state

        closes: list[float] = market.metadata.get("candle_closes", [])
        candle_highs: list[float] = market.metadata.get("candle_highs", [])
        candle_lows: list[float] = market.metadata.get("candle_lows", [])

        # Need at least a few candles for a meaningful session range
        if len(closes) < 5:
            return self._hold_signal(
                context, signal_id, correlation_id,
                f"Insufficient history: {len(closes)} candles"
            )

        # Use up to last 390 candles (~6.5 hours of 60s candles) for session range
        window = min(len(closes), 390)
        session_high = max(candle_highs[-window:]) if candle_highs else float(market.high_price)
        session_low = min(candle_lows[-window:]) if candle_lows else float(market.low_price)
        session_range = session_high - session_low

        current = float(market.current_price)

        # If we have a position, check exit conditions
        if position.quantity > 0:
            return self._check_exit(
                context, signal_id, correlation_id,
                position, market, exit_threshold, stop_loss,
            )

        # Entry: price near session low (buy the dip)
        if session_range > 0:
            distance_from_low = (current - session_low) / session_range
        else:
            distance_from_low = 0.5

        # Buy when price is in the lower entry_threshold of the session range
        if distance_from_low < entry_threshold:
            return self._buy_signal(
                context, signal_id, correlation_id, config,
                f"Near session low: {current:.2f} (low={session_low:.2f} high={session_high:.2f} dist={distance_from_low:.1%})",
            )

        return self._hold_signal(
            context, signal_id, correlation_id,
            f"Waiting for entry. dist_from_low={distance_from_low:.1%} threshold={entry_threshold:.1%}"
        )

    def _check_exit(
        self,
        context: StrategyContext,
        signal_id: UUID,
        correlation_id: UUID,
        position,
        market,
        exit_threshold: float,
        stop_loss: float,
    ) -> StrategySignal:
        """Check if should exit position."""
        if position.entry_price is None or position.entry_price <= 0:
            return self._hold_signal(
                context, signal_id, correlation_id, "No entry price recorded"
            )

        # Calculate P&L
        pnl = (market.current_price - position.entry_price) / position.entry_price

        # Profit target reached
        if pnl >= exit_threshold:
            return self._close_signal(
                context,
                signal_id,
                correlation_id,
                f"Profit target reached: {pnl:.2%}",
            )

        # Stop loss triggered
        if pnl <= -stop_loss:
            return self._close_signal(
                context,
                signal_id,
                correlation_id,
                f"Stop loss triggered: {pnl:.2%}",
            )

        # Still holding
        return self._hold_signal(
            context, signal_id, correlation_id, f"Position P&L: {pnl:.2%}"
        )

    def get_default_config(self) -> dict:
        """Return default configuration."""
        return {
            "entry_threshold": 0.01,
            "exit_threshold": 0.02,
            "stop_loss": 0.01,
            "max_position_age_hours": 4,
        }

    def get_config_schema(self) -> dict:
        """Return JSON schema for config."""
        return {
            "type": "object",
            "properties": {
                "entry_threshold": {
                    "type": "number",
                    "minimum": 0.0,
                    "maximum": 0.5,
                    "default": 0.01,
                    "description": "Price distance from low to trigger entry",
                },
                "exit_threshold": {
                    "type": "number",
                    "minimum": 0.001,
                    "maximum": 1.0,
                    "default": 0.02,
                    "description": "Profit target as percentage",
                },
                "stop_loss": {
                    "type": "number",
                    "minimum": 0.001,
                    "maximum": 1.0,
                    "default": 0.01,
                    "description": "Stop loss as percentage",
                },
                "max_position_age_hours": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": 24,
                    "default": 4,
                    "description": "Hours to hold position max",
                },
            },
        }

    def _buy_signal(
        self,
        context: StrategyContext,
        signal_id: UUID,
        correlation_id: UUID,
        config: dict,
        reason: str,
    ) -> StrategySignal:
        market = context.market_snapshot

        return StrategySignal(
            signal_id=signal_id,
            correlation_id=correlation_id,
            tenant_id=context.tenant_id,
            strategy_id=self.strategy_id,
            strategy_version=self.version,
            trading_mode=context.trading_mode,
            signal_type=SignalType.BUY,
            instrument=Instrument(symbol=market.symbol),
            side=Side.BUY,
            order_type=OrderType.LIMIT,
            limit_price=market.current_price * Decimal("0.99"),  # 1% below current
            confidence=0.75,
            reason=reason,
            metadata={"entry_strategy": "day_trading", "position_size_fraction": 0.1},
        )

    def _close_signal(
        self, context: StrategyContext, signal_id: UUID, correlation_id: UUID, reason: str
    ) -> StrategySignal:
        market = context.market_snapshot

        return StrategySignal(
            signal_id=signal_id,
            correlation_id=correlation_id,
            tenant_id=context.tenant_id,
            strategy_id=self.strategy_id,
            strategy_version=self.version,
            trading_mode=context.trading_mode,
            signal_type=SignalType.CLOSE,
            confidence=0.8,
            reason=reason,
            metadata={"exit_strategy": "day_trading"},
        )

    def _hold_signal(
        self, context: StrategyContext, signal_id: UUID, correlation_id: UUID, reason: str
    ) -> StrategySignal:
        market = context.market_snapshot

        return StrategySignal(
            signal_id=signal_id,
            correlation_id=correlation_id,
            tenant_id=context.tenant_id,
            strategy_id=self.strategy_id,
            strategy_version=self.version,
            trading_mode=context.trading_mode,
            signal_type=SignalType.HOLD,
            confidence=0.5,
            reason=reason,
        )
