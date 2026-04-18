"""Day trading strategy - intraday trading with same-day exit."""

from statistics import mean
from decimal import Decimal
from uuid import UUID

from oziebot_domain.strategy import SignalType, StrategySignal
from oziebot_domain.trading import Instrument, OrderType, Side
from oziebot_strategy_engine.strategy import StrategyContext, TradingStrategy


class DayTradingStrategy(TradingStrategy):
    """
    Day trading strategy - enters and exits positions within the same day.

    Configuration:
    - entry_threshold: Price vs low to trigger entry (default: 0.007)
    - exit_threshold: Profit target percentage (default: 0.015)
    - stop_loss_pct: Stop loss percentage (default: 0.008)
    - position_size_fraction: Fraction of capital to deploy per trade (default: 0.08)
    - max_position_age_hours: Force exit after N hours (default: 3 hours)
    """

    strategy_id = "day_trading"
    display_name = "Day Trading"
    description = "Intraday trading with same-day entry and exit"
    version = "1.0"

    def validate_config(self, config: dict) -> bool:
        """Validate day trading config."""
        entry_threshold = config.get("entry_threshold", 0.007)
        exit_threshold = config.get("exit_threshold", 0.015)
        stop_loss = config.get("stop_loss_pct", config.get("stop_loss", 0.008))
        position_size_fraction = float(config.get("position_size_fraction", 0.08))
        min_volume_multiplier = float(config.get("min_volume_multiplier", 1.3))
        min_volatility_pct = float(config.get("min_volatility_pct", 0.005))
        min_entry_signals = int(
            config.get("min_entry_confirmations", config.get("min_entry_signals", 1))
        )
        max_position_age_hours = int(config.get("max_position_age_hours", 3))

        if not (0.0 <= entry_threshold <= 0.5):
            raise ValueError(f"entry_threshold must be 0-0.5, got {entry_threshold}")
        if not (0.0 < exit_threshold <= 1.0):
            raise ValueError(f"exit_threshold must be >0 and <=1, got {exit_threshold}")
        if not (0.0 < stop_loss <= 1.0):
            raise ValueError(f"stop_loss must be >0 and <=1, got {stop_loss}")
        if not (0.01 <= position_size_fraction <= 1.0):
            raise ValueError(
                f"position_size_fraction must be 0.01-1.0, got {position_size_fraction}"
            )
        if not (1.0 <= min_volume_multiplier <= 10.0):
            raise ValueError(
                f"min_volume_multiplier must be 1.0-10.0, got {min_volume_multiplier}"
            )
        if not (0.0 <= min_volatility_pct <= 1.0):
            raise ValueError(
                f"min_volatility_pct must be 0.0-1.0, got {min_volatility_pct}"
            )
        if not (1 <= min_entry_signals <= 4):
            raise ValueError(f"min_entry_signals must be 1-4, got {min_entry_signals}")
        if not (1 <= max_position_age_hours <= 24):
            raise ValueError(
                f"max_position_age_hours must be 1-24, got {max_position_age_hours}"
            )

        return True

    def generate_signal(
        self,
        context: StrategyContext,
        config: dict,
        signal_id: UUID,
        correlation_id: UUID,
    ) -> StrategySignal:
        """Generate day trading signal using session high/low from candle history."""
        entry_threshold = float(config.get("entry_threshold", 0.007))
        exit_threshold = float(config.get("exit_threshold", 0.015))
        stop_loss = float(config.get("stop_loss_pct", config.get("stop_loss", 0.008)))
        position_size_fraction = float(config.get("position_size_fraction", 0.08))
        min_volume_multiplier = float(config.get("min_volume_multiplier", 1.3))
        min_volatility_pct = float(config.get("min_volatility_pct", 0.005))
        require_trend_alignment = bool(config.get("require_trend_alignment", True))
        breakout_lookback_candles = int(config.get("breakout_lookback_candles", 5))
        min_entry_signals = int(
            config.get("min_entry_confirmations", config.get("min_entry_signals", 1))
        )

        market = context.market_snapshot
        position = context.position_state

        closes: list[float] = market.metadata.get("candle_closes", [])
        candle_volumes: list[float] = market.metadata.get("candle_volumes", [])
        candle_highs: list[float] = market.metadata.get("candle_highs", [])
        candle_lows: list[float] = market.metadata.get("candle_lows", [])

        # Preserve stop-loss/profit exits even when only a single market snapshot is available.
        if position.quantity > 0:
            return self._check_exit(
                context,
                signal_id,
                correlation_id,
                position,
                market,
                exit_threshold,
                stop_loss,
            )

        # Need at least a few candles for a meaningful session range
        required_candles = max(21, breakout_lookback_candles + 1, 5)
        if len(closes) < required_candles:
            return self._generate_legacy_entry_signal(
                context,
                signal_id,
                correlation_id,
                entry_threshold,
                position_size_fraction,
            )

        # Use up to last 390 candles (~6.5 hours of 60s candles) for session range
        window = min(len(closes), 390)
        session_high = (
            max(candle_highs[-window:]) if candle_highs else float(market.high_price)
        )
        session_low = (
            min(candle_lows[-window:]) if candle_lows else float(market.low_price)
        )
        session_range = session_high - session_low

        current = float(market.current_price)

        # Entry: price near session low (buy the dip)
        if session_range > 0:
            distance_from_low = (current - session_low) / session_range
        else:
            distance_from_low = 0.5

        # Buy when price is in the lower entry_threshold of the session range
        if distance_from_low < entry_threshold:
            previous_volumes = (
                candle_volumes[-21:-1]
                if len(candle_volumes) >= 21
                else candle_volumes[:-1]
            )
            avg_volume = mean(previous_volumes) if previous_volumes else 0.0
            latest_volume = candle_volumes[-1] if candle_volumes else 0.0
            volume_spike = (
                avg_volume > 0 and latest_volume >= avg_volume * min_volume_multiplier
            )

            ema_fast = self._ema(closes[-21:], 9)
            ema_slow = self._ema(closes[-21:], 21)
            trend_alignment = ema_fast > ema_slow
            if require_trend_alignment and not trend_alignment:
                return self._hold_signal(
                    context,
                    signal_id,
                    correlation_id,
                    f"Trend alignment required: ema9={ema_fast:.4f} ema21={ema_slow:.4f}",
                )

            recent_highs = candle_highs[-(breakout_lookback_candles + 1) : -1]
            breakout = bool(recent_highs) and current >= max(recent_highs)

            volatility_window = closes[-10:]
            rolling_mean = mean(volatility_window) if volatility_window else current
            volatility_pct = (
                (max(volatility_window) - min(volatility_window)) / rolling_mean
                if volatility_window and rolling_mean > 0
                else 0.0
            )
            volatility_ok = volatility_pct >= min_volatility_pct

            confirmations = {
                "volume_spike": volume_spike,
                "trend_alignment": trend_alignment,
                "breakout": breakout,
                "volatility": volatility_ok,
            }
            confirmation_count = sum(1 for passed in confirmations.values() if passed)
            if confirmation_count < min_entry_signals:
                return self._hold_signal(
                    context,
                    signal_id,
                    correlation_id,
                    (
                        f"Entry confirmations too weak: got {confirmation_count}/{min_entry_signals} "
                        f"(volume={volume_spike}, trend={trend_alignment}, breakout={breakout}, volatility={volatility_ok})"
                    ),
                )

            return self._buy_signal(
                context,
                signal_id,
                correlation_id,
                min(0.95, 0.55 + (confirmation_count * 0.1)),
                position_size_fraction,
                f"Near session low with confirmations: {current:.2f} "
                f"(low={session_low:.2f} high={session_high:.2f} dist={distance_from_low:.1%}, "
                f"volume={volume_spike}, trend={trend_alignment}, breakout={breakout}, volatility={volatility_pct:.2%})",
            )

        return self._hold_signal(
            context,
            signal_id,
            correlation_id,
            f"Waiting for entry. dist_from_low={distance_from_low:.1%} threshold={entry_threshold:.1%}",
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

    def _generate_legacy_entry_signal(
        self,
        context: StrategyContext,
        signal_id: UUID,
        correlation_id: UUID,
        entry_threshold: float,
        position_size_fraction: float,
    ) -> StrategySignal:
        market = context.market_snapshot
        session_high = float(market.high_price)
        session_low = float(market.low_price)
        current = float(market.current_price)
        session_range = session_high - session_low

        if session_range <= 0 or session_low <= 0:
            return self._hold_signal(
                context,
                signal_id,
                correlation_id,
                "Insufficient history and invalid session range",
            )

        pct_above_low = (current - session_low) / session_low
        if pct_above_low <= entry_threshold:
            return self._buy_signal(
                context,
                signal_id,
                correlation_id,
                0.55,
                position_size_fraction,
                (
                    f"Near daily low (snapshot fallback): {current:.2f} "
                    f"(low={session_low:.2f} high={session_high:.2f} pct_above_low={pct_above_low:.1%})"
                ),
            )

        return self._hold_signal(
            context,
            signal_id,
            correlation_id,
            (
                f"Insufficient history: using snapshot fallback "
                f"(pct_above_low={pct_above_low:.1%} threshold={entry_threshold:.1%})"
            ),
        )

    def get_default_config(self) -> dict:
        """Return default configuration."""
        return {
            "entry_threshold": 0.007,
            "exit_threshold": 0.015,
            "stop_loss_pct": 0.008,
            "position_size_fraction": 0.08,
            "max_position_age_hours": 3,
            "min_volume_multiplier": 1.3,
            "min_volatility_pct": 0.005,
            "require_trend_alignment": True,
            "breakout_lookback_candles": 5,
            "min_entry_confirmations": 1,
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
                    "default": 0.007,
                    "description": "Price distance from low to trigger entry",
                },
                "exit_threshold": {
                    "type": "number",
                    "minimum": 0.001,
                    "maximum": 1.0,
                    "default": 0.015,
                    "description": "Profit target as percentage",
                },
                "stop_loss_pct": {
                    "type": "number",
                    "minimum": 0.001,
                    "maximum": 1.0,
                    "default": 0.008,
                    "description": "Stop loss as percentage",
                },
                "position_size_fraction": {
                    "type": "number",
                    "minimum": 0.01,
                    "maximum": 1.0,
                    "default": 0.08,
                    "description": "Fraction of capital to deploy per entry",
                },
                "max_position_age_hours": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": 24,
                    "default": 3,
                    "description": "Hours to hold position max",
                },
                "min_volume_multiplier": {
                    "type": "number",
                    "minimum": 1.0,
                    "maximum": 10.0,
                    "default": 1.3,
                    "description": "Require the latest volume to exceed this multiple of average volume",
                },
                "min_volatility_pct": {
                    "type": "number",
                    "minimum": 0.0,
                    "maximum": 1.0,
                    "default": 0.005,
                    "description": "Minimum recent volatility required before entering",
                },
                "require_trend_alignment": {
                    "type": "boolean",
                    "default": True,
                    "description": "If true, require EMA 9 to remain above EMA 21 before entering",
                },
                "breakout_lookback_candles": {
                    "type": "integer",
                    "minimum": 2,
                    "maximum": 30,
                    "default": 5,
                    "description": "Candles to inspect for a local breakout confirmation",
                },
                "min_entry_confirmations": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": 4,
                    "default": 1,
                    "description": "Minimum number of entry confirmation signals required",
                },
            },
        }

    def _buy_signal(
        self,
        context: StrategyContext,
        signal_id: UUID,
        correlation_id: UUID,
        confidence: float,
        position_size_fraction: float,
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
            confidence=confidence,
            reason=reason,
            metadata={
                "entry_strategy": "day_trading",
                "position_size_fraction": position_size_fraction,
            },
        )

    def _close_signal(
        self,
        context: StrategyContext,
        signal_id: UUID,
        correlation_id: UUID,
        reason: str,
    ) -> StrategySignal:
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
        self,
        context: StrategyContext,
        signal_id: UUID,
        correlation_id: UUID,
        reason: str,
    ) -> StrategySignal:
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

    @staticmethod
    def _ema(values: list[float], window: int) -> float:
        if not values:
            return 0.0
        series = values[-window:] if len(values) >= window else values
        alpha = 2 / (len(series) + 1)
        ema = series[0]
        for value in series[1:]:
            ema = (value * alpha) + (ema * (1 - alpha))
        return ema
