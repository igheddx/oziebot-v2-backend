"""Day trading strategy - intraday trading with same-day exit."""

from datetime import timedelta
from decimal import Decimal
from statistics import mean
from uuid import UUID

from oziebot_common.strategy_defaults import strategy_platform_config
from oziebot_domain.strategy import SignalType, StrategySignal
from oziebot_domain.trading import Instrument, OrderType, Quantity, Side
from oziebot_strategy_engine.strategy import StrategyContext, TradingStrategy

_DEFAULT_CONFIG = strategy_platform_config("day_trading")["strategy_params"]


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
        entry_threshold = config.get(
            "entry_threshold", _DEFAULT_CONFIG["entry_threshold"]
        )
        exit_threshold = config.get("exit_threshold", _DEFAULT_CONFIG["exit_threshold"])
        stop_loss = config.get(
            "stop_loss_pct", config.get("stop_loss", _DEFAULT_CONFIG["stop_loss_pct"])
        )
        position_size_fraction = float(
            config.get(
                "position_size_fraction", _DEFAULT_CONFIG["position_size_fraction"]
            )
        )
        min_volume_multiplier = float(
            config.get(
                "min_volume_multiplier", _DEFAULT_CONFIG["min_volume_multiplier"]
            )
        )
        min_volatility_pct = float(
            config.get("min_volatility_pct", _DEFAULT_CONFIG["min_volatility_pct"])
        )
        min_entry_signals = int(
            config.get(
                "min_entry_confirmations",
                config.get(
                    "min_entry_signals", _DEFAULT_CONFIG["min_entry_confirmations"]
                ),
            )
        )
        max_position_age_hours = int(
            config.get(
                "max_position_age_hours", _DEFAULT_CONFIG["max_position_age_hours"]
            )
        )
        trailing_stop_pct = float(
            config.get("trailing_stop_pct", _DEFAULT_CONFIG["trailing_stop_pct"])
        )
        trailing_stop_activation_pct = float(
            config.get(
                "trailing_stop_activation_pct",
                _DEFAULT_CONFIG["trailing_stop_activation_pct"],
            )
        )
        partial_take_profit_pct = float(
            config.get(
                "partial_take_profit_pct", _DEFAULT_CONFIG["partial_take_profit_pct"]
            )
        )
        partial_take_profit_fraction = float(
            config.get(
                "partial_take_profit_fraction",
                _DEFAULT_CONFIG["partial_take_profit_fraction"],
            )
        )
        min_trade_usd = float(
            config.get("min_trade_usd", _DEFAULT_CONFIG["min_trade_usd"])
        )
        max_trade_usd = float(
            config.get("max_trade_usd", _DEFAULT_CONFIG["max_trade_usd"])
        )
        target_bucket_utilization_pct = float(
            config.get("target_bucket_utilization_pct", 0.55)
        )
        drawdown_reduction_multiplier = float(
            config.get("drawdown_reduction_multiplier", 0.75)
        )

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
        if not (0.0 < trailing_stop_pct <= 1.0):
            raise ValueError(
                f"trailing_stop_pct must be >0 and <=1, got {trailing_stop_pct}"
            )
        if not (0.0 < trailing_stop_activation_pct <= 1.0):
            raise ValueError(
                "trailing_stop_activation_pct must be >0 and <=1, "
                f"got {trailing_stop_activation_pct}"
            )
        if not (0.0 < partial_take_profit_pct <= 1.0):
            raise ValueError(
                "partial_take_profit_pct must be >0 and <=1, "
                f"got {partial_take_profit_pct}"
            )
        if not (0.0 < partial_take_profit_fraction < 1.0):
            raise ValueError(
                "partial_take_profit_fraction must be >0 and <1, "
                f"got {partial_take_profit_fraction}"
            )
        if not (0.0 <= min_trade_usd <= max_trade_usd):
            raise ValueError(
                f"min_trade_usd must be >=0 and <= max_trade_usd ({max_trade_usd}), got {min_trade_usd}"
            )
        if not (0.0 <= target_bucket_utilization_pct <= 1.0):
            raise ValueError(
                "target_bucket_utilization_pct must be 0-1, "
                f"got {target_bucket_utilization_pct}"
            )
        if not (0.0 <= drawdown_reduction_multiplier <= 1.0):
            raise ValueError(
                "drawdown_reduction_multiplier must be 0-1, "
                f"got {drawdown_reduction_multiplier}"
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
        entry_threshold = float(
            config.get("entry_threshold", _DEFAULT_CONFIG["entry_threshold"])
        )
        exit_threshold = float(
            config.get("exit_threshold", _DEFAULT_CONFIG["exit_threshold"])
        )
        stop_loss = float(
            config.get(
                "stop_loss_pct",
                config.get("stop_loss", _DEFAULT_CONFIG["stop_loss_pct"]),
            )
        )
        position_size_fraction = float(
            config.get(
                "position_size_fraction", _DEFAULT_CONFIG["position_size_fraction"]
            )
        )
        min_volume_multiplier = float(
            config.get(
                "min_volume_multiplier", _DEFAULT_CONFIG["min_volume_multiplier"]
            )
        )
        min_volatility_pct = float(
            config.get("min_volatility_pct", _DEFAULT_CONFIG["min_volatility_pct"])
        )
        require_trend_alignment = bool(config.get("require_trend_alignment", True))
        breakout_lookback_candles = int(
            config.get(
                "breakout_lookback_candles",
                _DEFAULT_CONFIG["breakout_lookback_candles"],
            )
        )
        min_entry_signals = int(
            config.get(
                "min_entry_confirmations",
                config.get(
                    "min_entry_signals", _DEFAULT_CONFIG["min_entry_confirmations"]
                ),
            )
        )
        max_position_age_hours = int(
            config.get(
                "max_position_age_hours", _DEFAULT_CONFIG["max_position_age_hours"]
            )
        )
        trailing_stop_pct = float(
            config.get("trailing_stop_pct", _DEFAULT_CONFIG["trailing_stop_pct"])
        )
        trailing_stop_activation_pct = float(
            config.get(
                "trailing_stop_activation_pct",
                _DEFAULT_CONFIG["trailing_stop_activation_pct"],
            )
        )
        partial_take_profit_pct = float(
            config.get(
                "partial_take_profit_pct", _DEFAULT_CONFIG["partial_take_profit_pct"]
            )
        )
        partial_take_profit_fraction = float(
            config.get(
                "partial_take_profit_fraction",
                _DEFAULT_CONFIG["partial_take_profit_fraction"],
            )
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
                max_position_age_hours,
                trailing_stop_pct,
                trailing_stop_activation_pct,
                partial_take_profit_pct,
                partial_take_profit_fraction,
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
        max_position_age_hours: int,
        trailing_stop_pct: float,
        trailing_stop_activation_pct: float,
        partial_take_profit_pct: float,
        partial_take_profit_fraction: float,
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

        if (
            pnl >= partial_take_profit_pct
            and not position.partial_profit_taken
            and not position.partial_profit_pending
        ):
            partial_quantity = abs(position.quantity) * Decimal(
                str(partial_take_profit_fraction)
            )
            if partial_quantity > 0 and partial_quantity < abs(position.quantity):
                return self._close_signal(
                    context,
                    signal_id,
                    correlation_id,
                    f"Partial profit captured: {pnl:.2%}",
                    reason_code="partial_take_profit",
                    quantity=partial_quantity,
                    metadata={
                        "partial_take_profit_pct": partial_take_profit_pct,
                        "partial_take_profit_fraction": partial_take_profit_fraction,
                    },
                )

        peak_price = position.peak_price or max(
            position.entry_price, market.current_price
        )
        current_price = market.current_price
        if peak_price > 0 and pnl >= Decimal(str(trailing_stop_activation_pct)):
            retracement = (peak_price - current_price) / peak_price
            if retracement >= Decimal(str(trailing_stop_pct)):
                return self._close_signal(
                    context,
                    signal_id,
                    correlation_id,
                    (
                        f"Trailing stop hit: retracement={retracement:.2%} "
                        f"from peak={peak_price:.6f}"
                    ),
                    reason_code="trailing_stop_hit",
                    metadata={
                        "trailing_stop_pct": trailing_stop_pct,
                        "trailing_stop_activation_pct": trailing_stop_activation_pct,
                    },
                )

        if position.opened_at is not None:
            max_age = timedelta(hours=max_position_age_hours)
            held_for = market.timestamp - position.opened_at
            if held_for >= max_age:
                return self._close_signal(
                    context,
                    signal_id,
                    correlation_id,
                    f"Max position age reached: held_for={held_for} limit={max_age}",
                    reason_code="max_position_age_exceeded",
                    metadata={
                        "opened_at": position.opened_at.isoformat(),
                        "max_position_age_hours": max_position_age_hours,
                        "enforcement_source": "strategy_engine",
                    },
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
        return dict(_DEFAULT_CONFIG)

    def get_config_schema(self) -> dict:
        """Return JSON schema for config."""
        return {
            "type": "object",
            "properties": {
                "entry_threshold": {
                    "type": "number",
                    "minimum": 0.0,
                    "maximum": 0.5,
                    "default": _DEFAULT_CONFIG["entry_threshold"],
                    "description": "Price distance from low to trigger entry",
                },
                "exit_threshold": {
                    "type": "number",
                    "minimum": 0.001,
                    "maximum": 1.0,
                    "default": _DEFAULT_CONFIG["exit_threshold"],
                    "description": "Profit target as percentage",
                },
                "stop_loss_pct": {
                    "type": "number",
                    "minimum": 0.001,
                    "maximum": 1.0,
                    "default": _DEFAULT_CONFIG["stop_loss_pct"],
                    "description": "Stop loss as percentage",
                },
                "position_size_fraction": {
                    "type": "number",
                    "minimum": 0.01,
                    "maximum": 1.0,
                    "default": _DEFAULT_CONFIG["position_size_fraction"],
                    "description": "Fraction of capital to deploy per entry",
                },
                "max_position_age_hours": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": 24,
                    "default": _DEFAULT_CONFIG["max_position_age_hours"],
                    "description": "Hours to hold position max",
                },
                "min_volume_multiplier": {
                    "type": "number",
                    "minimum": 1.0,
                    "maximum": 10.0,
                    "default": _DEFAULT_CONFIG["min_volume_multiplier"],
                    "description": "Require the latest volume to exceed this multiple of average volume",
                },
                "min_volatility_pct": {
                    "type": "number",
                    "minimum": 0.0,
                    "maximum": 1.0,
                    "default": _DEFAULT_CONFIG["min_volatility_pct"],
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
                    "default": _DEFAULT_CONFIG["breakout_lookback_candles"],
                    "description": "Candles to inspect for a local breakout confirmation",
                },
                "min_entry_confirmations": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": 4,
                    "default": _DEFAULT_CONFIG["min_entry_confirmations"],
                    "description": "Minimum number of entry confirmation signals required",
                },
                "trailing_stop_pct": {
                    "type": "number",
                    "minimum": 0.001,
                    "maximum": 1.0,
                    "default": _DEFAULT_CONFIG["trailing_stop_pct"],
                    "description": "Exit if price retraces this far from peak after profit protection activates",
                },
                "trailing_stop_activation_pct": {
                    "type": "number",
                    "minimum": 0.001,
                    "maximum": 1.0,
                    "default": _DEFAULT_CONFIG["trailing_stop_activation_pct"],
                    "description": "Activate trailing protection after the trade reaches this profit threshold",
                },
                "partial_take_profit_pct": {
                    "type": "number",
                    "minimum": 0.001,
                    "maximum": 1.0,
                    "default": _DEFAULT_CONFIG["partial_take_profit_pct"],
                    "description": "Take a partial profit once price gains this much from entry",
                },
                "partial_take_profit_fraction": {
                    "type": "number",
                    "minimum": 0.01,
                    "maximum": 0.99,
                    "default": _DEFAULT_CONFIG["partial_take_profit_fraction"],
                    "description": "Fraction of the position to trim on the first profit capture",
                },
                "dynamic_sizing_enabled": {
                    "type": "boolean",
                    "default": True,
                    "description": "Scale trade size using bucket capital and utilization",
                },
                "min_trade_usd": {
                    "type": "number",
                    "minimum": 0,
                    "default": _DEFAULT_CONFIG["min_trade_usd"],
                    "description": "Minimum dynamic trade notional floor in USD",
                },
                "max_trade_usd": {
                    "type": "number",
                    "minimum": 1,
                    "default": _DEFAULT_CONFIG["max_trade_usd"],
                    "description": "Dynamic trade notional ceiling before risk caps",
                },
                "target_bucket_utilization_pct": {
                    "type": "number",
                    "minimum": 0,
                    "maximum": 1,
                    "default": _DEFAULT_CONFIG["target_bucket_utilization_pct"],
                    "description": "Target fraction of assigned bucket capital to keep deployed",
                },
                "drawdown_size_reduction_enabled": {
                    "type": "boolean",
                    "default": True,
                    "description": "Reduce trade size automatically during elevated drawdown",
                },
                "drawdown_reduction_multiplier": {
                    "type": "number",
                    "minimum": 0,
                    "maximum": 1,
                    "default": _DEFAULT_CONFIG["drawdown_reduction_multiplier"],
                    "description": "Multiplier applied when drawdown-aware sizing is active",
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
        *,
        reason_code: str | None = None,
        quantity: Decimal | None = None,
        metadata: dict[str, str | int | float] | None = None,
    ) -> StrategySignal:
        payload = {"exit_strategy": "day_trading"}
        if reason_code:
            payload["reason_code"] = reason_code
        if metadata:
            payload.update(metadata)
        return StrategySignal(
            signal_id=signal_id,
            correlation_id=correlation_id,
            tenant_id=context.tenant_id,
            strategy_id=self.strategy_id,
            strategy_version=self.version,
            trading_mode=context.trading_mode,
            signal_type=SignalType.CLOSE,
            quantity=Quantity(amount=str(quantity)) if quantity is not None else None,
            confidence=0.8,
            reason=reason,
            metadata=payload,
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
