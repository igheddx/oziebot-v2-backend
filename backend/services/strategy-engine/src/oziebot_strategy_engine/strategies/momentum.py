"""Momentum trading strategy - buy when price is rising, sell when falling."""

from datetime import timedelta
from decimal import Decimal
from uuid import UUID

from oziebot_common.strategy_defaults import strategy_platform_config
from oziebot_domain.strategy import SignalType, StrategySignal
from oziebot_domain.trading import Instrument, OrderType, Quantity, Side
from oziebot_strategy_engine.strategy import StrategyContext, TradingStrategy

_DEFAULT_CONFIG = strategy_platform_config("momentum")["strategy_params"]


class MomentumStrategy(TradingStrategy):
    """
    Momentum strategy - trades based on price momentum.

    Configuration:
    - short_window: Lookback period for short MA
    - long_window: Lookback period for long MA
    - strength_threshold: Minimum momentum to generate signal
    - position_size_fraction: Fraction of capital to use per trade
    - stop_loss_pct: Hard stop below entry
    - take_profit_pct: Hard take-profit above entry
    - trailing_stop_pct: Exit when price retraces from peak
    - max_hold_minutes: Max time to keep an open position
    """

    strategy_id = "momentum"
    display_name = "Momentum Trading"
    description = "Trades based on price momentum with moving averages"
    version = "1.0"

    def validate_config(self, config: dict) -> bool:
        """Validate momentum config."""
        short_window = config.get("short_window", config.get("short_ma_window", 10))
        long_window = config.get("long_window", config.get("long_ma_window", 40))
        strength_threshold = config.get(
            "strength_threshold", config.get("entry_threshold", 0.02)
        )
        position_size = config.get(
            "position_size_fraction",
            config.get("position_size", _DEFAULT_CONFIG["position_size_fraction"]),
        )
        stop_loss_pct = config.get("stop_loss_pct", 0.035)
        take_profit_pct = config.get("take_profit_pct", 0.045)
        trailing_stop_pct = config.get("trailing_stop_pct", 0.018)
        partial_take_profit_pct = config.get("partial_take_profit_pct", 0.02)
        partial_take_profit_fraction = config.get("partial_take_profit_fraction", 0.5)
        min_volume_multiplier = config.get("min_volume_multiplier", 1.2)
        max_hold_minutes = config.get(
            "max_hold_minutes", _DEFAULT_CONFIG["max_hold_minutes"]
        )
        min_trade_usd = float(
            config.get("min_trade_usd", _DEFAULT_CONFIG["min_trade_usd"])
        )
        max_trade_usd = float(
            config.get("max_trade_usd", _DEFAULT_CONFIG["max_trade_usd"])
        )
        target_bucket_utilization_pct = float(
            config.get("target_bucket_utilization_pct", 0.65)
        )
        drawdown_reduction_multiplier = float(
            config.get("drawdown_reduction_multiplier", 0.75)
        )

        if not (1 <= short_window < long_window):
            raise ValueError(
                f"short_window ({short_window}) must be >= 1 and < long_window ({long_window})"
            )
        if not (0.0 <= strength_threshold <= 1.0):
            raise ValueError(
                f"strength_threshold must be 0-1, got {strength_threshold}"
            )
        if not (0.0 < position_size <= 1.0):
            raise ValueError(f"position_size must be 0-1, got {position_size}")
        if not (0.0 < stop_loss_pct <= 1.0):
            raise ValueError(f"stop_loss_pct must be >0 and <=1, got {stop_loss_pct}")
        if not (0.0 < take_profit_pct <= 1.0):
            raise ValueError(
                f"take_profit_pct must be >0 and <=1, got {take_profit_pct}"
            )
        if not (0.0 < trailing_stop_pct <= 1.0):
            raise ValueError(
                f"trailing_stop_pct must be >0 and <=1, got {trailing_stop_pct}"
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
        if not (1.0 <= min_volume_multiplier <= 10.0):
            raise ValueError(
                f"min_volume_multiplier must be 1.0-10.0, got {min_volume_multiplier}"
            )
        if not (1 <= max_hold_minutes <= 10_080):
            raise ValueError(
                f"max_hold_minutes must be between 1 and 10080, got {max_hold_minutes}"
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
        """Generate momentum signal using short/long moving average crossover."""
        short_window = int(
            config.get("short_window", config.get("short_ma_window", 10))
        )
        long_window = int(config.get("long_window", config.get("long_ma_window", 40)))
        strength_threshold = float(
            config.get("strength_threshold", config.get("entry_threshold", 0.02))
        )
        stop_loss_pct = float(
            config.get("stop_loss_pct", _DEFAULT_CONFIG["stop_loss_pct"])
        )
        take_profit_pct = float(
            config.get("take_profit_pct", _DEFAULT_CONFIG["take_profit_pct"])
        )
        trailing_stop_pct = float(
            config.get("trailing_stop_pct", _DEFAULT_CONFIG["trailing_stop_pct"])
        )
        partial_take_profit_pct = float(config.get("partial_take_profit_pct", 0.02))
        partial_take_profit_fraction = float(
            config.get("partial_take_profit_fraction", 0.5)
        )
        min_volume_multiplier = float(config.get("min_volume_multiplier", 1.2))
        max_hold_minutes = int(
            config.get("max_hold_minutes", _DEFAULT_CONFIG["max_hold_minutes"])
        )

        market = context.market_snapshot
        position = context.position_state

        closes: list[float] = market.metadata.get("candle_closes", [])
        candle_volumes: list[float] = market.metadata.get("candle_volumes", [])

        # Need at least long_window candles for a valid signal
        if len(closes) < long_window:
            return self._hold_signal(
                context,
                signal_id,
                correlation_id,
                f"Insufficient history: {len(closes)}/{long_window} candles",
            )

        short_ma = sum(closes[-short_window:]) / short_window
        long_ma = sum(closes[-long_window:]) / long_window

        # Momentum = how far short MA is above/below long MA (normalised)
        momentum = (short_ma - long_ma) / long_ma

        if position.quantity > 0:
            managed_exit = self._check_exit(
                context=context,
                signal_id=signal_id,
                correlation_id=correlation_id,
                position=position,
                market=market,
                momentum=momentum,
                strength_threshold=strength_threshold,
                stop_loss_pct=stop_loss_pct,
                take_profit_pct=take_profit_pct,
                trailing_stop_pct=trailing_stop_pct,
                partial_take_profit_pct=partial_take_profit_pct,
                partial_take_profit_fraction=partial_take_profit_fraction,
                max_hold_minutes=max_hold_minutes,
            )
            if managed_exit is not None:
                return managed_exit

        if momentum > strength_threshold:
            if position.quantity <= 0:
                previous_volumes = (
                    candle_volumes[-21:-1]
                    if len(candle_volumes) >= 21
                    else candle_volumes[:-1]
                )
                avg_volume = (
                    sum(previous_volumes) / len(previous_volumes)
                    if previous_volumes
                    else 0.0
                )
                latest_volume = candle_volumes[-1] if candle_volumes else 0.0
                if (
                    avg_volume <= 0
                    or latest_volume < avg_volume * min_volume_multiplier
                ):
                    return self._hold_signal(
                        context,
                        signal_id,
                        correlation_id,
                        (
                            "Volume filter blocked entry: "
                            f"latest={latest_volume:.2f} avg={avg_volume:.2f} "
                            f"min_multiplier={min_volume_multiplier:.2f}"
                        ),
                    )
                return self._buy_signal(
                    context,
                    signal_id,
                    correlation_id,
                    config,
                    f"MA crossover bullish: short_ma={short_ma:.2f} long_ma={long_ma:.2f} momentum={momentum:.3%}",
                )
            return self._hold_signal(
                context,
                signal_id,
                correlation_id,
                f"Already long. momentum={momentum:.3%}",
            )
        elif momentum < -strength_threshold:
            if position.quantity > 0:
                return self._close_signal(
                    context,
                    signal_id,
                    correlation_id,
                    f"MA crossover bearish: momentum={momentum:.3%}",
                )
            return self._hold_signal(
                context,
                signal_id,
                correlation_id,
                f"No position to close. momentum={momentum:.3%}",
            )
        else:
            return self._hold_signal(
                context,
                signal_id,
                correlation_id,
                f"Neutral momentum={momentum:.3%} (threshold ±{strength_threshold:.3%})",
            )

    def get_default_config(self) -> dict:
        """Return default configuration."""
        return dict(_DEFAULT_CONFIG)

    def get_config_schema(self) -> dict:
        """Return JSON schema for config validation."""
        return {
            "type": "object",
            "properties": {
                "short_window": {
                    "type": "integer",
                    "minimum": 1,
                    "default": _DEFAULT_CONFIG["short_window"],
                    "description": "Short moving average window",
                },
                "long_window": {
                    "type": "integer",
                    "minimum": 2,
                    "default": _DEFAULT_CONFIG["long_window"],
                    "description": "Long moving average window",
                },
                "strength_threshold": {
                    "type": "number",
                    "minimum": 0.0,
                    "maximum": 1.0,
                    "default": _DEFAULT_CONFIG["strength_threshold"],
                    "description": "Minimum momentum to trade (e.g. 0.02 = 2%)",
                },
                "position_size_fraction": {
                    "type": "number",
                    "minimum": 0.01,
                    "maximum": 1.0,
                    "default": _DEFAULT_CONFIG["position_size_fraction"],
                    "description": "Fraction of capital to use per trade",
                },
                "stop_loss_pct": {
                    "type": "number",
                    "minimum": 0.001,
                    "maximum": 1.0,
                    "default": _DEFAULT_CONFIG["stop_loss_pct"],
                    "description": "Exit if price falls this far below entry",
                },
                "take_profit_pct": {
                    "type": "number",
                    "minimum": 0.001,
                    "maximum": 1.0,
                    "default": _DEFAULT_CONFIG["take_profit_pct"],
                    "description": "Take profit once price rises this far above entry",
                },
                "trailing_stop_pct": {
                    "type": "number",
                    "minimum": 0.001,
                    "maximum": 1.0,
                    "default": _DEFAULT_CONFIG["trailing_stop_pct"],
                    "description": "Exit if price retraces this far from peak after entry",
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
                "min_volume_multiplier": {
                    "type": "number",
                    "minimum": 1.0,
                    "maximum": 10.0,
                    "default": _DEFAULT_CONFIG["min_volume_multiplier"],
                    "description": "Require the latest volume to exceed this multiple of recent average volume",
                },
                "max_hold_minutes": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": 10080,
                    "default": _DEFAULT_CONFIG["max_hold_minutes"],
                    "description": "Maximum time to hold an open position",
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
            "required": ["short_window", "long_window"],
        }

    def _check_exit(
        self,
        *,
        context: StrategyContext,
        signal_id: UUID,
        correlation_id: UUID,
        position,
        market,
        momentum: float,
        strength_threshold: float,
        stop_loss_pct: float,
        take_profit_pct: float,
        trailing_stop_pct: float,
        partial_take_profit_pct: float,
        partial_take_profit_fraction: float,
        max_hold_minutes: int,
    ) -> StrategySignal | None:
        if position.entry_price is None or position.entry_price <= 0:
            return self._hold_signal(
                context,
                signal_id,
                correlation_id,
                "Already long but entry price is missing",
            )

        current_price = market.current_price
        entry_price = position.entry_price
        pnl = (current_price - entry_price) / entry_price

        if pnl <= Decimal(str(-stop_loss_pct)):
            return self._close_signal(
                context,
                signal_id,
                correlation_id,
                f"Stop loss hit: pnl={pnl:.2%} threshold=-{stop_loss_pct:.2%}",
            )

        if pnl >= Decimal(str(take_profit_pct)):
            return self._close_signal(
                context,
                signal_id,
                correlation_id,
                f"Take profit hit: pnl={pnl:.2%} threshold={take_profit_pct:.2%}",
            )

        peak_price = position.peak_price or max(entry_price, current_price)
        if peak_price > 0 and current_price < peak_price:
            retracement = (peak_price - current_price) / peak_price
            if retracement >= Decimal(str(trailing_stop_pct)) and pnl > 0:
                return self._close_signal(
                    context,
                    signal_id,
                    correlation_id,
                    f"Trailing stop hit: retracement={retracement:.2%} from peak={peak_price:.6f}",
                )

        opened_at = position.opened_at
        if opened_at is not None:
            max_hold = timedelta(minutes=max_hold_minutes)
            held_for = market.timestamp - opened_at
            if held_for >= max_hold:
                return self._close_signal(
                    context,
                    signal_id,
                    correlation_id,
                    f"Max hold reached: held_for={held_for} limit={max_hold}",
                )

        if (
            pnl >= Decimal(str(partial_take_profit_pct))
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
                    (
                        f"Partial profit captured: pnl={pnl:.2%} "
                        f"fraction={partial_take_profit_fraction:.0%}"
                    ),
                    reason_code="partial_take_profit",
                    quantity=partial_quantity,
                    metadata={"partial_take_profit_pct": partial_take_profit_pct},
                )

        if momentum < -strength_threshold:
            return self._close_signal(
                context,
                signal_id,
                correlation_id,
                f"MA crossover bearish: momentum={momentum:.3%}",
            )

        return None

    def _buy_signal(
        self,
        context: StrategyContext,
        signal_id: UUID,
        correlation_id: UUID,
        config: dict,
        reason: str,
    ) -> StrategySignal:
        market = context.market_snapshot
        position_size = config.get(
            "position_size_fraction",
            config.get("position_size", _DEFAULT_CONFIG["position_size_fraction"]),
        )

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
            order_type=OrderType.MARKET,
            quantity=None,  # Will be calculated by execution layer
            confidence=0.7,
            reason=reason,
            metadata={
                "position_size_fraction": position_size,
                "price": str(market.current_price),
            },
        )

    def _sell_signal(
        self,
        context: StrategyContext,
        signal_id: UUID,
        correlation_id: UUID,
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
            signal_type=SignalType.SELL,
            instrument=Instrument(symbol=market.symbol),
            side=Side.SELL,
            order_type=OrderType.MARKET,
            confidence=0.6,
            reason=reason,
            metadata={"price": str(market.current_price)},
        )

    def _hold_signal(
        self,
        context: StrategyContext,
        signal_id: UUID,
        correlation_id: UUID,
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
            signal_type=SignalType.HOLD,
            confidence=0.5,
            reason=reason,
            metadata={"price": str(market.current_price)},
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
        metadata: dict[str, float] | None = None,
    ) -> StrategySignal:
        market = context.market_snapshot
        payload = {"price": str(market.current_price)}
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
            confidence=0.7,
            reason=reason,
            metadata=payload,
        )
