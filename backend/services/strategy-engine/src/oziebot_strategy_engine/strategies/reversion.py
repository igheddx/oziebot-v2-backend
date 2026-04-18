"""Mean reversion strategy using rolling z-score, RSI, and managed exits."""

from __future__ import annotations

from datetime import timedelta
from decimal import Decimal
from math import sqrt
from statistics import mean
from uuid import UUID

from oziebot_domain.strategy import SignalType, StrategySignal
from oziebot_domain.trading import Instrument, OrderType, Side
from oziebot_strategy_engine.strategy import (
    MarketSnapshot,
    PositionState,
    StrategyContext,
    TradingStrategy,
)


class ReversionStrategy(TradingStrategy):
    """
    Long-only mean reversion strategy.

    Entries look for statistically stretched downside moves using a rolling z-score,
    oversold RSI, and a minimum band-width filter so the strategy does not trade flat
    or noisy ranges. Open positions use layered exits so trades can close cleanly on
    a snap-back, stop-loss, take-profit, or max hold timeout.
    """

    strategy_id = "reversion"
    display_name = "Mean Reversion"
    description = "Contrarian entries on stretched downside moves with managed exits"
    version = "1.1"

    def validate_config(self, config: dict) -> bool:
        band_window = int(config.get("band_window", 20))
        rsi_period = int(config.get("rsi_period", 14))
        entry_zscore = float(config.get("entry_zscore", 1.8))
        exit_zscore = float(config.get("exit_zscore", 0.35))
        rsi_buy_threshold = float(config.get("rsi_buy_threshold", 32))
        rsi_exit_threshold = float(config.get("rsi_exit_threshold", 52))
        rsi_sell_threshold = float(config.get("rsi_sell_threshold", 68))
        position_size = float(config.get("position_size", 0.06))
        stop_loss_pct = float(config.get("stop_loss_pct", 0.025))
        take_profit_pct = float(config.get("take_profit_pct", 0.045))
        min_bandwidth_pct = float(config.get("min_bandwidth_pct", 0.015))
        max_hold_minutes = int(config.get("max_hold_minutes", 180))
        ema_long_window = int(config.get("ema_long_window", 200))

        if not (5 <= band_window <= 200):
            raise ValueError(f"band_window must be 5-200, got {band_window}")
        if not (2 <= rsi_period <= 100):
            raise ValueError(f"rsi_period must be 2-100, got {rsi_period}")
        if not (0.1 <= entry_zscore <= 5.0):
            raise ValueError(f"entry_zscore must be 0.1-5.0, got {entry_zscore}")
        if not (0.0 <= exit_zscore < entry_zscore):
            raise ValueError(
                f"exit_zscore must be >=0 and < entry_zscore ({entry_zscore}), got {exit_zscore}"
            )
        if not (1 <= rsi_buy_threshold < rsi_exit_threshold < rsi_sell_threshold <= 99):
            raise ValueError(
                "RSI thresholds invalid: "
                f"buy={rsi_buy_threshold}, exit={rsi_exit_threshold}, sell={rsi_sell_threshold}"
            )
        if not (0.01 <= position_size <= 1.0):
            raise ValueError(f"position_size must be 0.01-1.0, got {position_size}")
        if not (0.001 <= stop_loss_pct <= 1.0):
            raise ValueError(f"stop_loss_pct must be 0.001-1.0, got {stop_loss_pct}")
        if not (0.001 <= take_profit_pct <= 1.0):
            raise ValueError(
                f"take_profit_pct must be 0.001-1.0, got {take_profit_pct}"
            )
        if not (0.0 <= min_bandwidth_pct <= 1.0):
            raise ValueError(
                f"min_bandwidth_pct must be 0.0-1.0, got {min_bandwidth_pct}"
            )
        if not (1 <= max_hold_minutes <= 10_080):
            raise ValueError(
                f"max_hold_minutes must be between 1 and 10080, got {max_hold_minutes}"
            )
        if not (5 <= ema_long_window <= 500):
            raise ValueError(
                f"ema_long_window must be between 5 and 500, got {ema_long_window}"
            )

        return True

    def generate_signal(
        self,
        context: StrategyContext,
        config: dict,
        signal_id: UUID,
        correlation_id: UUID,
    ) -> StrategySignal:
        market = context.market_snapshot
        position = context.position_state

        band_window = int(config.get("band_window", 20))
        rsi_period = int(config.get("rsi_period", 14))
        entry_zscore = float(config.get("entry_zscore", 1.8))
        exit_zscore = float(config.get("exit_zscore", 0.35))
        rsi_buy_threshold = float(config.get("rsi_buy_threshold", 32))
        rsi_exit_threshold = float(config.get("rsi_exit_threshold", 52))
        rsi_sell_threshold = float(config.get("rsi_sell_threshold", 68))
        position_size = float(config.get("position_size", 0.06))
        stop_loss_pct = float(config.get("stop_loss_pct", 0.025))
        take_profit_pct = float(config.get("take_profit_pct", 0.045))
        min_bandwidth_pct = float(config.get("min_bandwidth_pct", 0.015))
        max_hold_minutes = int(config.get("max_hold_minutes", 180))
        use_trend_filter = bool(config.get("use_trend_filter", True))
        ema_long_window = int(config.get("ema_long_window", 200))

        closes = [float(value) for value in market.metadata.get("candle_closes", [])]
        required = max(
            band_window,
            rsi_period + 1,
            ema_long_window if use_trend_filter and position.quantity <= 0 else 0,
        )
        if len(closes) < required:
            return self._hold_signal(
                context,
                signal_id,
                correlation_id,
                f"Insufficient history: {len(closes)}/{required} candles",
            )

        window = closes[-band_window:]
        rolling_mean = mean(window)
        if rolling_mean <= 0:
            return self._hold_signal(
                context,
                signal_id,
                correlation_id,
                "Rolling mean is invalid for reversion signal",
            )

        variance = sum((price - rolling_mean) ** 2 for price in window) / len(window)
        stdev = sqrt(max(variance, 0.0))
        if stdev <= 1e-9:
            return self._hold_signal(
                context,
                signal_id,
                correlation_id,
                "Insufficient dispersion for reversion signal",
            )

        price = float(market.current_price)
        zscore = (price - rolling_mean) / stdev
        bandwidth_pct = ((2 * stdev) / rolling_mean) * 2
        rsi_value = self._compute_rsi(closes[-(rsi_period + 1) :])

        use_fear_index_filter = bool(config.get("use_fear_index_filter", False))
        fear_index_buy_max = float(config.get("fear_index_buy_max", 35))
        fear_index_sell_min = float(config.get("fear_index_sell_min", 60))
        fear_index = self._fear_index(market)

        fear_buy_ok = True
        fear_sell_ok = True
        if use_fear_index_filter and fear_index is not None:
            fear_buy_ok = fear_index <= fear_index_buy_max
            fear_sell_ok = fear_index >= fear_index_sell_min

        market_regime = self._market_regime(market)
        ema_long = self._ema(closes[-ema_long_window:], ema_long_window)
        trend_buy_ok = (not use_trend_filter) or (
            price >= ema_long and market_regime != "bearish"
        )
        strong_downtrend = self._strong_downtrend(
            closes=closes,
            price=price,
            ema_long=ema_long,
            market_regime=market_regime,
        )

        if position.quantity > 0:
            managed_exit = self._check_exit(
                context=context,
                signal_id=signal_id,
                correlation_id=correlation_id,
                market=market,
                position=position,
                zscore=zscore,
                rsi_value=rsi_value,
                exit_zscore=exit_zscore,
                rsi_exit_threshold=rsi_exit_threshold,
                rsi_sell_threshold=rsi_sell_threshold,
                fear_sell_ok=fear_sell_ok,
                stop_loss_pct=stop_loss_pct,
                take_profit_pct=take_profit_pct,
                max_hold_minutes=max_hold_minutes,
            )
            if managed_exit is not None:
                return managed_exit
            return self._hold_signal(
                context,
                signal_id,
                correlation_id,
                f"Holding long: z={zscore:.2f}, rsi={rsi_value:.1f}, bandwidth={bandwidth_pct:.2%}",
            )

        if bandwidth_pct < min_bandwidth_pct:
            return self._hold_signal(
                context,
                signal_id,
                correlation_id,
                f"Range too tight: bandwidth={bandwidth_pct:.2%} min={min_bandwidth_pct:.2%}",
            )

        if not trend_buy_ok:
            return self._hold_signal(
                context,
                signal_id,
                correlation_id,
                f"Trend filter blocked entry: price={price:.4f}, ema_long={ema_long:.4f}, regime={market_regime or 'unknown'}",
            )

        if strong_downtrend:
            return self._hold_signal(
                context,
                signal_id,
                correlation_id,
                (
                    f"Strong downtrend blocked entry: price={price:.4f}, ema_long={ema_long:.4f}, "
                    f"regime={market_regime or 'unknown'}"
                ),
            )

        if zscore <= -entry_zscore and rsi_value <= rsi_buy_threshold and fear_buy_ok:
            confidence = min(
                0.9, 0.65 + abs(zscore) * 0.05 + max(0.0, (50 - rsi_value) / 100)
            )
            return self._buy_signal(
                context,
                signal_id,
                correlation_id,
                position_size=position_size,
                confidence=confidence,
                reason=(
                    f"Oversold reversion entry: z={zscore:.2f}, rsi={rsi_value:.1f}, "
                    f"mean={rolling_mean:.4f}, bandwidth={bandwidth_pct:.2%}"
                ),
                metadata={
                    "zscore": round(zscore, 4),
                    "rsi": round(rsi_value, 2),
                    "rolling_mean": round(rolling_mean, 8),
                    "bandwidth_pct": round(bandwidth_pct, 6),
                },
            )

        return self._hold_signal(
            context,
            signal_id,
            correlation_id,
            f"No entry: z={zscore:.2f}, rsi={rsi_value:.1f}, bandwidth={bandwidth_pct:.2%}",
        )

    def get_default_config(self) -> dict:
        return {
            "band_window": 20,
            "rsi_period": 14,
            "entry_zscore": 1.8,
            "exit_zscore": 0.35,
            "rsi_buy_threshold": 32,
            "rsi_exit_threshold": 52,
            "rsi_sell_threshold": 68,
            "position_size": 0.06,
            "stop_loss_pct": 0.025,
            "take_profit_pct": 0.045,
            "min_bandwidth_pct": 0.015,
            "max_hold_minutes": 180,
            "use_fear_index_filter": False,
            "fear_index_buy_max": 35,
            "fear_index_sell_min": 60,
            "use_trend_filter": True,
            "ema_long_window": 200,
        }

    def get_config_schema(self) -> dict:
        return {
            "type": "object",
            "properties": {
                "band_window": {
                    "type": "integer",
                    "minimum": 5,
                    "maximum": 200,
                    "default": 20,
                    "description": "Rolling window used for mean and standard deviation",
                },
                "rsi_period": {
                    "type": "integer",
                    "minimum": 2,
                    "maximum": 100,
                    "default": 14,
                    "description": "Lookback period used for RSI calculation",
                },
                "entry_zscore": {
                    "type": "number",
                    "minimum": 0.1,
                    "maximum": 5.0,
                    "default": 1.8,
                    "description": "Buy when price is this many standard deviations below the mean",
                },
                "exit_zscore": {
                    "type": "number",
                    "minimum": 0.0,
                    "maximum": 3.0,
                    "default": 0.35,
                    "description": "Exit once price reverts back near the rolling mean",
                },
                "rsi_buy_threshold": {
                    "type": "number",
                    "minimum": 1,
                    "maximum": 50,
                    "default": 32,
                    "description": "Buy only when RSI is at or below this value",
                },
                "rsi_exit_threshold": {
                    "type": "number",
                    "minimum": 20,
                    "maximum": 80,
                    "default": 52,
                    "description": "Exit once RSI normalises back above this level",
                },
                "rsi_sell_threshold": {
                    "type": "number",
                    "minimum": 50,
                    "maximum": 99,
                    "default": 68,
                    "description": "Prefer exits when the bounce becomes overbought",
                },
                "position_size": {
                    "type": "number",
                    "minimum": 0.01,
                    "maximum": 1.0,
                    "default": 0.06,
                    "description": "Fraction of capital to deploy per entry",
                },
                "stop_loss_pct": {
                    "type": "number",
                    "minimum": 0.001,
                    "maximum": 1.0,
                    "default": 0.025,
                    "description": "Cut the trade if price falls this far below entry",
                },
                "take_profit_pct": {
                    "type": "number",
                    "minimum": 0.001,
                    "maximum": 1.0,
                    "default": 0.045,
                    "description": "Take profit on a strong oversold bounce",
                },
                "min_bandwidth_pct": {
                    "type": "number",
                    "minimum": 0.0,
                    "maximum": 1.0,
                    "default": 0.015,
                    "description": "Skip trades when the rolling band width is narrower than this",
                },
                "max_hold_minutes": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": 10080,
                    "default": 180,
                    "description": "Maximum time to hold an open reversion position",
                },
                "use_fear_index_filter": {
                    "type": "boolean",
                    "default": False,
                    "description": "If true, apply fear-index gates when sentiment data exists",
                },
                "fear_index_buy_max": {
                    "type": "number",
                    "minimum": 0,
                    "maximum": 100,
                    "default": 35,
                    "description": "Only buy when fear index is at or below this level",
                },
                "fear_index_sell_min": {
                    "type": "number",
                    "minimum": 0,
                    "maximum": 100,
                    "default": 60,
                    "description": "Prefer exits when fear index is at or above this level",
                },
                "use_trend_filter": {
                    "type": "boolean",
                    "default": True,
                    "description": "If true, block entries below the long EMA or in bearish market regime",
                },
                "ema_long_window": {
                    "type": "integer",
                    "minimum": 5,
                    "maximum": 500,
                    "default": 200,
                    "description": "Long lookback window used for the hard trend filter",
                },
            },
            "required": ["band_window", "rsi_period", "entry_zscore", "exit_zscore"],
        }

    def _check_exit(
        self,
        *,
        context: StrategyContext,
        signal_id: UUID,
        correlation_id: UUID,
        market: MarketSnapshot,
        position: PositionState,
        zscore: float,
        rsi_value: float,
        exit_zscore: float,
        rsi_exit_threshold: float,
        rsi_sell_threshold: float,
        fear_sell_ok: bool,
        stop_loss_pct: float,
        take_profit_pct: float,
        max_hold_minutes: int,
    ) -> StrategySignal | None:
        if position.entry_price is None or position.entry_price <= 0:
            return self._hold_signal(
                context,
                signal_id,
                correlation_id,
                "Already long but entry price is missing",
            )

        pnl = (market.current_price - position.entry_price) / position.entry_price
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

        if abs(zscore) <= exit_zscore and rsi_value >= rsi_exit_threshold:
            return self._close_signal(
                context,
                signal_id,
                correlation_id,
                f"Mean reversion exit: z={zscore:.2f}, rsi={rsi_value:.1f}",
            )

        if zscore > 0 and rsi_value >= rsi_sell_threshold and fear_sell_ok:
            return self._close_signal(
                context,
                signal_id,
                correlation_id,
                f"Overbought bounce exit: z={zscore:.2f}, rsi={rsi_value:.1f}",
            )

        if position.opened_at is not None:
            max_hold = timedelta(minutes=max_hold_minutes)
            held_for = market.timestamp - position.opened_at
            if held_for >= max_hold:
                return self._close_signal(
                    context,
                    signal_id,
                    correlation_id,
                    f"Max hold reached: held_for={held_for} limit={max_hold}",
                )

        return None

    def _buy_signal(
        self,
        context: StrategyContext,
        signal_id: UUID,
        correlation_id: UUID,
        *,
        position_size: float,
        confidence: float,
        reason: str,
        metadata: dict[str, float],
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
            order_type=OrderType.MARKET,
            quantity=None,
            confidence=confidence,
            reason=reason,
            metadata={
                "position_size_fraction": position_size,
                "price": str(market.current_price),
                **metadata,
            },
        )

    @staticmethod
    def _strong_downtrend(
        *, closes: list[float], price: float, ema_long: float, market_regime: str | None
    ) -> bool:
        if market_regime == "bearish" and price < ema_long * 0.99:
            return True
        if len(closes) < 20 or ema_long <= 0:
            return False
        lookback_price = closes[-20]
        if lookback_price <= 0:
            return False
        twenty_bar_return = (price - lookback_price) / lookback_price
        return twenty_bar_return <= -0.06 and price < ema_long * 0.98

    def _close_signal(
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
            signal_type=SignalType.CLOSE,
            confidence=0.72,
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

    @staticmethod
    def _compute_rsi(closes: list[float]) -> float:
        gains = 0.0
        losses = 0.0
        for prev, cur in zip(closes, closes[1:]):
            delta = cur - prev
            if delta > 0:
                gains += delta
            elif delta < 0:
                losses += abs(delta)

        periods = max(1, len(closes) - 1)
        avg_gain = gains / periods
        avg_loss = losses / periods
        if avg_loss <= 1e-9:
            return 100.0
        if avg_gain <= 1e-9:
            return 0.0
        rs = avg_gain / avg_loss
        return 100.0 - (100.0 / (1.0 + rs))

    @staticmethod
    def _fear_index(market: MarketSnapshot) -> float | None:
        if not isinstance(market.metadata, dict):
            return None
        raw = market.metadata.get("fear_index")
        if raw is None:
            return None
        try:
            return float(raw)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _market_regime(market: MarketSnapshot) -> str | None:
        if not isinstance(market.metadata, dict):
            return None
        raw = market.metadata.get("market_regime")
        if raw is None:
            return None
        return str(raw).strip().lower() or None

    @staticmethod
    def _ema(values: list[float], window: int) -> float:
        series = values[-window:] if len(values) >= window else values
        if not series:
            return 0.0
        alpha = 2 / (len(series) + 1)
        ema = series[0]
        for value in series[1:]:
            ema = (value * alpha) + (ema * (1 - alpha))
        return ema
