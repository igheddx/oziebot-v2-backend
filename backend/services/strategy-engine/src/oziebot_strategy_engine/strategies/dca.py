"""DCA (Dollar Cost Averaging) strategy - regular fixed purchases."""

from uuid import UUID

from oziebot_domain.strategy import SignalType, StrategySignal
from oziebot_domain.trading import Instrument, OrderType, Side
from oziebot_strategy_engine.strategy import StrategyContext, TradingStrategy


class DCAStrategy(TradingStrategy):
    """
    DCA (Dollar Cost Averaging) Strategy - buys fixed amounts at intervals.

    Long-term accumulation strategy that reduces timing risk by purchasing
    regularly regardless of price.

    Configuration:
    - buy_amount_usd: Fixed USD amount to buy each cycle (default: 100)
    - buy_interval_hours: Hours between buys (default: 24 = daily)
    - only_on_green_days: Skip buy if price is down today (default: false)
    """

    strategy_id = "dca"
    display_name = "Dollar Cost Averaging"
    description = "Regular fixed-amount purchases to build position over time"
    version = "1.0"

    def validate_config(self, config: dict) -> bool:
        """Validate DCA config."""
        buy_amount = config.get("buy_amount_usd", 100)
        buy_interval = config.get("buy_interval_hours", 24)
        min_trade_usd = float(config.get("min_trade_usd", 100))
        max_trade_usd = float(config.get("max_trade_usd", 150))
        target_bucket_utilization_pct = float(
            config.get("target_bucket_utilization_pct", 0.50)
        )
        drawdown_reduction_multiplier = float(
            config.get("drawdown_reduction_multiplier", 0.75)
        )

        if not (1 <= buy_amount <= 1000000):
            raise ValueError(f"buy_amount_usd must be 1-1000000, got {buy_amount}")
        if not (1 <= buy_interval <= 720):  # Max 30 days
            raise ValueError(f"buy_interval_hours must be 1-720, got {buy_interval}")
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
        """Generate DCA signal."""
        buy_amount_usd = config.get("buy_amount_usd", 100)
        only_on_green = config.get("only_on_green_days", False)

        market = context.market_snapshot

        # Check if today is green (close higher than open)
        if only_on_green and market.close_price <= market.open_price:
            return self._hold_signal(
                context,
                signal_id,
                correlation_id,
                f"Skipping DCA: red day (close: {market.close_price} < open: {market.open_price})",
            )

        # Time-based check would be handled by caller/scheduler
        # For now, assume we've reached the interval - generate BUY signal

        return self._buy_signal(
            context,
            signal_id,
            correlation_id,
            buy_amount_usd,
            f"DCA buy cycle: ${buy_amount_usd}",
        )

    def get_default_config(self) -> dict:
        """Return default configuration."""
        return {
            "buy_amount_usd": 100,
            "buy_interval_hours": 24,
            "only_on_green_days": False,
            "dynamic_sizing_enabled": True,
            "min_trade_usd": 100,
            "max_trade_usd": 150,
            "target_bucket_utilization_pct": 0.50,
            "drawdown_size_reduction_enabled": True,
            "drawdown_reduction_multiplier": 0.75,
        }

    def get_config_schema(self) -> dict:
        """Return JSON schema for config."""
        return {
            "type": "object",
            "properties": {
                "buy_amount_usd": {
                    "type": "number",
                    "minimum": 1,
                    "maximum": 1000000,
                    "default": 100,
                    "description": "USD amount to buy each cycle",
                },
                "buy_interval_hours": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": 720,
                    "default": 24,
                    "description": "Hours between buy cycles",
                },
                "only_on_green_days": {
                    "type": "boolean",
                    "default": False,
                    "description": "Skip buy if price is down today",
                },
                "dynamic_sizing_enabled": {
                    "type": "boolean",
                    "default": True,
                    "description": "Scale buy amount toward bucket utilization targets",
                },
                "min_trade_usd": {
                    "type": "number",
                    "minimum": 0,
                    "default": 100,
                    "description": "Minimum dynamic DCA trade notional floor in USD",
                },
                "max_trade_usd": {
                    "type": "number",
                    "minimum": 1,
                    "default": 150,
                    "description": "Dynamic DCA trade notional ceiling before risk caps",
                },
                "target_bucket_utilization_pct": {
                    "type": "number",
                    "minimum": 0,
                    "maximum": 1,
                    "default": 0.50,
                    "description": "Target fraction of assigned bucket capital to keep deployed",
                },
                "drawdown_size_reduction_enabled": {
                    "type": "boolean",
                    "default": True,
                    "description": "Reduce DCA size automatically during elevated drawdown",
                },
                "drawdown_reduction_multiplier": {
                    "type": "number",
                    "minimum": 0,
                    "maximum": 1,
                    "default": 0.75,
                    "description": "Multiplier applied when drawdown-aware sizing is active",
                },
            },
        }

    def _buy_signal(
        self,
        context: StrategyContext,
        signal_id: UUID,
        correlation_id: UUID,
        buy_amount_usd: float,
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
            order_type=OrderType.MARKET,
            confidence=0.9,  # High confidence - predetermined amount
            reason=reason,
            metadata={
                "buy_amount_usd": buy_amount_usd,
                "current_price": str(market.current_price),
                "strategy_type": "dca",
            },
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
            metadata={"strategy_type": "dca"},
        )
