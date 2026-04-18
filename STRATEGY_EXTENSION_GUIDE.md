"""
Strategy Framework Extension Guide

This document explains how to add new trading strategies to the oziebot platform
WITHOUT modifying any execution or risk management code.

## Key Principles

1. **Pluggable Design**: New strategies can be added by just implementing one interface
2. **Stateless Signals**: Strategies only generate signals, no direct trading
3. **Mode-Agnostic Logic**: Same code works for PAPER and LIVE trading
4. **Configuration**: Each strategy can have user-customizable parameters
5. **Performance Tracking**: Built-in monitoring of strategy behavior

## Step 1: Create Your Strategy Class

Create a new file in `services/strategy-engine/src/oziebot_strategy_engine/strategies/`

```python
# services/strategy-engine/src/oziebot_strategy_engine/strategies/my_strategy.py

from uuid import UUID
from oz iebot_domain.strategy import SignalType, StrategySignal
from oziebot_strategy_engine.strategy import TradingStrategy, StrategyContext


class MyStrategy(TradingStrategy):
    \"\"\"
    Your strategy description.
    \"\"\"

    strategy_id = "my_strategy"  # Unique identifier - lowercase, underscore-separated
    display_name = "My Strategy"
    description = "What this strategy does"
    version = "1.0"

    def validate_config(self, config: dict) -> bool:
        \"\"\"
        Validate user-provided configuration.
        
        Raise ValueError if config is invalid.
        \"\"\"
        # Example validation
        threshold = config.get("threshold", 0.05)
        if not (0.0 <= threshold <= 1.0):
            raise ValueError(f"threshold must be 0-1, got {threshold}")
        return True

    def generate_signal(
        self,
        context: StrategyContext,
        config: dict,
        signal_id: UUID,
        correlation_id: UUID,
    ) -> StrategySignal:
        \"\"\"
        Analyze market conditions and generate a signal.
        
        This is called for BOTH PAPER and LIVE trading.
        Your logic must be identical - only execution differs.
        
        Args:
            context: Has market_snapshot, position_state, tenant_id, trading_mode
            config: User's strategy configuration
            signal_id: UUID for this signal
            correlation_id: Correlation ID for tracking
            
        Returns:
            StrategySignal with recommendation
        \"\"\"
        market = context.market_snapshot
        position = context.position_state
        
        # Your analysis here...
        
        # Generate appropriate signal
        if your_bullish_condition:
            return StrategySignal(
                signal_id=signal_id,
                correlation_id=correlation_id,
                tenant_id=context.tenant_id,
                strategy_id=self.strategy_id,
                strategy_version=self.version,
                trading_mode=context.trading_mode,  # PAPER or LIVE - both supported
                signal_type=SignalType.BUY,
                instrument=Instrument(symbol=market.symbol),
                side=Side.BUY,
                order_type=OrderType.MARKET,
                confidence=0.75,
                reason="Why you think this is a good trade",
                metadata={"analysis": "any strategy-specific data"},
            )
        
        return StrategySignal(
            signal_id=signal_id,
            correlation_id=correlation_id,
            tenant_id=context.tenant_id,
            strategy_id=self.strategy_id,
            strategy_version=self.version,
            trading_mode=context.trading_mode,
            signal_type=SignalType.HOLD,
            reason="Waiting for better conditions",
        )

    def get_default_config(self) -> dict:
        \"\"\"Return default configuration for this strategy.\"\"\"
        return {
            "threshold": 0.05,
            "window_size": 20,
        }

    def get_config_schema(self) -> dict:
        \"\"\"
        Return JSON schema for configuration parameters.
        
        This is used by the frontend to generate dynamic UI.
        \"\"\"
        return {
            "type": "object",
            "properties": {
                "threshold": {
                    "type": "number",
                    "minimum": 0.0,
                    "maximum": 1.0,
                    "default": 0.05,
                    "description": "Signal threshold",
                },
                "window_size": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": 100,
                    "default": 20,
                    "description": "Lookback window in periods",
                },
            },
        }
```

## Step 2: Register Your Strategy

Add your strategy to the registry in `strategies/__init__.py`:

```python
# services/strategy-engine/src/oziebot_strategy_engine/strategies/__init__.py

from oziebot_strategy_engine.registry import StrategyRegistry
from oziebot_strategy_engine.strategies.my_strategy import MyStrategy

# Register all built-in strategies
StrategyRegistry.register(MyStrategy)
```

That's it! Your strategy is now available.

## Step 3: Test Your Strategy

```python
# services/api/tests/test_my_strategy.py

import pytest
from oziebot_strategy_engine.strategies.my_strategy import MyStrategy
from oziebot_strategy_engine.strategy import MarketSnapshot, PositionState, StrategyContext
from oziebot_domain.trading_mode import TradingMode

def test_my_strategy_buy_signal():
    strategy = MyStrategy()
    config = strategy.get_default_config()
    
    market = MarketSnapshot(
        timestamp=datetime.now(UTC),
        symbol="BTC",
        current_price=Decimal("50000"),
        # ... other market data
    )
    
    context = StrategyContext(
        tenant_id=uuid.uuid4(),
        trading_mode=TradingMode.PAPER,
        market_snapshot=market,
        position_state=PositionState("BTC", Decimal(0)),
    )
    
    signal = strategy.generate_signal(context, config, uuid.uuid4(), uuid.uuid4())
    
    assert signal.signal_type in (SignalType.BUY, SignalType.HOLD)
    assert signal.trading_mode == TradingMode.PAPER
```

## Architecture Overview

```
User wants to trade
         |
         v
API: POST /me/strategies (configure strategy with custom parameters)
         |
         v
Database: UserStrategy stores config per user
         |
         v
Strategy Engine receives signal generation request
         |
         v
StrategyRegistry.get_strategy(strategy_id) retrieves implementation
         |
         v
Strategy.generate_signal() analyzes market & returns StrategySignal
         |
         v
StrategySignal has:
- trading_mode (PAPER or LIVE - identical logic)
- signal_type (BUY, SELL, HOLD, CLOSE)
- instrument, side, order_type, quantity
- confidence and reasoning
         |
         v
Risk Engine validates signal (separate service)
         |
         v
Execution Engine executes approved signal (separate service)
```

## Signal Types

- **BUY**: Open or add to position
- **SELL**: Reduce or exit position  
- **HOLD**: Do nothing, wait for better conditions
- **CLOSE**: Completely exit current position

## Context Available to Strategy

In `StrategyContext`:

```python
context.tenant_id        # User's tenant ID
context.trading_mode     # TradingMode.PAPER or TradingMode.LIVE
context.market_snapshot  # Current market data
  .timestamp             # When this snapshot was taken
  .symbol                # Trading pair (e.g. "BTC", "ETH")
  .current_price         # Current bid/ask mid
  .bid_price             # Best bid
  .ask_price             # Best ask  
  .volume_24h            # 24-hour volume
  .open_price            # Open of current period
  .high_price            # High of current period
  .low_price             # Low of current period
  .close_price           # Close of previous period

context.position_state   # Current position data
  .symbol                # Pair this position is in
  .quantity              # Units held (0 if no position)
  .entry_price           # Price entered at
```

## Configuration Pattern

Every strategy should support configuration:

1. **get_default_config()** - Returns dict of default parameter values
2. **get_config_schema()** - Returns JSON schema for validation and UI generation
3. **validate_config()** - Checks user-provided config, raises ValueError if invalid

This allows users to customize each strategy via the UI without code.

## PAPER vs LIVE - Important!

Your strategy's logic MUST be identical for both modes:

```python
# ✅ CORRECT - Same logic, mode tracked in signal
def generate_signal(self, context, config, signal_id, correlation_id):
    market = context.market_snapshot
    
    # Same analysis regardless of mode
    if market.close_price > market.open_price:
        return self._buy_signal(context, signal_id, correlation_id)
    
    return self._hold_signal(context, signal_id, correlation_id)

# ❌ WRONG - Different logic based on mode
def generate_signal(self, context, config, signal_id, correlation_id):
    if context.trading_mode == TradingMode.PAPER:
        # Different logic for paper
        ...
    else:
        # Different logic for live
        ...
```

The signal's `trading_mode` field records which mode was active when the signal was generated.
Risk and execution engines use this to apply appropriate checks/behavior - not the strategy.

## Performance Tracking

Strategies are automatically tracked:

```
GET /v1/me/strategies/{strategy_id}/performance?trading_mode=PAPER
```

Returns:
- total_signals
- buy_signals, sell_signals, hold_signals, close_signals
- avg_confidence
- last_signal_at

```
GET /v1/me/strategies/{strategy_id}/signals?limit=100
```

Returns recent signals with all parameters for analysis.

## No Direct Trading

IMPORTANT: Strategies CANNOT directly call trading functions.
They ONLY generate signals. This is intentional:

1. **Risk Management**: All signals go through risk engine first
2. **Compliance**: Platform-wide settings/restrictions apply
3. **Execution**: May be routed to different exchanges/modes
4. **Auditability**: Clear separation of concerns
5. **Testing**: Can test signals independently from execution

The flow is always: Strategy -> Signal -> Risk -> Execution

## Best Practices

1. **Keep it simple**: Strategy should be easy to understand
2. **Make it configurable**: Use get_default_config() for parameters
3. **Add metadata**: Include reasoning in signal.reason field
4. **Set confidence**: 0.5+ for strong signals, < 0.5 for weak signals
5. **Handle edge cases**: Invalid prices, missing data, extreme conditions
6. **Document code**: Comment your analysis logic
7. **Test both modes**: Verify PAPER and LIVE generate equivalent signals

## Example: Adding Support for Limit Orders

```python
def _buy_with_limit(self, context, signal_id, correlation_id, limit_fraction=0.99):
    \"\"\"Generate BUY signal with limit order.\"\"\"
    market = context.market_snapshot
    
    # Limit price slightly below current ask
    limit_price = market.current_price * Decimal(str(limit_fraction))
    
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
        limit_price=limit_price,  # Execution will use this
        confidence=0.65,
        reason=f"Limit buy at {limit_price}",
    )
```

## Summary

To add a new strategy:

1. Create class inheriting from `TradingStrategy`
2. Implement `validate_config()` and `generate_signal()`
3. Register in `strategies/__init__.py`
4. Test with both PAPER and LIVE modes
5. Write tests demonstrating strategy behavior
6. That's it - users can now use your strategy!

No changes to risk engine, execution engine, or any other system needed.
The pluggable architecture ensures new strategies integrate seamlessly.
"""
