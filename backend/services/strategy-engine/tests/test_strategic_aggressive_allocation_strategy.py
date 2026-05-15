from __future__ import annotations

import uuid
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from oziebot_domain.strategy import SignalType
from oziebot_domain.trading_mode import TradingMode
from oziebot_strategy_engine.strategy import (
    MarketSnapshot,
    PositionState,
    StrategyContext,
)
from oziebot_strategy_engine.strategies.strategic_aggressive_allocation import (
    DRY_POWDER_BUCKET,
    HIGH_CONVICTION_BUCKET,
    StrategicAggressiveAllocationStrategy,
    default_strategy_config,
)


def _market(symbol: str, current_price: str = "2.0") -> MarketSnapshot:
    closes = [1.0 + (0.03 * index) for index in range(30)]
    return MarketSnapshot(
        timestamp=datetime.now(UTC),
        symbol=symbol,
        current_price=Decimal(current_price),
        bid_price=Decimal(current_price) - Decimal("0.01"),
        ask_price=Decimal(current_price) + Decimal("0.01"),
        volume_24h=Decimal("10000"),
        open_price=Decimal("1.0"),
        high_price=Decimal(current_price) + Decimal("0.1"),
        low_price=Decimal("0.9"),
        close_price=Decimal(current_price),
        candle_closes=closes,
        candle_volumes=[1000 + index * 10 for index in range(30)],
        candle_highs=[close + 0.05 for close in closes],
        candle_lows=[close - 0.05 for close in closes],
    )


def _context(
    *,
    symbol: str = "BTC-USD",
    quantity: str = "1",
    current_price: str = "2.0",
    extra: dict | None = None,
) -> StrategyContext:
    return StrategyContext(
        tenant_id=uuid.uuid4(),
        trading_mode=TradingMode.PAPER,
        market_snapshot=_market(symbol, current_price=current_price),
        position_state=PositionState(
            symbol=symbol,
            quantity=Decimal(quantity),
            entry_price=Decimal("1.0") if Decimal(quantity) > 0 else None,
            peak_price=Decimal("2.6") if Decimal(quantity) > 0 else None,
        ),
        **(extra or {}),
    )


def test_validate_requires_bucket_total_100():
    strategy = StrategicAggressiveAllocationStrategy()
    config = default_strategy_config()
    config["bucket_allocations"][0]["allocation_pct"] = 41.5
    with pytest.raises(ValueError):
        strategy.validate_config(config)


def test_validate_rejects_dry_powder_tokens():
    strategy = StrategicAggressiveAllocationStrategy()
    config = default_strategy_config()
    config["selected_tokens"][DRY_POWDER_BUCKET] = ["BTC-USD"]
    with pytest.raises(ValueError):
        strategy.validate_config(config)


def test_profit_target_executes_only_once():
    strategy = StrategicAggressiveAllocationStrategy()
    context = _context(
        quantity="1",
        current_price="1.4",
        extra={
            "strategic_allocation": {
                "bucket_id": HIGH_CONVICTION_BUCKET,
                "score": 0.9,
                "rank": 1,
                "profit_targets_pct": [35.0, 75.0],
                "scale_out_fraction_pct": 25.0,
                "stop_loss_pct": 18.0,
                "trailing_stop_activation_pct": 50.0,
                "trailing_stop_pct": 12.0,
            },
            "runtime_symbol_state": {"completed_profit_events": ["profit_target_1"]},
        },
    )

    signal = strategy.generate_signal(
        context,
        default_strategy_config(),
        uuid.uuid4(),
        uuid.uuid4(),
    )

    assert signal.signal_type == SignalType.HOLD


def test_stop_loss_triggers_close():
    strategy = StrategicAggressiveAllocationStrategy()
    context = _context(
        quantity="1",
        current_price="0.78",
        extra={
            "strategic_allocation": {
                "bucket_id": HIGH_CONVICTION_BUCKET,
                "score": 0.9,
                "rank": 1,
                "profit_targets_pct": [35.0, 75.0],
                "scale_out_fraction_pct": 25.0,
                "stop_loss_pct": 18.0,
                "trailing_stop_activation_pct": 50.0,
                "trailing_stop_pct": 12.0,
            }
        },
    )

    signal = strategy.generate_signal(
        context,
        default_strategy_config(),
        uuid.uuid4(),
        uuid.uuid4(),
    )

    assert signal.signal_type == SignalType.CLOSE
    assert "stop_loss_triggered" in signal.reason


def test_trailing_stop_triggers_after_activation():
    strategy = StrategicAggressiveAllocationStrategy()
    context = _context(
        quantity="1",
        current_price="2.2",
        extra={
            "strategic_allocation": {
                "bucket_id": HIGH_CONVICTION_BUCKET,
                "score": 0.9,
                "rank": 1,
                "profit_targets_pct": [35.0, 75.0],
                "scale_out_fraction_pct": 25.0,
                "stop_loss_pct": 18.0,
                "trailing_stop_activation_pct": 50.0,
                "trailing_stop_pct": 12.0,
            }
        },
    )

    signal = strategy.generate_signal(
        context,
        default_strategy_config(),
        uuid.uuid4(),
        uuid.uuid4(),
    )

    assert signal.signal_type == SignalType.CLOSE
    assert "trailing_stop_triggered" in signal.reason
