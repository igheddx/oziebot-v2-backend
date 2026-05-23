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
from oziebot_strategy_engine.strategies.volatility_harvest import (
    VolatilityHarvestStrategy,
    default_strategy_config,
)


def _market(symbol: str, current_price: str = "2.0") -> MarketSnapshot:
    closes = [1.0 + (0.04 * index) for index in range(30)]
    return MarketSnapshot(
        timestamp=datetime.now(UTC),
        symbol=symbol,
        current_price=Decimal(current_price),
        bid_price=Decimal(current_price) - Decimal("0.001"),
        ask_price=Decimal(current_price) + Decimal("0.001"),
        volume_24h=Decimal("200000"),
        open_price=Decimal("1.0"),
        high_price=Decimal(current_price) + Decimal("0.1"),
        low_price=Decimal("0.9"),
        close_price=Decimal(current_price),
        candle_closes=closes,
        candle_volumes=[1000 + index * 15 for index in range(30)],
        candle_highs=[close + 0.05 for close in closes],
        candle_lows=[close - 0.05 for close in closes],
    )


def _context(
    *,
    quantity: str = "0",
    current_price: str = "2.0",
    extra: dict | None = None,
) -> StrategyContext:
    return StrategyContext(
        tenant_id=uuid.uuid4(),
        trading_mode=TradingMode.PAPER,
        market_snapshot=_market("AERO-USD", current_price=current_price),
        position_state=PositionState(
            symbol="AERO-USD",
            quantity=Decimal(quantity),
            entry_price=Decimal("1.0") if Decimal(quantity) > 0 else None,
            peak_price=Decimal("2.4") if Decimal(quantity) > 0 else None,
        ),
        **(extra or {}),
    )


def test_validate_requires_selected_tokens():
    strategy = VolatilityHarvestStrategy()
    config = default_strategy_config()
    with pytest.raises(ValueError):
        strategy.validate_config(config)


def test_initial_entry_emits_buy_signal():
    strategy = VolatilityHarvestStrategy()
    config = default_strategy_config()
    config["selected_tokens"] = ["AERO-USD"]
    signal = strategy.generate_signal(
        _context(
            extra={
                "volatility_harvest": {
                    "target_token_capital_usd": 500.0,
                    "target_core_capital_usd": 350.0,
                    "target_trading_capital_usd": 150.0,
                    "entry_layers": config["entry_layers"],
                    "regime": {"regime": "normal"},
                },
                "runtime_symbol_state": {},
            }
        ),
        config,
        uuid.uuid4(),
        uuid.uuid4(),
    )
    assert signal.signal_type == SignalType.BUY
    assert "initial_layered_entry" in signal.reason


def test_harvest_signal_sells_only_trading_slice():
    strategy = VolatilityHarvestStrategy()
    config = default_strategy_config()
    config["selected_tokens"] = ["AERO-USD"]
    signal = strategy.generate_signal(
        _context(
            quantity="100",
            current_price="2.00",
            extra={
                "volatility_harvest": {
                    "core_quantity": 70.0,
                    "trading_quantity": 30.0,
                    "avg_trading_entry_price": 1.0,
                    "harvest_bands": config["harvest_bands"],
                    "risk_controls": config["risk_controls"],
                    "fee_settings": config["fee_settings"],
                    "regime": {"harvest_sell_multiplier": 1.0},
                },
                "runtime_symbol_state": {},
            },
        ),
        config,
        uuid.uuid4(),
        uuid.uuid4(),
    )
    assert signal.signal_type == SignalType.CLOSE
    assert signal.quantity is not None
    assert Decimal(str(signal.quantity.amount)) < Decimal("30.0")
    assert "harvest_trigger" in signal.reason


def test_rebuy_signal_uses_harvested_cash_after_pullback():
    strategy = VolatilityHarvestStrategy()
    config = default_strategy_config()
    config["selected_tokens"] = ["AERO-USD"]
    signal = strategy.generate_signal(
        _context(
            quantity="7",
            current_price="1.80",
            extra={
                "volatility_harvest": {
                    "core_quantity": 7.0,
                    "trading_quantity": 0.5,
                    "harvested_cash_usd": 120.0,
                    "target_trading_capital_usd": 180.0,
                    "last_local_high": 2.1,
                    "rebuy_bands": config["rebuy_bands"],
                    "risk_controls": config["risk_controls"],
                    "volatility_settings": config["volatility_settings"],
                    "atr_pct": 0.03,
                    "rsi": 30.0,
                    "regime": {"regime": "normal", "suspend_rebuys": False},
                },
                "runtime_symbol_state": {},
            },
        ),
        config,
        uuid.uuid4(),
        uuid.uuid4(),
    )
    assert signal.signal_type == SignalType.BUY
    assert "rebuy_trigger" in signal.reason
