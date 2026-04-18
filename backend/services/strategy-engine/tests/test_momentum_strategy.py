from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

from oziebot_domain.strategy import SignalType
from oziebot_domain.trading_mode import TradingMode
from oziebot_strategy_engine.strategies.momentum import MomentumStrategy
from oziebot_strategy_engine.strategy import (
    MarketSnapshot,
    PositionState,
    StrategyContext,
)


def _context(
    *,
    current_price: str,
    closes: list[float],
    quantity: str = "0",
    entry_price: str | None = None,
    peak_price: str | None = None,
    opened_at: datetime | None = None,
) -> StrategyContext:
    now = datetime.now(UTC)
    price = Decimal(current_price)
    position = PositionState(
        symbol="AERO-USD",
        quantity=Decimal(quantity),
        entry_price=Decimal(entry_price) if entry_price is not None else None,
        peak_price=Decimal(peak_price) if peak_price is not None else None,
        opened_at=opened_at,
    )
    market = MarketSnapshot(
        timestamp=now,
        symbol="AERO-USD",
        current_price=price,
        bid_price=price,
        ask_price=price,
        volume_24h=Decimal("1000"),
        open_price=price,
        high_price=price,
        low_price=price,
        close_price=price,
        candle_closes=closes,
    )
    return StrategyContext(
        tenant_id=uuid4(),
        trading_mode=TradingMode.PAPER,
        market_snapshot=market,
        position_state=position,
    )


def test_momentum_exits_on_stop_loss():
    strategy = MomentumStrategy()
    context = _context(
        current_price="95",
        closes=[100.0] * 33 + [101.0],
        quantity="10",
        entry_price="100",
        peak_price="102",
        opened_at=datetime.now(UTC) - timedelta(minutes=10),
    )

    signal = strategy.generate_signal(context, {}, uuid4(), uuid4())

    assert signal.signal_type == SignalType.CLOSE
    assert "Stop loss hit" in signal.reason


def test_momentum_exits_on_take_profit():
    strategy = MomentumStrategy()
    context = _context(
        current_price="107",
        closes=[100.0] * 33 + [102.0],
        quantity="10",
        entry_price="100",
        peak_price="107",
        opened_at=datetime.now(UTC) - timedelta(minutes=10),
    )

    signal = strategy.generate_signal(context, {}, uuid4(), uuid4())

    assert signal.signal_type == SignalType.CLOSE
    assert "Take profit hit" in signal.reason


def test_momentum_exits_on_trailing_stop():
    strategy = MomentumStrategy()
    context = _context(
        current_price="102",
        closes=[100.0] * 33 + [101.0],
        quantity="10",
        entry_price="100",
        peak_price="106",
        opened_at=datetime.now(UTC) - timedelta(minutes=10),
    )

    signal = strategy.generate_signal(
        context, {"take_profit_pct": 0.10}, uuid4(), uuid4()
    )

    assert signal.signal_type == SignalType.CLOSE
    assert "Trailing stop hit" in signal.reason


def test_momentum_exits_on_max_hold_time():
    strategy = MomentumStrategy()
    context = _context(
        current_price="101",
        closes=[100.0] * 33 + [100.5],
        quantity="10",
        entry_price="100",
        peak_price="103",
        opened_at=datetime.now(UTC) - timedelta(minutes=500),
    )

    signal = strategy.generate_signal(context, {}, uuid4(), uuid4())

    assert signal.signal_type == SignalType.CLOSE
    assert "Max hold reached" in signal.reason
