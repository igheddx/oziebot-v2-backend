from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

from oziebot_domain.strategy import SignalType
from oziebot_domain.trading_mode import TradingMode
from oziebot_strategy_engine.strategies.reversion import ReversionStrategy
from oziebot_strategy_engine.strategy import MarketSnapshot, PositionState, StrategyContext


def _context(
    *,
    closes: list[float],
    current_price: str | None = None,
    quantity: str = "0",
    entry_price: str | None = None,
    opened_at: datetime | None = None,
    fear_index: float | None = None,
) -> StrategyContext:
    now = datetime.now(UTC)
    price = Decimal(current_price or str(closes[-1]))
    metadata = {"candle_closes": closes}
    if fear_index is not None:
        metadata["fear_index"] = fear_index
    market = MarketSnapshot(
        timestamp=now,
        symbol="AERO-USD",
        current_price=price,
        bid_price=price,
        ask_price=price,
        volume_24h=Decimal("1000"),
        open_price=Decimal(str(closes[-1])),
        high_price=price,
        low_price=price,
        close_price=price,
        **metadata,
    )
    return StrategyContext(
        tenant_id=uuid4(),
        trading_mode=TradingMode.PAPER,
        market_snapshot=market,
        position_state=PositionState(
            symbol="AERO-USD",
            quantity=Decimal(quantity),
            entry_price=Decimal(entry_price) if entry_price is not None else None,
            opened_at=opened_at,
        ),
    )


def test_reversion_buys_oversold_stretch():
    strategy = ReversionStrategy()
    closes = [100, 101, 100, 99, 100, 98, 97, 96, 95, 94, 93, 92, 91, 90, 89, 88, 87, 86, 85, 83]
    context = _context(closes=closes, current_price="83")

    signal = strategy.generate_signal(context, {}, uuid4(), uuid4())

    assert signal.signal_type == SignalType.BUY
    assert "Oversold reversion entry" in signal.reason


def test_reversion_holds_when_history_is_short():
    strategy = ReversionStrategy()
    context = _context(closes=[100, 99, 98, 97, 96], current_price="96")

    signal = strategy.generate_signal(context, {}, uuid4(), uuid4())

    assert signal.signal_type == SignalType.HOLD
    assert "Insufficient history" in signal.reason


def test_reversion_closes_after_mean_snapback():
    strategy = ReversionStrategy()
    closes = [100, 99, 98, 97, 96, 95, 94, 94, 95, 96, 97, 98, 99, 100, 101, 101, 100, 100, 100.5, 101]
    context = _context(
        closes=closes,
        current_price="101",
        quantity="10",
        entry_price="94",
        opened_at=datetime.now(UTC) - timedelta(minutes=45),
    )

    signal = strategy.generate_signal(context, {}, uuid4(), uuid4())

    assert signal.signal_type == SignalType.CLOSE
    assert "Mean reversion exit" in signal.reason or "Take profit hit" in signal.reason


def test_reversion_honors_fear_filter_for_entries():
    strategy = ReversionStrategy()
    closes = [100, 101, 100, 99, 100, 98, 97, 96, 95, 94, 93, 92, 91, 90, 89, 88, 87, 86, 85, 83]
    context = _context(closes=closes, current_price="83", fear_index=60)

    signal = strategy.generate_signal(context, {"use_fear_index_filter": True}, uuid4(), uuid4())

    assert signal.signal_type == SignalType.HOLD
    assert "No entry" in signal.reason


def test_reversion_exits_on_stop_loss():
    strategy = ReversionStrategy()
    closes = [100, 99, 98, 97, 96, 95, 94, 94, 93, 92, 91, 90, 89, 88, 87, 86, 85, 84, 83, 82]
    context = _context(
        closes=closes,
        current_price="82",
        quantity="10",
        entry_price="86",
        opened_at=datetime.now(UTC) - timedelta(minutes=30),
    )

    signal = strategy.generate_signal(context, {}, uuid4(), uuid4())

    assert signal.signal_type == SignalType.CLOSE
    assert "Stop loss hit" in signal.reason
