from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

from oziebot_domain.strategy import SignalType
from oziebot_domain.trading_mode import TradingMode
from oziebot_strategy_engine.strategies.day_trading import DayTradingStrategy
from oziebot_strategy_engine.strategy import (
    MarketSnapshot,
    PositionState,
    StrategyContext,
)


def _context(
    *,
    current_price: str,
    closes: list[float],
    highs: list[float],
    lows: list[float],
    volumes: list[float],
) -> StrategyContext:
    now = datetime.now(UTC)
    price = Decimal(current_price)
    market = MarketSnapshot(
        timestamp=now,
        symbol="BTC-USD",
        current_price=price,
        bid_price=price,
        ask_price=price,
        volume_24h=Decimal("1000"),
        open_price=Decimal(str(closes[0])),
        high_price=Decimal(str(max(highs))),
        low_price=Decimal(str(min(lows))),
        close_price=price,
        candle_closes=closes,
        candle_highs=highs,
        candle_lows=lows,
        candle_volumes=volumes,
    )
    return StrategyContext(
        tenant_id=uuid4(),
        trading_mode=TradingMode.PAPER,
        market_snapshot=market,
        position_state=PositionState(symbol="BTC-USD", quantity=Decimal("0")),
    )


def test_day_trading_can_enter_with_single_confirmation():
    strategy = DayTradingStrategy()
    closes = [100.0] * 20 + [99.2, 99.3, 99.25, 99.22, 99.2]
    highs = [100.3] * 20 + [99.4, 99.45, 99.42, 99.4, 99.38]
    lows = [99.0] * 20 + [99.0, 99.02, 99.01, 99.0, 99.0]
    volumes = [100.0] * 24 + [100.0]
    context = _context(
        current_price="99.005",
        closes=closes,
        highs=highs,
        lows=lows,
        volumes=volumes,
    )

    signal = strategy.generate_signal(
        context,
        {"require_trend_alignment": False},
        uuid4(),
        uuid4(),
    )

    assert signal.signal_type == SignalType.BUY
    assert "Near session low with confirmations" in signal.reason
