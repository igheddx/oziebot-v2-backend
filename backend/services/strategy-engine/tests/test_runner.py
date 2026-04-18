from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from oziebot_domain.signal_pipeline import StrategySignalEvent
from oziebot_domain.strategy import SignalType, StrategySignal
from oziebot_domain.trading import Quantity
from oziebot_domain.trading_mode import TradingMode
from oziebot_strategy_engine.runner import StrategyScheduleState, StrategyRunner
from oziebot_strategy_engine.strategy import MarketSnapshot, PositionState


class DummyRedis:
    def __init__(self, kv: dict[str, str] | None = None):
        self.kv = kv or {}

    def get(self, key: str):
        return self.kv.get(key)

    def lrange(self, key: str, start: int, end: int):
        value = self.kv.get(key, [])
        if isinstance(value, list):
            return value[start : end + 1 if end >= 0 else None]
        return []

    def lpush(self, key: str, value: str):
        existing = self.kv.setdefault(key, [])
        if isinstance(existing, list):
            existing.insert(0, value)


def test_schedule_pattern_intervals():
    sched = StrategyScheduleState()
    now = datetime.now(UTC)

    assert sched.should_run(
        user_id="u1",
        strategy_name="momentum",
        trading_mode="paper",
        symbol="BTC-USD",
        now=now,
        interval_seconds=30,
    )
    assert not sched.should_run(
        user_id="u1",
        strategy_name="momentum",
        trading_mode="paper",
        symbol="BTC-USD",
        now=now + timedelta(seconds=10),
        interval_seconds=30,
    )
    assert sched.should_run(
        user_id="u1",
        strategy_name="momentum",
        trading_mode="paper",
        symbol="BTC-USD",
        now=now + timedelta(seconds=31),
        interval_seconds=30,
    )


def test_runner_load_market_snapshot_from_normalized_cache():
    symbol = "BTC-USD"
    redis = DummyRedis(
        {
            f"oziebot:md:bbo:{symbol}": json.dumps(
                {
                    "best_bid_price": "50000.0",
                    "best_ask_price": "50010.0",
                }
            ),
            f"oziebot:md:candle:60:{symbol}": json.dumps(
                {
                    "open": "49000",
                    "high": "50500",
                    "low": "48500",
                    "close": "50000",
                    "volume": "1234.5",
                }
            ),
        }
    )

    # Engine is unused by this method in this test path.
    runner = StrategyRunner(engine=None, redis_client=redis)  # type: ignore[arg-type]
    snap = runner._load_market_snapshot(symbol)

    assert snap is not None
    assert snap.symbol == symbol
    assert snap.current_price == Decimal("50005.0")
    assert snap.volume_24h == Decimal("1234.5")


def test_signal_event_schema_fields_include_trading_mode_and_size():
    signal = StrategySignal(
        signal_id=uuid.uuid4(),
        correlation_id=uuid.uuid4(),
        tenant_id=uuid.uuid4(),
        strategy_id="momentum",
        trading_mode=TradingMode.PAPER,
        signal_type=SignalType.BUY,
        confidence=0.81,
        reason="uptrend",
        quantity=Quantity(amount="0.25"),
        metadata={"foo": "bar"},
    )

    event = StrategyRunner._to_signal_event(
        run_id=uuid.uuid4(),
        user_id=uuid.uuid4(),
        strategy_name="momentum",
        symbol="BTC-USD",
        signal=signal,
        trading_mode=TradingMode.LIVE,
        timestamp=datetime.now(UTC),
    )

    assert event.strategy_name == "momentum"
    assert event.symbol == "BTC-USD"
    assert event.action == SignalType.BUY
    assert event.confidence == 0.81
    assert event.suggested_size == Decimal("0.25")
    assert event.trading_mode == TradingMode.LIVE
    assert "reason" in event.reasoning_metadata


def test_close_signal_event_uses_open_position_size():
    signal = StrategySignal(
        signal_id=uuid.uuid4(),
        correlation_id=uuid.uuid4(),
        tenant_id=uuid.uuid4(),
        strategy_id="momentum",
        trading_mode=TradingMode.PAPER,
        signal_type=SignalType.CLOSE,
        confidence=0.7,
        reason="exit",
    )

    event = StrategyRunner._to_signal_event(
        run_id=uuid.uuid4(),
        user_id=uuid.uuid4(),
        strategy_name="momentum",
        symbol="AERO-USD",
        signal=signal,
        trading_mode=TradingMode.PAPER,
        timestamp=datetime.now(UTC),
        position_state=PositionState(
            symbol="AERO-USD",
            quantity=Decimal("46.56468999557635445042024633"),
            entry_price=Decimal("0.43"),
        ),
    )

    assert event.action == SignalType.CLOSE
    assert event.suggested_size == Decimal("46.56468999557635445042024633")


def test_runner_resolves_all_allowed_symbols_by_default():
    runner = StrategyRunner(engine=None, redis_client=DummyRedis())  # type: ignore[arg-type]

    assert runner._resolve_symbols(config={}, allowed_symbols=["AERO-USD", "BTC-USD"]) == ["AERO-USD", "BTC-USD"]
    assert runner._resolve_symbols(config={"symbol": "BTC-USD"}, allowed_symbols=["AERO-USD", "BTC-USD"]) == ["BTC-USD"]
    assert runner._resolve_symbols(
        config={"symbols": ["BTC-USD", "DOGE-USD"]},
        allowed_symbols=["AERO-USD", "BTC-USD", "ETH-USD"],
    ) == ["BTC-USD"]


def test_runner_coerces_legacy_and_multi_symbol_runtime_state():
    assert StrategyRunner._coerce_symbol_runtime_states(
        {"symbol": "AERO-USD", "peak_price": "0.50", "opened_at": "2026-04-17T16:00:00+00:00"}
    ) == {
        "AERO-USD": {"peak_price": "0.50", "opened_at": "2026-04-17T16:00:00+00:00"}
    }
    assert StrategyRunner._coerce_symbol_runtime_states(
        {"symbols": {"BTC-USD": {"peak_price": "51000"}, "ETH-USD": {"opened_at": "2026-04-17T16:01:00+00:00"}}}
    ) == {
        "BTC-USD": {"peak_price": "51000"},
        "ETH-USD": {"opened_at": "2026-04-17T16:01:00+00:00"},
    }


def test_merge_symbol_runtime_state_preserves_other_symbols():
    now = datetime.now(UTC)
    market = MarketSnapshot(
        timestamp=now,
        symbol="BTC-USD",
        current_price=Decimal("51000"),
        bid_price=Decimal("50990"),
        ask_price=Decimal("51010"),
        volume_24h=Decimal("1000"),
        open_price=Decimal("50000"),
        high_price=Decimal("52000"),
        low_price=Decimal("49000"),
        close_price=Decimal("51000"),
    )

    state = StrategyRunner._merge_symbol_runtime_states(
        {},
        position_state=PositionState(symbol="BTC-USD", quantity=Decimal("1"), entry_price=Decimal("50000")),
        market=market,
        now=now,
    )
    state = StrategyRunner._merge_symbol_runtime_states(
        state,
        position_state=PositionState(symbol="ETH-USD", quantity=Decimal("2"), entry_price=Decimal("2500")),
        market=market,
        now=now,
    )

    assert set(state.keys()) == {"BTC-USD", "ETH-USD"}
    cleared = StrategyRunner._merge_symbol_runtime_states(
        state,
        position_state=PositionState(symbol="BTC-USD", quantity=Decimal("0")),
        market=market,
        now=now,
    )
    assert set(cleared.keys()) == {"ETH-USD"}


def test_run_once_processes_all_allowed_symbols():
    class FanoutRunner(StrategyRunner):
        def __init__(self):
            super().__init__(engine=None, redis_client=DummyRedis())  # type: ignore[arg-type]
            self.events: list[StrategySignalEvent] = []

        def _load_enabled_user_strategies(self) -> list[dict[str, str]]:
            return [
                {
                    "user_id": str(uuid.uuid4()),
                    "strategy_id": "day_trading",
                    "tenant_id": uuid.uuid4(),
                    "config": {},
                }
            ]

        def _load_platform_strategy_config(self, strategy_id: str) -> dict[str, object]:
            return {}

        def _load_allowed_symbols(self, user_id: str) -> list[str]:
            return ["AERO-USD", "BTC-USD", "ETH-USD"]

        def _load_market_snapshot(self, symbol: str) -> MarketSnapshot | None:
            return MarketSnapshot(
                timestamp=datetime.now(UTC),
                symbol=symbol,
                current_price=Decimal("1"),
                bid_price=Decimal("0.99"),
                ask_price=Decimal("1.01"),
                volume_24h=Decimal("100"),
                open_price=Decimal("1"),
                high_price=Decimal("1.1"),
                low_price=Decimal("0.9"),
                close_price=Decimal("1"),
            )

        def _load_position_state(self, *, user_id: str, strategy_name: str, trading_mode: str, symbol: str) -> PositionState:
            return PositionState(symbol=symbol, quantity=Decimal("0"))

        def _sync_position_runtime_state(
            self,
            *,
            user_id: str,
            strategy_name: str,
            trading_mode: str,
            position_state: PositionState,
            market: MarketSnapshot,
            now: datetime,
        ) -> PositionState:
            return position_state

        def _generate_signal(
            self,
            *,
            tenant_id,
            strategy_name: str,
            trading_mode: TradingMode,
            market: MarketSnapshot,
            position_state: PositionState,
            config: dict[str, object],
        ) -> StrategySignal:
            return StrategySignal(
                signal_id=uuid.uuid4(),
                correlation_id=uuid.uuid4(),
                tenant_id=uuid.uuid4(),
                strategy_id=strategy_name,
                trading_mode=trading_mode,
                signal_type=SignalType.HOLD,
                confidence=0.5,
                reason=f"checked {market.symbol}",
            )

        def _persist_run(self, **kwargs) -> None:
            return None

        def _persist_signal(self, event: StrategySignalEvent) -> None:
            self.events.append(event)

    runner = FanoutRunner()

    processed = runner.run_once()

    assert processed == 6
    assert {(event.symbol, event.trading_mode.value) for event in runner.events} == {
        ("AERO-USD", "paper"),
        ("AERO-USD", "live"),
        ("BTC-USD", "paper"),
        ("BTC-USD", "live"),
        ("ETH-USD", "paper"),
        ("ETH-USD", "live"),
    }


def test_dca_scheduler_enforces_buy_interval_from_runtime_state():
    class DcaRunner(StrategyRunner):
        def __init__(self):
            super().__init__(engine=None, redis_client=DummyRedis())  # type: ignore[arg-type]
            self.generated = 0

        def _load_enabled_user_strategies(self) -> list[dict[str, str]]:
            return [
                {
                    "user_id": str(uuid.uuid4()),
                    "strategy_id": "dca",
                    "tenant_id": uuid.uuid4(),
                    "config": {},
                }
            ]

        def _load_platform_strategy_config(self, strategy_id: str) -> dict[str, object]:
            return {"strategy_params": {"buy_interval_hours": 24}}

        def _load_allowed_symbols(self, user_id: str) -> list[str]:
            return ["BTC-USD"]

        def _load_market_snapshot(self, symbol: str) -> MarketSnapshot | None:
            return MarketSnapshot(
                timestamp=datetime.now(UTC),
                symbol=symbol,
                current_price=Decimal("50000"),
                bid_price=Decimal("49990"),
                ask_price=Decimal("50010"),
                volume_24h=Decimal("100"),
                open_price=Decimal("50000"),
                high_price=Decimal("50500"),
                low_price=Decimal("49500"),
                close_price=Decimal("50000"),
            )

        def _load_position_state(self, *, user_id: str, strategy_name: str, trading_mode: str, symbol: str) -> PositionState:
            return PositionState(symbol=symbol, quantity=Decimal("0"))

        def _sync_position_runtime_state(
            self,
            *,
            user_id: str,
            strategy_name: str,
            trading_mode: str,
            position_state: PositionState,
            market: MarketSnapshot,
            now: datetime,
        ) -> PositionState:
            return position_state

        def _load_strategy_runtime_state(self, *, user_id: str, strategy_name: str, trading_mode: str) -> dict[str, object]:
            last_buy_at = (datetime.now(UTC) - timedelta(hours=1)).isoformat()
            return {"symbols": {"BTC-USD": {"last_buy_at": last_buy_at}}}

        def _generate_signal(
            self,
            *,
            tenant_id,
            strategy_name: str,
            trading_mode: TradingMode,
            market: MarketSnapshot,
            position_state: PositionState,
            config: dict[str, object],
        ) -> StrategySignal:
            self.generated += 1
            return StrategySignal(
                signal_id=uuid.uuid4(),
                correlation_id=uuid.uuid4(),
                tenant_id=uuid.uuid4(),
                strategy_id=strategy_name,
                trading_mode=trading_mode,
                signal_type=SignalType.BUY,
                confidence=0.9,
                reason="scheduled buy",
            )

        def _persist_run(self, **kwargs) -> None:
            return None

        def _persist_signal(self, event: StrategySignalEvent) -> None:
            raise AssertionError("signal should have been scheduled out")

    runner = DcaRunner()

    processed = runner.run_once()

    assert processed == 0
    assert runner.generated == 0
