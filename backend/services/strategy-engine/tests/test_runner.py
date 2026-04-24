from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

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


def _setup_intelligence_db(db_path: Path) -> None:
    eng = create_engine(f"sqlite+pysqlite:///{db_path}")
    with eng.begin() as conn:
        conn.execute(
            text(
                "CREATE TABLE strategy_runs (id TEXT PRIMARY KEY, run_id TEXT, user_id TEXT, strategy_name TEXT, symbol TEXT, trading_mode TEXT, status TEXT, trace_id TEXT, metadata TEXT, started_at TEXT, completed_at TEXT)"
            )
        )
        conn.execute(
            text(
                "CREATE TABLE strategy_signals (id TEXT PRIMARY KEY, signal_id TEXT, run_id TEXT, user_id TEXT, strategy_name TEXT, symbol TEXT, action TEXT, confidence REAL, suggested_size TEXT, reasoning_metadata TEXT, trading_mode TEXT, timestamp TEXT)"
            )
        )
        conn.execute(
            text(
                "CREATE TABLE strategy_signal_snapshots (id TEXT PRIMARY KEY, user_id TEXT, tenant_id TEXT, trading_mode TEXT, strategy_name TEXT, token_symbol TEXT, timestamp TEXT, current_price TEXT, best_bid TEXT, best_ask TEXT, spread_pct TEXT, estimated_slippage_pct TEXT, volume TEXT, volatility TEXT, confidence_score REAL, raw_feature_json TEXT, token_policy_status TEXT, token_policy_multiplier TEXT)"
            )
        )
        conn.execute(
            text(
                "CREATE TABLE platform_settings (key TEXT PRIMARY KEY, value TEXT, updated_at TEXT, updated_by_user_id TEXT)"
            )
        )
        conn.execute(
            text(
                "CREATE TABLE platform_token_allowlist (id TEXT PRIMARY KEY, symbol TEXT, quote_currency TEXT, is_enabled BOOLEAN)"
            )
        )
        conn.execute(
            text(
                "CREATE TABLE token_strategy_policy (id TEXT PRIMARY KEY, token_id TEXT, strategy_id TEXT, admin_enabled BOOLEAN, recommendation_status TEXT, recommendation_reason TEXT, recommendation_status_override TEXT, recommendation_reason_override TEXT, max_position_pct_override REAL)"
            )
        )
        conn.execute(
            text(
                "CREATE TABLE strategy_decision_audits (id TEXT PRIMARY KEY, signal_snapshot_id TEXT, stage TEXT, decision TEXT, reason_code TEXT, reason_detail TEXT, size_before TEXT, size_after TEXT, created_at TEXT)"
            )
        )
        conn.execute(
            text(
                "CREATE TABLE ai_inference_records (id TEXT PRIMARY KEY, signal_snapshot_id TEXT, model_name TEXT, model_version TEXT, recommendation TEXT, confidence_score REAL, explanation_json TEXT, created_at TEXT)"
            )
        )
        conn.execute(
            text(
                "CREATE TABLE strategy_capital_buckets (id TEXT PRIMARY KEY, user_id TEXT, strategy_id TEXT, trading_mode TEXT, assigned_capital_cents INTEGER, available_buying_power_cents INTEGER, reserved_cash_cents INTEGER, locked_capital_cents INTEGER, realized_pnl_cents INTEGER)"
            )
        )
        conn.execute(
            text(
                "CREATE TABLE strategy_capital_ledger (id TEXT PRIMARY KEY, user_id TEXT, strategy_id TEXT, trading_mode TEXT, event_type TEXT, metadata TEXT, created_at TEXT)"
            )
        )


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


def test_runner_applies_dynamic_bucket_sizing_to_buy_signal(tmp_path: Path):
    db_path = tmp_path / "runner-dynamic-sizing.sqlite"
    _setup_intelligence_db(db_path)
    engine = create_engine(f"sqlite+pysqlite:///{db_path}")
    user_id = "4f095c5a-34c1-4dbc-bf09-8c35b3601ea1"
    now = datetime.now(UTC)
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO strategy_capital_buckets (id, user_id, strategy_id, trading_mode, assigned_capital_cents, available_buying_power_cents, reserved_cash_cents, locked_capital_cents, realized_pnl_cents) "
                "VALUES (:id, :user_id, 'momentum', 'paper', 200000, 200000, 0, 0, 0)"
            ),
            {"id": uuid.uuid4().hex, "user_id": user_id},
        )

    runner = StrategyRunner(engine=engine, redis_client=DummyRedis())
    market = MarketSnapshot(
        timestamp=now,
        symbol="BTC-USD",
        current_price=Decimal("100"),
        bid_price=Decimal("99.5"),
        ask_price=Decimal("100.5"),
        volume_24h=Decimal("1000"),
        open_price=Decimal("95"),
        high_price=Decimal("101"),
        low_price=Decimal("94"),
        close_price=Decimal("100"),
    )
    signal = StrategySignal(
        signal_id=uuid.uuid4(),
        correlation_id=uuid.uuid4(),
        tenant_id=uuid.uuid4(),
        strategy_id="momentum",
        trading_mode=TradingMode.PAPER,
        signal_type=SignalType.BUY,
        confidence=0.8,
        reason="dynamic sizing",
        metadata={"position_size_fraction": 0.25},
    )
    sized = runner._apply_dynamic_position_sizing(
        user_id=user_id,
        strategy_name="momentum",
        trading_mode="paper",
        signal=signal,
        market=market,
        position_state=PositionState(symbol="BTC-USD", quantity=Decimal("0")),
        config={
            "dynamic_sizing_enabled": True,
            "min_trade_usd": 75,
            "max_trade_usd": 300,
            "target_bucket_utilization_pct": 0.65,
            "drawdown_size_reduction_enabled": True,
            "drawdown_reduction_multiplier": 0.75,
        },
        risk_caps={"max_position_usd": 300},
    )

    assert sized.metadata is not None
    assert sized.metadata["sizing"]["final_trade_usd"] == "300.00"

    event = StrategyRunner._to_signal_event(
        run_id=uuid.uuid4(),
        user_id=uuid.UUID(user_id),
        strategy_name="momentum",
        symbol="BTC-USD",
        signal=sized,
        trading_mode=TradingMode.PAPER,
        timestamp=now,
        market=market,
    )

    assert event.suggested_size == Decimal("3")


def test_runner_resolves_all_allowed_symbols_by_default():
    runner = StrategyRunner(engine=None, redis_client=DummyRedis())  # type: ignore[arg-type]

    assert runner._resolve_symbols(
        config={}, allowed_symbols=["AERO-USD", "BTC-USD"]
    ) == ["AERO-USD", "BTC-USD"]
    assert runner._resolve_symbols(
        config={"symbol": "BTC-USD"}, allowed_symbols=["AERO-USD", "BTC-USD"]
    ) == ["BTC-USD"]
    assert runner._resolve_symbols(
        config={"symbols": ["BTC-USD", "DOGE-USD"]},
        allowed_symbols=["AERO-USD", "BTC-USD", "ETH-USD"],
    ) == ["BTC-USD"]


def test_runner_applies_more_permissive_paper_controls():
    config, signal_rules, risk_caps = StrategyRunner._paper_relaxed_controls(
        strategy_name="day_trading",
        config={
            "entry_threshold": 0.007,
            "min_volume_multiplier": 1.3,
            "min_volatility_pct": 0.005,
            "require_trend_alignment": True,
            "breakout_lookback_candles": 5,
        },
        signal_rules={
            "min_confidence": 0.6,
            "cooldown_seconds": 20,
            "max_signals_per_day": 6,
            "only_during_liquid_hours": True,
            "require_volume_confirmation": True,
        },
        risk_caps={"max_open_positions": 1, "max_daily_loss_pct": 0.12},
    )

    assert config["entry_threshold"] == 0.03
    assert config["min_volume_multiplier"] == 1.0
    assert config["min_volatility_pct"] == 0.002
    assert config["require_trend_alignment"] is False
    assert config["breakout_lookback_candles"] == 3
    assert signal_rules["min_confidence"] == 0.45
    assert signal_rules["cooldown_seconds"] == 0
    assert signal_rules["max_signals_per_day"] == 0
    assert signal_rules["only_during_liquid_hours"] is False
    assert signal_rules["require_volume_confirmation"] is False
    assert risk_caps["max_open_positions"] == 0
    assert risk_caps["max_daily_loss_pct"] == 0


def test_runner_coerces_legacy_and_multi_symbol_runtime_state():
    assert StrategyRunner._coerce_symbol_runtime_states(
        {
            "symbol": "AERO-USD",
            "peak_price": "0.50",
            "opened_at": "2026-04-17T16:00:00+00:00",
        }
    ) == {"AERO-USD": {"peak_price": "0.50", "opened_at": "2026-04-17T16:00:00+00:00"}}
    assert StrategyRunner._coerce_symbol_runtime_states(
        {
            "symbols": {
                "BTC-USD": {"peak_price": "51000"},
                "ETH-USD": {"opened_at": "2026-04-17T16:01:00+00:00"},
            }
        }
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
        position_state=PositionState(
            symbol="BTC-USD", quantity=Decimal("1"), entry_price=Decimal("50000")
        ),
        market=market,
        now=now,
    )
    state = StrategyRunner._merge_symbol_runtime_states(
        state,
        position_state=PositionState(
            symbol="ETH-USD", quantity=Decimal("2"), entry_price=Decimal("2500")
        ),
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


def test_run_once_processes_only_current_trading_mode_symbols():
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
                    "current_trading_mode": "paper",
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

        def _load_position_state(
            self, *, user_id: str, strategy_name: str, trading_mode: str, symbol: str
        ) -> PositionState:
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

    assert processed == 3
    assert {(event.symbol, event.trading_mode.value) for event in runner.events} == {
        ("AERO-USD", "paper"),
        ("BTC-USD", "paper"),
        ("ETH-USD", "paper"),
    }


def test_run_once_keeps_open_position_symbols_when_entries_disabled():
    class ExitAwareRunner(StrategyRunner):
        def __init__(self):
            super().__init__(engine=None, redis_client=DummyRedis())  # type: ignore[arg-type]
            self.events: list[StrategySignalEvent] = []

        def _load_enabled_user_strategies(self) -> list[dict[str, str]]:
            return [
                {
                    "user_id": str(uuid.uuid4()),
                    "strategy_id": "momentum",
                    "tenant_id": uuid.uuid4(),
                    "config": {},
                }
            ]

        def _load_platform_strategy_config(self, strategy_id: str) -> dict[str, object]:
            return {}

        def _load_allowed_symbols(self, user_id: str) -> list[str]:
            return ["BTC-USD"]

        def _load_open_position_symbols(
            self, *, user_id: str, strategy_name: str, trading_mode: str
        ) -> list[str]:
            if trading_mode == "live":
                return ["SOL-USD"]
            return []

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

        def _load_position_state(
            self, *, user_id: str, strategy_name: str, trading_mode: str, symbol: str
        ) -> PositionState:
            quantity = (
                Decimal("1")
                if symbol == "SOL-USD" and trading_mode == "live"
                else Decimal("0")
            )
            return PositionState(
                symbol=symbol, quantity=quantity, entry_price=Decimal("1")
            )

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

    runner = ExitAwareRunner()

    processed = runner.run_once()

    assert processed == 2
    assert {(event.symbol, event.trading_mode.value) for event in runner.events} == {
        ("BTC-USD", "paper"),
        ("SOL-USD", "live"),
    }


def test_run_once_persists_signal_snapshots_and_ai_inference(tmp_path: Path):
    db_path = tmp_path / "runner-intelligence.sqlite"
    _setup_intelligence_db(db_path)

    class IntelligenceRunner(StrategyRunner):
        def __init__(self):
            super().__init__(
                engine=create_engine(f"sqlite+pysqlite:///{db_path}"),
                redis_client=DummyRedis(),
            )

        def _load_enabled_user_strategies(self) -> list[dict[str, str]]:
            return [
                {
                    "user_id": str(uuid.uuid4()),
                    "strategy_id": "momentum",
                    "tenant_id": uuid.uuid4(),
                    "config": {},
                }
            ]

        def _load_platform_strategy_config(self, strategy_id: str) -> dict[str, object]:
            return {"strategy_params": {"short_window": 3, "long_window": 5}}

        def _load_allowed_symbols(self, user_id: str) -> list[str]:
            return ["BTC-USD"]

        def _load_market_snapshot(self, symbol: str) -> MarketSnapshot | None:
            return MarketSnapshot(
                timestamp=datetime.now(UTC),
                symbol=symbol,
                current_price=Decimal("50000"),
                bid_price=Decimal("49990"),
                ask_price=Decimal("50010"),
                volume_24h=Decimal("1000"),
                open_price=Decimal("49000"),
                high_price=Decimal("50100"),
                low_price=Decimal("48900"),
                close_price=Decimal("50000"),
                candle_closes=[49000, 49200, 49500, 49800, 50000],
            )

        def _load_position_state(
            self, *, user_id: str, strategy_name: str, trading_mode: str, symbol: str
        ) -> PositionState:
            return PositionState(symbol=symbol, quantity=Decimal("0"))

        def _sync_position_runtime_state(self, **kwargs) -> PositionState:
            return kwargs["position_state"]

        def _generate_signal(self, **kwargs) -> StrategySignal:
            return StrategySignal(
                signal_id=uuid.uuid4(),
                correlation_id=uuid.uuid4(),
                tenant_id=uuid.uuid4(),
                strategy_id="momentum",
                trading_mode=kwargs["trading_mode"],
                signal_type=SignalType.BUY,
                confidence=0.82,
                reason="bullish crossover",
                quantity=Quantity(amount="0.12"),
            )

    runner = IntelligenceRunner()
    processed = runner.run_once()

    eng = create_engine(f"sqlite+pysqlite:///{db_path}")
    with eng.begin() as conn:
        snapshot_rows = conn.execute(
            text(
                "SELECT trading_mode, raw_feature_json FROM strategy_signal_snapshots ORDER BY trading_mode"
            )
        ).all()
        ai_count = conn.execute(
            text("SELECT COUNT(*) FROM ai_inference_records")
        ).scalar_one()
    assert processed == 1
    assert [row[0] for row in snapshot_rows] == ["paper"]
    assert ai_count == 1
    assert "momentum_value" in json.loads(snapshot_rows[0][1])


def test_run_once_continues_when_trade_intelligence_persistence_fails(
    tmp_path: Path, monkeypatch
):
    db_path = tmp_path / "runner-intelligence-failure.sqlite"
    _setup_intelligence_db(db_path)

    class ResilientRunner(StrategyRunner):
        def __init__(self):
            super().__init__(
                engine=create_engine(f"sqlite+pysqlite:///{db_path}"),
                redis_client=DummyRedis(),
            )

        def _load_enabled_user_strategies(self) -> list[dict[str, str]]:
            return [
                {
                    "user_id": str(uuid.uuid4()),
                    "strategy_id": "momentum",
                    "tenant_id": uuid.uuid4(),
                    "config": {},
                }
            ]

        def _load_platform_strategy_config(self, strategy_id: str) -> dict[str, object]:
            return {"strategy_params": {"short_window": 3, "long_window": 5}}

        def _load_allowed_symbols(self, user_id: str) -> list[str]:
            return ["BTC-USD"]

        def _load_market_snapshot(self, symbol: str) -> MarketSnapshot | None:
            return MarketSnapshot(
                timestamp=datetime.now(UTC),
                symbol=symbol,
                current_price=Decimal("50000"),
                bid_price=Decimal("49990"),
                ask_price=Decimal("50010"),
                volume_24h=Decimal("1000"),
                open_price=Decimal("49000"),
                high_price=Decimal("50100"),
                low_price=Decimal("48900"),
                close_price=Decimal("50000"),
                candle_closes=[49000, 49200, 49500, 49800, 50000],
            )

        def _load_position_state(
            self, *, user_id: str, strategy_name: str, trading_mode: str, symbol: str
        ) -> PositionState:
            return PositionState(symbol=symbol, quantity=Decimal("0"))

        def _sync_position_runtime_state(self, **kwargs) -> PositionState:
            return kwargs["position_state"]

        def _generate_signal(self, **kwargs) -> StrategySignal:
            return StrategySignal(
                signal_id=uuid.uuid4(),
                correlation_id=uuid.uuid4(),
                tenant_id=uuid.uuid4(),
                strategy_id="momentum",
                trading_mode=kwargs["trading_mode"],
                signal_type=SignalType.BUY,
                confidence=0.82,
                reason="bullish crossover",
                quantity=Quantity(amount="0.12"),
            )

    monkeypatch.setattr(
        "oziebot_strategy_engine.runner.persist_signal_snapshot",
        lambda *args, **kwargs: (_ for _ in ()).throw(SQLAlchemyError("boom")),
    )

    runner = ResilientRunner()
    processed = runner.run_once()

    eng = create_engine(f"sqlite+pysqlite:///{db_path}")
    with eng.begin() as conn:
        signal_count = conn.execute(
            text("SELECT COUNT(*) FROM strategy_signals")
        ).scalar_one()
        ai_count = conn.execute(
            text("SELECT COUNT(*) FROM ai_inference_records")
        ).scalar_one()
    assert processed == 1
    assert signal_count == 1
    assert ai_count == 0


def test_run_once_continues_when_decision_audit_persistence_fails(
    tmp_path: Path, monkeypatch
):
    db_path = tmp_path / "runner-audit-failure.sqlite"
    _setup_intelligence_db(db_path)

    class ResilientRunner(StrategyRunner):
        def __init__(self):
            super().__init__(
                engine=create_engine(f"sqlite+pysqlite:///{db_path}"),
                redis_client=DummyRedis(),
            )

        def _load_enabled_user_strategies(self) -> list[dict[str, str]]:
            return [
                {
                    "user_id": str(uuid.uuid4()),
                    "strategy_id": "momentum",
                    "tenant_id": uuid.uuid4(),
                    "config": {},
                }
            ]

        def _load_platform_strategy_config(self, strategy_id: str) -> dict[str, object]:
            return {"strategy_params": {"short_window": 3, "long_window": 5}}

        def _load_allowed_symbols(self, user_id: str) -> list[str]:
            return ["BTC-USD"]

        def _load_market_snapshot(self, symbol: str) -> MarketSnapshot | None:
            return MarketSnapshot(
                timestamp=datetime.now(UTC),
                symbol=symbol,
                current_price=Decimal("50000"),
                bid_price=Decimal("49990"),
                ask_price=Decimal("50010"),
                volume_24h=Decimal("1000"),
                open_price=Decimal("49000"),
                high_price=Decimal("50100"),
                low_price=Decimal("48900"),
                close_price=Decimal("50000"),
                candle_closes=[49000, 49200, 49500, 49800, 50000],
            )

        def _load_position_state(
            self, *, user_id: str, strategy_name: str, trading_mode: str, symbol: str
        ) -> PositionState:
            return PositionState(symbol=symbol, quantity=Decimal("0"))

        def _sync_position_runtime_state(self, **kwargs) -> PositionState:
            return kwargs["position_state"]

        def _generate_signal(self, **kwargs) -> StrategySignal:
            return StrategySignal(
                signal_id=uuid.uuid4(),
                correlation_id=uuid.uuid4(),
                tenant_id=uuid.uuid4(),
                strategy_id="momentum",
                trading_mode=kwargs["trading_mode"],
                signal_type=SignalType.BUY,
                confidence=0.82,
                reason="bullish crossover",
                quantity=Quantity(amount="0.12"),
            )

    monkeypatch.setattr(
        "oziebot_strategy_engine.runner.persist_decision_audit",
        lambda *args, **kwargs: (_ for _ in ()).throw(SQLAlchemyError("boom")),
    )

    runner = ResilientRunner()
    processed = runner.run_once()

    eng = create_engine(f"sqlite+pysqlite:///{db_path}")
    with eng.begin() as conn:
        signal_count = conn.execute(
            text("SELECT COUNT(*) FROM strategy_signals")
        ).scalar_one()
    assert processed == 1
    assert signal_count == 1


def test_run_once_persists_suppression_audit(tmp_path: Path):
    db_path = tmp_path / "runner-suppression.sqlite"
    _setup_intelligence_db(db_path)

    class SuppressedRunner(StrategyRunner):
        def __init__(self):
            super().__init__(
                engine=create_engine(f"sqlite+pysqlite:///{db_path}"),
                redis_client=DummyRedis(),
            )

        def _load_enabled_user_strategies(self) -> list[dict[str, str]]:
            return [
                {
                    "user_id": str(uuid.uuid4()),
                    "strategy_id": "momentum",
                    "tenant_id": uuid.uuid4(),
                    "config": {},
                }
            ]

        def _load_platform_strategy_config(self, strategy_id: str) -> dict[str, object]:
            return {}

        def _load_allowed_symbols(self, user_id: str) -> list[str]:
            return ["BTC-USD"]

        def _load_market_snapshot(self, symbol: str) -> MarketSnapshot | None:
            return MarketSnapshot(
                timestamp=datetime.now(UTC),
                symbol=symbol,
                current_price=Decimal("50000"),
                bid_price=Decimal("49990"),
                ask_price=Decimal("50010"),
                volume_24h=Decimal("1000"),
                open_price=Decimal("49000"),
                high_price=Decimal("50100"),
                low_price=Decimal("48900"),
                close_price=Decimal("50000"),
            )

        def _load_position_state(
            self, *, user_id: str, strategy_name: str, trading_mode: str, symbol: str
        ) -> PositionState:
            return PositionState(symbol=symbol, quantity=Decimal("0"))

        def _sync_position_runtime_state(self, **kwargs) -> PositionState:
            return kwargs["position_state"]

        def _generate_signal(self, **kwargs) -> StrategySignal:
            return StrategySignal(
                signal_id=uuid.uuid4(),
                correlation_id=uuid.uuid4(),
                tenant_id=uuid.uuid4(),
                strategy_id="momentum",
                trading_mode=kwargs["trading_mode"],
                signal_type=SignalType.BUY,
                confidence=0.4,
                reason="weak signal",
                quantity=Quantity(amount="0.10"),
            )

        def _suppression_reason(self, **kwargs) -> str | None:
            return "min_confidence"

    runner = SuppressedRunner()
    runner.run_once()

    eng = create_engine(f"sqlite+pysqlite:///{db_path}")
    with eng.begin() as conn:
        rows = conn.execute(
            text(
                "SELECT stage, decision, reason_code FROM strategy_decision_audits ORDER BY created_at"
            )
        ).all()
    assert rows
    assert all(row[0] == "suppression" for row in rows)
    assert all(row[1] == "rejected" for row in rows)
    assert all(row[2] == "min_confidence" for row in rows)


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

        def _load_position_state(
            self, *, user_id: str, strategy_name: str, trading_mode: str, symbol: str
        ) -> PositionState:
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

        def _load_strategy_runtime_state(
            self, *, user_id: str, strategy_name: str, trading_mode: str
        ) -> dict[str, object]:
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


def test_momentum_runner_skips_blocked_token_policy():
    class PolicyRunner(StrategyRunner):
        def __init__(self):
            super().__init__(engine=None, redis_client=DummyRedis())  # type: ignore[arg-type]
            self.generated = 0

        def _load_enabled_user_strategies(self) -> list[dict[str, str]]:
            return [
                {
                    "user_id": str(uuid.uuid4()),
                    "strategy_id": "momentum",
                    "tenant_id": uuid.uuid4(),
                    "config": {},
                }
            ]

        def _load_platform_strategy_config(self, strategy_id: str) -> dict[str, object]:
            return {}

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

        def _load_position_state(
            self, *, user_id: str, strategy_name: str, trading_mode: str, symbol: str
        ) -> PositionState:
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

        def _load_token_strategy_policy(
            self, *, symbol: str, strategy_name: str
        ) -> dict[str, object] | None:
            return {
                "admin_enabled": True,
                "recommendation_status": "allowed",
                "recommendation_reason": "computed allowed",
                "recommendation_status_override": "blocked",
                "recommendation_reason_override": "blocked token",
            }

        def _generate_signal(self, **kwargs) -> StrategySignal:
            self.generated += 1
            raise AssertionError("blocked token should not reach strategy generation")

        def _persist_run(self, **kwargs) -> None:
            return None

        def _persist_signal(self, event: StrategySignalEvent) -> None:
            raise AssertionError("blocked token should not emit a signal")

    runner = PolicyRunner()
    assert runner.run_once() == 0
    assert runner.generated == 0


def test_mean_reversion_runner_skips_admin_disabled_token_policy():
    class PolicyRunner(StrategyRunner):
        def __init__(self):
            super().__init__(engine=None, redis_client=DummyRedis())  # type: ignore[arg-type]
            self.generated = 0

        def _load_enabled_user_strategies(self) -> list[dict[str, str]]:
            return [
                {
                    "user_id": str(uuid.uuid4()),
                    "strategy_id": "reversion",
                    "tenant_id": uuid.uuid4(),
                    "config": {},
                }
            ]

        def _load_platform_strategy_config(self, strategy_id: str) -> dict[str, object]:
            return {}

        def _load_allowed_symbols(self, user_id: str) -> list[str]:
            return ["ETH-USD"]

        def _load_market_snapshot(self, symbol: str) -> MarketSnapshot | None:
            return MarketSnapshot(
                timestamp=datetime.now(UTC),
                symbol=symbol,
                current_price=Decimal("3000"),
                bid_price=Decimal("2999"),
                ask_price=Decimal("3001"),
                volume_24h=Decimal("250"),
                open_price=Decimal("2950"),
                high_price=Decimal("3025"),
                low_price=Decimal("2940"),
                close_price=Decimal("3000"),
            )

        def _load_position_state(
            self, *, user_id: str, strategy_name: str, trading_mode: str, symbol: str
        ) -> PositionState:
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

        def _load_token_strategy_policy(
            self, *, symbol: str, strategy_name: str
        ) -> dict[str, object] | None:
            return {
                "admin_enabled": False,
                "recommendation_status": "allowed",
                "recommendation_reason": "manually disabled",
            }

        def _generate_signal(self, **kwargs) -> StrategySignal:
            self.generated += 1
            raise AssertionError(
                "admin-disabled token should not reach strategy generation"
            )

        def _persist_run(self, **kwargs) -> None:
            return None

        def _persist_signal(self, event: StrategySignalEvent) -> None:
            raise AssertionError("admin-disabled token should not emit a signal")

    runner = PolicyRunner()
    assert runner.run_once() == 0
    assert runner.generated == 0


def test_apply_token_policy_to_signal_returns_updated_copy_for_frozen_signal():
    runner = StrategyRunner(engine=None, redis_client=DummyRedis())  # type: ignore[arg-type]
    signal = StrategySignal(
        signal_id=uuid.uuid4(),
        correlation_id=uuid.uuid4(),
        tenant_id=uuid.uuid4(),
        strategy_id="momentum",
        trading_mode=TradingMode.PAPER,
        signal_type=SignalType.BUY,
        confidence=0.8,
        reason="entry",
        metadata={"price": "50000"},
    )

    updated = runner._apply_token_policy_to_signal(
        signal=signal,
        token_policy={
            "admin_enabled": True,
            "recommendation_status": "allowed",
            "recommendation_reason": "healthy market",
            "size_multiplier": "0.75",
            "max_position_pct_override": None,
        },
        trading_mode=TradingMode.PAPER,
    )

    assert updated is not signal
    assert signal.metadata == {"price": "50000"}
    assert updated.metadata is not None
    assert updated.metadata["price"] == "50000"
    assert updated.metadata["token_policy"] == {
        "admin_enabled": True,
        "computed_recommendation_status": "allowed",
        "recommendation_status": "allowed",
        "recommendation_reason": "healthy market",
        "size_multiplier": "1",
        "max_position_pct_override": None,
    }


def test_runner_requires_max_position_usd_for_fractional_sizing():
    class SizingRunner(StrategyRunner):
        def __init__(self):
            super().__init__(engine=None, redis_client=DummyRedis())  # type: ignore[arg-type]
            self.generated = 0

        def _load_enabled_user_strategies(self) -> list[dict[str, str]]:
            return [
                {
                    "user_id": str(uuid.uuid4()),
                    "strategy_id": "momentum",
                    "tenant_id": uuid.uuid4(),
                    "config": {},
                }
            ]

        def _load_platform_strategy_config(self, strategy_id: str) -> dict[str, object]:
            return {"risk_caps": {}}

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

        def _load_position_state(
            self, *, user_id: str, strategy_name: str, trading_mode: str, symbol: str
        ) -> PositionState:
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

        def _generate_signal(self, **kwargs) -> StrategySignal:
            self.generated += 1
            return StrategySignal(
                signal_id=uuid.uuid4(),
                correlation_id=uuid.uuid4(),
                tenant_id=uuid.uuid4(),
                strategy_id="momentum",
                trading_mode=kwargs["trading_mode"],
                signal_type=SignalType.BUY,
                confidence=0.8,
                reason="fractional entry",
                metadata={"position_size_fraction": 0.1},
            )

        def _persist_run(self, **kwargs) -> None:
            return None

        def _persist_signal(self, event: StrategySignalEvent) -> None:
            raise AssertionError(
                "signal should be suppressed when max_position_usd is missing"
            )

    runner = SizingRunner()

    assert runner.run_once() == 0
    assert runner.generated == 1
    assert runner.metrics_snapshot()["signals_rejected"] == 1
    assert (
        runner.metrics_snapshot()["rejection_reasons"][
            "max_position_usd required for usd-normalized sizing"
        ]
        == 1
    )
