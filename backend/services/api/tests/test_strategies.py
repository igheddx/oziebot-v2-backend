"""Tests for strategy framework - implementations, registry, and API."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from decimal import Decimal

import pytest
from sqlalchemy import select
from oziebot_api.models.user import User
from oziebot_api.models.strategy_signal_pipeline import StrategySignalRecord
from oziebot_domain.strategy import SignalType
from oziebot_domain.trading_mode import TradingMode
from oziebot_strategy_engine.registry import StrategyRegistry
from oziebot_strategy_engine.strategy import MarketSnapshot, PositionState, StrategyContext
from oziebot_strategy_engine.strategies.momentum import MomentumStrategy
from oziebot_strategy_engine.strategies.day_trading import DayTradingStrategy
from oziebot_strategy_engine.strategies.dca import DCAStrategy


# ============================================================================
# Strategy Implementation Tests
# ============================================================================


class TestMomentumStrategy:
    """Test momentum trading strategy."""

    def test_momentum_validate_config_valid(self):
        """Valid config passes validation."""
        strategy = MomentumStrategy()
        config = {
            "short_window": 8,
            "long_window": 34,
            "strength_threshold": 0.012,
            "position_size_fraction": 0.12,
        }
        assert strategy.validate_config(config)

    def test_momentum_validate_config_invalid_windows(self):
        """Invalid window config raises error."""
        strategy = MomentumStrategy()
        config = {"short_window": 20, "long_window": 5}  # Wrong order
        with pytest.raises(ValueError):
            strategy.validate_config(config)

    def test_momentum_buy_signal_on_positive_momentum(self):
        """Generate BUY signal on positive momentum."""
        strategy = MomentumStrategy()
        config = strategy.get_default_config()

        market = MarketSnapshot(
            timestamp=datetime.now(UTC),
            symbol="BTC",
            current_price=Decimal("50000"),
            bid_price=Decimal("49900"),
            ask_price=Decimal("50100"),
            volume_24h=Decimal("1000"),
            open_price=Decimal("48000"),  # Lower open -> positive momentum
            high_price=Decimal("51000"),
            low_price=Decimal("47000"),
            close_price=Decimal("50500"),
        )

        context = StrategyContext(
            tenant_id=uuid.uuid4(),
            trading_mode=TradingMode.PAPER,
            market_snapshot=market,
            position_state=PositionState("BTC", Decimal(0)),
        )

        signal = strategy.generate_signal(
            context,
            config,
            uuid.uuid4(),
            uuid.uuid4(),
        )

        assert signal.signal_type in (SignalType.BUY, SignalType.HOLD)
        assert signal.trading_mode == TradingMode.PAPER

    def test_momentum_works_in_both_modes(self):
        """Momentum strategy generates signals in both PAPER and LIVE."""
        strategy = MomentumStrategy()
        config = strategy.get_default_config()

        market = MarketSnapshot(
            timestamp=datetime.now(UTC),
            symbol="ETH",
            current_price=Decimal("3000"),
            bid_price=Decimal("2990"),
            ask_price=Decimal("3010"),
            volume_24h=Decimal("2000"),
            open_price=Decimal("2900"),
            high_price=Decimal("3100"),
            low_price=Decimal("2800"),
            close_price=Decimal("3050"),
        )

        # Test PAPER mode
        context_paper = StrategyContext(
            tenant_id=uuid.uuid4(),
            trading_mode=TradingMode.PAPER,
            market_snapshot=market,
            position_state=PositionState("ETH", Decimal(0)),
        )

        signal_paper = strategy.generate_signal(
            context_paper,
            config,
            uuid.uuid4(),
            uuid.uuid4(),
        )

        # Test LIVE mode
        context_live = StrategyContext(
            tenant_id=uuid.uuid4(),
            trading_mode=TradingMode.LIVE,
            market_snapshot=market,
            position_state=PositionState("ETH", Decimal(0)),
        )

        signal_live = strategy.generate_signal(
            context_live,
            config,
            uuid.uuid4(),
            uuid.uuid4(),
        )

        # Both should have trading_mode set correctly
        assert signal_paper.trading_mode == TradingMode.PAPER
        assert signal_live.trading_mode == TradingMode.LIVE
        # Strategy logic should produce same signal type
        assert signal_paper.signal_type == signal_live.signal_type


class TestDayTradingStrategy:
    """Test day trading strategy."""

    def test_day_trading_default_config(self):
        """Day trading has sensible defaults."""
        strategy = DayTradingStrategy()
        config = strategy.get_default_config()
        assert "entry_threshold" in config
        assert "exit_threshold" in config
        assert "stop_loss_pct" in config
        assert config["position_size_fraction"] == 0.15

    def test_day_trading_entry_signal_near_low(self):
        """Generate entry signal when price is near daily low."""
        strategy = DayTradingStrategy()
        config = strategy.get_default_config()

        market = MarketSnapshot(
            timestamp=datetime.now(UTC),
            symbol="SOL",
            current_price=Decimal("95.5"),
            bid_price=Decimal("95.4"),
            ask_price=Decimal("95.6"),
            volume_24h=Decimal("5000"),
            open_price=Decimal("102"),
            high_price=Decimal("110"),
            low_price=Decimal("95"),  # Recently hit low
            close_price=Decimal("95.5"),
        )

        context = StrategyContext(
            tenant_id=uuid.uuid4(),
            trading_mode=TradingMode.PAPER,
            market_snapshot=market,
            position_state=PositionState("SOL", Decimal(0)),
        )

        signal = strategy.generate_signal(context, config, uuid.uuid4(), uuid.uuid4())

        assert signal.signal_type == SignalType.BUY

    def test_day_trading_exit_on_stop_loss(self):
        """Close position when stop loss is hit."""
        strategy = DayTradingStrategy()
        config = strategy.get_default_config()

        market = MarketSnapshot(
            timestamp=datetime.now(UTC),
            symbol="AVAX",
            current_price=Decimal("80"),
            bid_price=Decimal("79"),
            ask_price=Decimal("81"),
            volume_24h=Decimal("3000"),
            open_price=Decimal("100"),
            high_price=Decimal("105"),
            low_price=Decimal("78"),
            close_price=Decimal("80"),
        )

        # Has position from entry at 100
        context = StrategyContext(
            tenant_id=uuid.uuid4(),
            trading_mode=TradingMode.PAPER,
            market_snapshot=market,
            position_state=PositionState("AVAX", Decimal(1), Decimal("100")),
        )

        signal = strategy.generate_signal(context, config, uuid.uuid4(), uuid.uuid4())

        # Should close due to > 1% loss (stop_loss=0.01)
        assert signal.signal_type == SignalType.CLOSE


class TestDCAStrategy:
    """Test DCA strategy."""

    def test_dca_default_config(self):
        """DCA has sensible defaults."""
        strategy = DCAStrategy()
        config = strategy.get_default_config()
        assert config["buy_amount_usd"] == 100
        assert config["buy_interval_hours"] == 24

    def test_dca_always_buys_on_green_day(self):
        """DCA generates buy signal on green day."""
        strategy = DCAStrategy()
        config = {"only_on_green_days": True, "buy_amount_usd": 100}

        market = MarketSnapshot(
            timestamp=datetime.now(UTC),
            symbol="BNB",
            current_price=Decimal("300"),
            bid_price=Decimal("299"),
            ask_price=Decimal("301"),
            volume_24h=Decimal("8000"),
            open_price=Decimal("250"),  # Green day
            high_price=Decimal("310"),
            low_price=Decimal("240"),
            close_price=Decimal("300"),
        )

        context = StrategyContext(
            tenant_id=uuid.uuid4(),
            trading_mode=TradingMode.PAPER,
            market_snapshot=market,
            position_state=PositionState("BNB", Decimal(5), Decimal("250")),
        )

        signal = strategy.generate_signal(context, config, uuid.uuid4(), uuid.uuid4())

        assert signal.signal_type == SignalType.BUY

    def test_dca_skips_red_day_if_configured(self):
        """DCA skips buy on red day when configured."""
        strategy = DCAStrategy()
        config = {"only_on_green_days": True, "buy_amount_usd": 100}

        market = MarketSnapshot(
            timestamp=datetime.now(UTC),
            symbol="XRP",
            current_price=Decimal("0.50"),
            bid_price=Decimal("0.49"),
            ask_price=Decimal("0.51"),
            volume_24h=Decimal("10000"),
            open_price=Decimal("0.55"),  # Red day
            high_price=Decimal("0.60"),
            low_price=Decimal("0.48"),
            close_price=Decimal("0.50"),
        )

        context = StrategyContext(
            tenant_id=uuid.uuid4(),
            trading_mode=TradingMode.PAPER,
            market_snapshot=market,
            position_state=PositionState("XRP", Decimal(0)),
        )

        signal = strategy.generate_signal(context, config, uuid.uuid4(), uuid.uuid4())

        assert signal.signal_type == SignalType.HOLD


# ============================================================================
# Registry Tests
# ============================================================================


class TestStrategyRegistry:
    """Test strategy registry."""

    def test_registry_has_built_in_strategies(self):
        """Registry loaded with built-in strategies."""
        strategies = StrategyRegistry.list_strategies()
        strategy_ids = [s["strategy_id"] for s in strategies]
        assert "momentum" in strategy_ids
        assert "day_trading" in strategy_ids
        assert "dca" in strategy_ids

    def test_get_strategy_instance(self):
        """Retrieve strategy instance from registry."""
        momentum = StrategyRegistry.get_strategy("momentum")
        assert momentum.strategy_id == "momentum"
        assert momentum.display_name == "Momentum Trading"

    def test_strategy_not_found_raises_error(self):
        """Unknown strategy raises KeyError."""
        with pytest.raises(KeyError):
            StrategyRegistry.get_strategy("nonexistent_strategy")

    def test_duplicate_registration_raises_error(self):
        """Cannot register same strategy twice."""
        with pytest.raises(ValueError):
            StrategyRegistry.register(MomentumStrategy)

    def test_strategy_exists_check(self):
        """Check if strategy exists in registry."""
        assert StrategyRegistry.strategy_exists("momentum")
        assert StrategyRegistry.strategy_exists("day_trading")
        assert StrategyRegistry.strategy_exists("dca")
        assert not StrategyRegistry.strategy_exists("fake_strategy")


# ============================================================================
# API Endpoint Tests
# ============================================================================


class TestStrategyAPI:
    """Test strategy API endpoints."""

    def test_list_available_strategies(self, client):
        """Admin can list available strategies."""
        r = client.get("/v1/me/strategies/available")
        assert r.status_code == 200
        data = r.json()
        assert "strategies" in data
        assert data["total"] >= 3  # At least momentum, day_trading, dca

    def test_create_strategy_requires_auth(self, client):
        """Creating strategy requires authentication."""
        r = client.post(
            "/v1/me/strategies",
            json={"strategy_id": "momentum"},
        )
        assert r.status_code == 401

    def test_create_strategy_for_user(self, client, regular_user_and_token):
        """User can add strategy to their account."""
        _, token = regular_user_and_token

        r = client.post(
            "/v1/me/strategies",
            headers={"Authorization": f"Bearer {token}"},
            json={
                "strategy_id": "momentum",
                "is_enabled": True,
                "config": {"short_window": 5, "long_window": 20},
            },
        )

        assert r.status_code == 200
        data = r.json()
        assert data["strategy_id"] == "momentum"
        assert data["is_enabled"] is True

    def test_cannot_add_nonexistent_strategy(self, client, regular_user_and_token):
        """Cannot add unknown strategy."""
        _, token = regular_user_and_token

        r = client.post(
            "/v1/me/strategies",
            headers={"Authorization": f"Bearer {token}"},
            json={"strategy_id": "fake_strategy"},
        )

        assert r.status_code == 404

    def test_cannot_duplicate_strategy(self, client, regular_user_and_token):
        """Cannot add same strategy twice."""
        _, token = regular_user_and_token

        # Add first time
        client.post(
            "/v1/me/strategies",
            headers={"Authorization": f"Bearer {token}"},
            json={"strategy_id": "dca"},
        )

        # Try to add again
        r = client.post(
            "/v1/me/strategies",
            headers={"Authorization": f"Bearer {token}"},
            json={"strategy_id": "dca"},
        )

        assert r.status_code == 409

    def test_list_user_strategies(self, client, regular_user_and_token):
        """User can list their configured strategies."""
        _, token = regular_user_and_token

        # Add a strategy
        client.post(
            "/v1/me/strategies",
            headers={"Authorization": f"Bearer {token}"},
            json={"strategy_id": "momentum"},
        )

        # List strategies
        r = client.get(
            "/v1/me/strategies",
            headers={"Authorization": f"Bearer {token}"},
        )

        assert r.status_code == 200
        data = r.json()
        assert data["total"] >= 1

    def test_update_strategy_config(self, client, regular_user_and_token):
        """User can update strategy configuration."""
        _, token = regular_user_and_token

        # Create strategy
        client.post(
            "/v1/me/strategies",
            headers={"Authorization": f"Bearer {token}"},
            json={"strategy_id": "day_trading", "is_enabled": True},
        )

        # Update config
        r = client.patch(
            "/v1/me/strategies/day_trading",
            headers={"Authorization": f"Bearer {token}"},
            json={
                "config": {
                    "entry_threshold": 0.02,
                    "exit_threshold": 0.03,
                    "stop_loss": 0.01,
                }
            },
        )

        assert r.status_code == 200
        data = r.json()
        assert data["config"]["entry_threshold"] == 0.02

    def test_disable_strategy(self, client, regular_user_and_token):
        """User can disable strategy."""
        _, token = regular_user_and_token

        # Create strategy
        client.post(
            "/v1/me/strategies",
            headers={"Authorization": f"Bearer {token}"},
            json={"strategy_id": "dca", "is_enabled": True},
        )

        # Disable it
        r = client.patch(
            "/v1/me/strategies/dca",
            headers={"Authorization": f"Bearer {token}"},
            json={"is_enabled": False},
        )

        assert r.status_code == 200
        assert r.json()["is_enabled"] is False

    def test_strategy_state_upsert_and_get(self, client, regular_user_and_token):
        """User can persist and fetch strategy runtime state per mode."""
        _, token = regular_user_and_token

        client.post(
            "/v1/me/strategies",
            headers={"Authorization": f"Bearer {token}"},
            json={"strategy_id": "momentum", "is_enabled": True},
        )

        put_state = client.put(
            "/v1/me/strategies/momentum/state",
            headers={"Authorization": f"Bearer {token}"},
            json={
                "trading_mode": "paper",
                "state": {"last_price": "50000", "position_open": False},
            },
        )
        assert put_state.status_code == 200
        assert put_state.json()["trading_mode"] == "paper"

        get_state = client.get(
            "/v1/me/strategies/momentum/state",
            headers={"Authorization": f"Bearer {token}"},
            params={"trading_mode": "paper"},
        )
        assert get_state.status_code == 200
        assert get_state.json()["state"]["last_price"] == "50000"

    def test_strategy_state_is_partitioned_by_mode(self, client, regular_user_and_token):
        """PAPER and LIVE states are independent for the same strategy."""
        _, token = regular_user_and_token

        client.post(
            "/v1/me/strategies",
            headers={"Authorization": f"Bearer {token}"},
            json={"strategy_id": "dca", "is_enabled": True},
        )

        r1 = client.put(
            "/v1/me/strategies/dca/state",
            headers={"Authorization": f"Bearer {token}"},
            json={"trading_mode": "paper", "state": {"cycles_completed": 3}},
        )
        r2 = client.put(
            "/v1/me/strategies/dca/state",
            headers={"Authorization": f"Bearer {token}"},
            json={"trading_mode": "live", "state": {"cycles_completed": 1}},
        )
        assert r1.status_code == 200
        assert r2.status_code == 200

        paper = client.get(
            "/v1/me/strategies/dca/state",
            headers={"Authorization": f"Bearer {token}"},
            params={"trading_mode": "paper"},
        )
        live = client.get(
            "/v1/me/strategies/dca/state",
            headers={"Authorization": f"Bearer {token}"},
            params={"trading_mode": "live"},
        )
        assert paper.status_code == 200
        assert live.status_code == 200
        assert paper.json()["state"]["cycles_completed"] == 3
        assert live.json()["state"]["cycles_completed"] == 1

    def test_get_strategy_signals_reads_current_signal_pipeline_table(
        self, client, db_session, regular_user_and_token
    ):
        email, token = regular_user_and_token
        user = db_session.scalars(select(User).where(User.email == email)).one()
        row = StrategySignalRecord(
            signal_id=uuid.uuid4(),
            run_id=uuid.uuid4(),
            user_id=user.id,
            strategy_name="momentum",
            symbol="BTC-USD",
            action="buy",
            confidence=0.81,
            suggested_size="0.12",
            reasoning_metadata={"reason": "bullish crossover"},
            trading_mode="paper",
            timestamp=datetime.now(UTC),
        )
        db_session.add(row)
        db_session.commit()

        response = client.get(
            "/v1/me/strategies/momentum/signals",
            headers={"Authorization": f"Bearer {token}"},
        )

        assert response.status_code == 200
        payload = response.json()
        assert payload["total_fetched"] == 1
        assert payload["signals"][0]["strategy_id"] == "momentum"
        assert payload["signals"][0]["signal_type"] == "buy"
        assert payload["signals"][0]["reason"] == "bullish crossover"


# ============================================================================
# Extension Example - How to Add New Strategy
# ============================================================================


class CustomZigzagStrategy:
    """Example of adding a new strategy (not yet registered)."""

    strategy_id = "zigzag"
    display_name = "Zigzag Trading"
    description = "Trades on zigzag pattern reversals"
    version = "1.0"

    def validate_config(self, config: dict) -> bool:
        return True

    def generate_signal(self, context, config, signal_id, correlation_id):
        from oziebot_domain.strategy import StrategySignal

        return StrategySignal(
            signal_id=signal_id,
            correlation_id=correlation_id,
            tenant_id=context.tenant_id,
            strategy_id=self.strategy_id,
            strategy_version=self.version,
            trading_mode=context.trading_mode,
            signal_type=SignalType.HOLD,
            confidence=0.5,
            reason="Zigzag strategy (demo)",
        )


def test_adding_new_strategy_to_registry():
    """
    Example: How to add a new strategy to the framework.

    To add a new strategy:
    1. Create a class inheriting from TradingStrategy
    2. Implement validate_config() and generate_signal()
    3. Call StrategyRegistry.register(YourStrategy)
    4. Done! No other code changes needed.
    """
    # This would normally be done in strategies/__init__.py
    # but here we demonstrate it works
    initial_count = len(StrategyRegistry.list_strategies())

    # Register custom strategy
    StrategyRegistry.register(CustomZigzagStrategy)

    # Verify it's available
    new_count = len(StrategyRegistry.list_strategies())
    assert new_count == initial_count + 1
    assert StrategyRegistry.strategy_exists("zigzag")

    # Can be used immediately
    strategy = StrategyRegistry.get_strategy("zigzag")
    assert strategy.strategy_id == "zigzag"
