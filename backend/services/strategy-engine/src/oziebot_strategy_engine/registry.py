"""Load strategies via entry points (group: oziebot.strategies)."""

from __future__ import annotations

import importlib.metadata
from typing import Protocol

from oziebot_domain.intents import TradeIntent
from oziebot_domain.trading_mode import TradingMode
from oziebot_strategy_engine.strategy import TradingStrategy


class Strategy(Protocol):
    strategy_id: str

    def evaluate(
        self, tenant_id: str, trading_mode: TradingMode
    ) -> list[TradeIntent]: ...


def iter_registered_strategies() -> list[str]:
    eps = importlib.metadata.entry_points(group="oziebot.strategies")
    return [ep.name for ep in eps]


# ============================================================================
# Enhanced Registry for TradingStrategy Framework
# ============================================================================


class StrategyRegistry:
    """
    Central registry for trading strategies.
    
    Supports both:
    1. Entry point based loading (for plugins)
    2. Direct registration (for built-in strategies)
    
    To add a new strategy:
    1. Create a class inheriting from TradingStrategy
    2. Call StrategyRegistry.register(MyStrategy)
    3. Done! No other changes needed - available to users immediately
    """

    _strategies: dict[str, type[TradingStrategy]] = {}

    @classmethod
    def _ensure_builtins_loaded(cls) -> None:
        """Load built-in strategies once, in an idempotent way."""
        if {"momentum", "day_trading", "dca", "reversion"}.issubset(cls._strategies.keys()):
            return

        from oziebot_strategy_engine.strategies.dca import DCAStrategy
        from oziebot_strategy_engine.strategies.day_trading import DayTradingStrategy
        from oziebot_strategy_engine.strategies.momentum import MomentumStrategy
        from oziebot_strategy_engine.strategies.reversion import ReversionStrategy

        for strategy_class in (MomentumStrategy, DayTradingStrategy, DCAStrategy, ReversionStrategy):
            strategy_id = strategy_class().strategy_id
            if strategy_id not in cls._strategies:
                cls._strategies[strategy_id] = strategy_class

    @classmethod
    def register(cls, strategy_class: type[TradingStrategy]) -> None:
        """
        Register a strategy class.
        
        Args:
            strategy_class: Class with strategy_id attribute
            
        Raises:
            ValueError: If duplicate registration
        """
        instance = strategy_class()
        strategy_id = instance.strategy_id

        if strategy_id in cls._strategies:
            raise ValueError(f"Strategy '{strategy_id}' already registered")

        cls._strategies[strategy_id] = strategy_class

    @classmethod
    def get_strategy(cls, strategy_id: str) -> TradingStrategy:
        """Get strategy instance by ID."""
        cls._ensure_builtins_loaded()
        if strategy_id not in cls._strategies:
            available = ", ".join(sorted(cls._strategies.keys()))
            raise KeyError(
                f"Strategy '{strategy_id}' not found. Available: {available}"
            )
        return cls._strategies[strategy_id]()

    @classmethod
    def list_strategies(cls) -> list[dict[str, object]]:
        """List all registered strategies with metadata."""
        cls._ensure_builtins_loaded()
        result = []
        for strategy_class in cls._strategies.values():
            instance = strategy_class()
            config_schema = {}
            if hasattr(instance, "get_config_schema"):
                config_schema = instance.get_config_schema()
            result.append({
                "strategy_id": instance.strategy_id,
                "display_name": getattr(instance, "display_name", instance.strategy_id),
                "description": getattr(instance, "description", ""),
                "version": getattr(instance, "version", "1.0"),
                "config_schema": config_schema,
            })
        return sorted(result, key=lambda x: x["strategy_id"])

    @classmethod
    def strategy_exists(cls, strategy_id: str) -> bool:
        """Check if strategy is registered."""
        cls._ensure_builtins_loaded()
        return strategy_id in cls._strategies
