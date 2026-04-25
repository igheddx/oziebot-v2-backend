from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    redis_url: str = "redis://localhost:6379/0"
    database_url: str = "postgresql+psycopg://oziebot:oziebot@localhost:5432/oziebot"

    risk_max_per_trade_risk_pct: float = 0.12
    risk_max_position_size_cents: int = 0
    risk_max_strategy_allocation_pct: float = 0.8
    risk_max_token_concentration_pct: float = 0.35
    risk_max_daily_loss_cents: int = 3_000
    risk_cooldown_loss_count: int = 3
    risk_cooldown_minutes: int = 45

    risk_stale_trade_seconds: int = 30
    risk_stale_bbo_seconds: int = 30
    risk_stale_candle_seconds: int = 180
    risk_critical_stale_multiplier: int = 3
    risk_stale_degraded_confidence_multiplier: float = 0.75
    risk_max_spread_pct: float = 0.012
    risk_max_slippage_pct: float = 0.02

    # Comma-separated rule names that can be relaxed for PAPER mode.
    risk_relaxed_paper_rules: str = (
        "max_daily_loss,cooldown_after_losses,fee_economics,execution_quality"
    )


def get_settings() -> Settings:
    return Settings()
