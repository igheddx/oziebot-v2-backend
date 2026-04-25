from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "postgresql+psycopg://oziebot:oziebot@localhost:5432/oziebot"
    redis_url: str = "redis://localhost:6379/0"

    coinbase_ws_url: str = "wss://advanced-trade-ws.coinbase.com"
    coinbase_rest_url: str = "https://api.coinbase.com/api/v3/brokerage"

    orderbook_depth: int = 10
    stale_trade_seconds: int = 15
    stale_bbo_seconds: int = 10
    stale_candle_seconds: int = 120

    candles_granularity_sec: int = 60  # poll REST every 60s for candles
    cache_ttl_seconds: int = 120
    candle_history_ttl_seconds: int = 1800
    signal_panel_retention_seconds: int = 60
    signal_panel_sample_interval_seconds: int = 5
    signal_panel_snapshot_event_interval_seconds: int = 15
    redis_pressure_check_interval_seconds: int = 30
    redis_pressure_warning_pct: float = 70.0
    redis_pressure_critical_pct: float = 85.0
    operational_alert_cooldown_seconds: int = 300
    stale_alert_after_seconds: int = 90
    trade_recovery_limit: int = 20
    trade_reconcile_interval_seconds: int = 15
    bbo_reconcile_interval_seconds: int = 10
    universe_refresh_interval_seconds: int = 30
    token_policy_recalc_interval_seconds: int = 900
    loop_sleep_sec: float = 1.0


def get_settings() -> Settings:
    return Settings()
