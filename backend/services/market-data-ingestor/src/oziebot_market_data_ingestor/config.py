from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "postgresql+psycopg://oziebot:oziebot@localhost:5432/oziebot"
    redis_url: str = "redis://localhost:6379/0"

    coinbase_ws_url: str = "wss://ws-feed.exchange.coinbase.com"
    coinbase_rest_url: str = "https://api.exchange.coinbase.com"

    orderbook_depth: int = 10
    stale_trade_seconds: int = 15
    stale_bbo_seconds: int = 10
    stale_candle_seconds: int = 120

    candles_granularity_sec: int = 60  # poll REST every 60s for candles
    cache_ttl_seconds: int = 300
    trade_recovery_limit: int = 20
    token_policy_recalc_interval_seconds: int = 900
    loop_sleep_sec: float = 1.0


def get_settings() -> Settings:
    return Settings()
