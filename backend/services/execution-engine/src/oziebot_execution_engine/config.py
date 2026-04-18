from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str | None = None
    redis_url: str = "redis://localhost:6379/0"
    coinbase_api_base_url: str = "https://api.coinbase.com"
    exchange_credentials_encryption_key: str | None = None
    paper_default_fee_bps: int = Field(default=15, ge=0)
    paper_default_slippage_bps: int = Field(default=8, ge=0)
    reconciliation_interval_seconds: int = Field(default=60, ge=5)
    reconciliation_health_failure_threshold: int = Field(default=3, ge=1)
    reconciliation_balance_drift_tolerance_cents: int = Field(default=500, ge=0)


def get_settings() -> Settings:
    return Settings()