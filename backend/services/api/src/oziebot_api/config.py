from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    app_env: str = "development"
    database_url: str | None = None
    redis_url: str = "redis://localhost:6379/0"
    api_secret: str = Field(
        default="dev-insecure", description="Legacy internal signing; prefer jwt_secret"
    )
    jwt_secret: str = Field(
        default="change-me-use-openssl-rand-hex-32",
        description="HS256 signing key; set env JWT_SECRET in production",
    )
    jwt_access_exp_minutes: int = 15
    jwt_refresh_exp_days: int = 7
    cors_origins: str = "http://localhost:3000"
    stripe_secret_key: str | None = None
    stripe_webhook_secret: str | None = None
    stripe_checkout_success_url: str = (
        "http://localhost:3000/billing/success?session_id={CHECKOUT_SESSION_ID}"
    )
    stripe_checkout_cancel_url: str = "http://localhost:3000/billing/cancel"
    exchange_credentials_encryption_key: str | None = Field(
        default=None,
        description="Fernet key (urlsafe base64) for exchange API secrets at rest",
    )
    coinbase_api_base_url: str = "https://api.coinbase.com"
    coinbase_force_ipv4: bool = Field(
        default=False,
        description="Force Coinbase API calls to use IPv4 egress only",
    )
    api_slow_request_ms: int = Field(
        default=1000,
        description="Log API requests at warning level when duration exceeds this threshold.",
    )
    api_slow_query_ms: int = Field(
        default=250,
        description="Log SQL queries at warning level when duration exceeds this threshold.",
    )


def get_settings() -> Settings:
    return Settings()
