from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    redis_url: str = "redis://localhost:6379/0"
    database_url: str | None = None
    notify_max_retries: int = Field(default=3, ge=1)
    sms_webhook_url: str | None = None
    slack_webhook_url: str | None = None
    telegram_bot_token: str | None = None


def get_settings() -> Settings:
    return Settings()
