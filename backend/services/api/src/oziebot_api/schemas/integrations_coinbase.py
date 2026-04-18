from __future__ import annotations

from pydantic import BaseModel, Field


class CoinbaseConnectionCreate(BaseModel):
    """CDP API key name + ECDSA private key (PEM). Never echoed back."""

    api_key_name: str = Field(min_length=1, max_length=512)
    api_secret_pem: str = Field(
        min_length=1,
        description="PEM-encoded ECDSA private key from Coinbase Developer Platform",
    )


class CoinbaseConnectionPatch(BaseModel):
    """Rotate API key name and/or private key (PEM). Omit fields to leave unchanged."""

    api_key_name: str | None = Field(default=None, max_length=512)
    api_secret_pem: str | None = Field(
        default=None,
        description="New PEM private key — validated before replacing ciphertext",
    )


class CoinbaseConnectionOut(BaseModel):
    id: str
    provider: str
    api_key_name_masked: str
    validation_status: str
    health_status: str | None = None
    last_validated_at: str | None
    last_health_check_at: str | None
    last_error: str | None
    can_trade: bool | None
    can_read_balances: bool | None
    created_at: str
    updated_at: str


class CoinbaseConnectionStatusOut(BaseModel):
    connected: bool
    provider: str = "coinbase"
    api_key_name_masked: str | None = None
    validation_status: str | None = None
    health_status: str | None = None
    last_validated_at: str | None = None
    last_health_check_at: str | None = None
    last_error: str | None = None
    can_trade: bool | None = None
    can_read_balances: bool | None = None
    created_at: str | None = None
    updated_at: str | None = None
