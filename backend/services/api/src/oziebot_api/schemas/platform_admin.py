from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


class SettingValueBody(BaseModel):
    value: dict[str, Any]


class GlobalPauseBody(BaseModel):
    paused: bool
    reason: str | None = None


class TokenAllowlistCreate(BaseModel):
    symbol: str = Field(min_length=1, max_length=64)
    quote_currency: str = "USD"
    network: str = "mainnet"
    contract_address: str | None = None
    display_name: str | None = None
    is_enabled: bool = True
    sort_order: int = 0
    extra: dict[str, Any] | None = None


class TokenAllowlistPatch(BaseModel):
    symbol: str | None = None
    quote_currency: str | None = None
    network: str | None = None
    contract_address: str | None = None
    display_name: str | None = None
    is_enabled: bool | None = None
    sort_order: int | None = None
    extra: dict[str, Any] | None = None


class StrategyCatalogCreate(BaseModel):
    slug: str = Field(min_length=1, max_length=64)
    display_name: str = Field(min_length=1, max_length=256)
    description: str | None = None
    is_enabled: bool = True
    entry_point: str | None = None
    config_schema: dict[str, Any] | None = None
    sort_order: int = 0


class StrategyCatalogPatch(BaseModel):
    display_name: str | None = None
    description: str | None = None
    is_enabled: bool | None = None
    entry_point: str | None = None
    config_schema: dict[str, Any] | None = None
    sort_order: int | None = None


class SubscriptionPlanCreate(BaseModel):
    slug: str = Field(min_length=1, max_length=64)
    display_name: str = Field(min_length=1, max_length=256)
    description: str | None = None
    plan_kind: Literal["all_strategies", "per_strategy"] = "all_strategies"
    stripe_price_id: str = Field(min_length=1, max_length=255)
    stripe_product_id: str | None = None
    billing_interval: str = Field(pattern="^(month|year)$")
    amount_cents: int = Field(ge=0)
    currency: str = "usd"
    is_active: bool = True
    features: dict[str, Any] | None = None
    trial_days_override: int | None = Field(default=None, ge=0)
    sort_order: int = 0


class SubscriptionPlanPatch(BaseModel):
    display_name: str | None = None
    description: str | None = None
    plan_kind: Literal["all_strategies", "per_strategy"] | None = None
    stripe_price_id: str | None = None
    stripe_product_id: str | None = None
    billing_interval: str | None = None
    amount_cents: int | None = Field(default=None, ge=0)
    currency: str | None = None
    is_active: bool | None = None
    features: dict[str, Any] | None = None
    trial_days_override: int | None = None
    sort_order: int | None = None


class TrialPolicyBody(BaseModel):
    is_enabled: bool = True
    trial_duration_days: int = Field(ge=0, le=365)
    max_trials_per_tenant: int = Field(ge=0, le=100)
    grace_period_days: int = Field(ge=0, le=90)
    policy_metadata: dict[str, Any] | None = None


class TenantCoinbaseHealthPatch(BaseModel):
    """Simulate or record health probe (admin / cron)."""

    health_status: str | None = Field(
        default=None,
        description="healthy | unhealthy | unknown",
    )
    last_error: str | None = None
    connected: bool | None = None
