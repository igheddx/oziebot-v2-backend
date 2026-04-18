from __future__ import annotations

from pydantic import BaseModel, Field


class CheckoutRequest(BaseModel):
    plan_slug: str = Field(min_length=1, max_length=64)
    strategy_slugs: list[str] | None = None


class CheckoutResponse(BaseModel):
    checkout_url: str
    stripe_checkout_session_id: str


class BillingSummaryOut(BaseModel):
    trial_started_at: str | None = None
    trial_ends_at: str | None = None
    trial_active: bool = False
    subscription_status: str | None = None
    stripe_subscription_id: str | None = None
    current_period_end: str | None = None
