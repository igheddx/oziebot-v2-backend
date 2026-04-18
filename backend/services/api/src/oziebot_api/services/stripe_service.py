"""Stripe SDK wrapper (testable; no-op when secret unset)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class StripeService:
    secret_key: str | None

    def __post_init__(self) -> None:
        self._enabled = bool(self.secret_key)
        if self._enabled:
            import stripe

            stripe.api_key = self.secret_key
            self._stripe = stripe
        else:
            self._stripe = None

    @property
    def enabled(self) -> bool:
        return self._enabled

    def customers_create(self, **params: Any) -> Any:
        if not self._stripe:
            raise RuntimeError("Stripe is not configured")
        return self._stripe.Customer.create(**params)

    def checkout_sessions_create(self, **params: Any) -> Any:
        if not self._stripe:
            raise RuntimeError("Stripe is not configured")
        return self._stripe.checkout.Session.create(**params)

    def subscriptions_retrieve(self, subscription_id: str, **params: Any) -> Any:
        if not self._stripe:
            raise RuntimeError("Stripe is not configured")
        return self._stripe.Subscription.retrieve(subscription_id, **params)

    def construct_webhook_event(self, payload: bytes, sig_header: str | None, secret: str) -> Any:
        if not self._stripe:
            raise RuntimeError("Stripe is not configured")
        return self._stripe.Webhook.construct_event(payload, sig_header, secret)
