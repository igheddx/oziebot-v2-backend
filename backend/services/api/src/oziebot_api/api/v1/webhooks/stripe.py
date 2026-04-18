"""Stripe subscription webhooks — signature verification when STRIPE_WEBHOOK_SECRET is set."""

from __future__ import annotations

from fastapi import APIRouter, Depends, Header, HTTPException, Request

from oziebot_api.config import Settings
from oziebot_api.deps import DbSession, settings_dep
from oziebot_api.services.stripe_service import StripeService
from oziebot_api.services.stripe_webhook import process_stripe_event

router = APIRouter(prefix="/webhooks/stripe", tags=["stripe"])


def _stripe_svc(settings: Settings = Depends(settings_dep)) -> StripeService:
    return StripeService(secret_key=settings.stripe_secret_key)


@router.post("")
async def stripe_webhook(
    request: Request,
    db: DbSession,
    settings: Settings = Depends(settings_dep),
    stripe_svc: StripeService = Depends(_stripe_svc),
    stripe_signature: str | None = Header(default=None, alias="stripe-signature"),
) -> dict:
    body = await request.body()
    if not settings.stripe_webhook_secret:
        raise HTTPException(
            status_code=503,
            detail="Stripe webhook secret is not configured",
        )
    if not stripe_svc.enabled:
        raise HTTPException(status_code=503, detail="Stripe is not configured")
    if not stripe_signature:
        raise HTTPException(status_code=400, detail="Missing stripe-signature header")
    try:
        event = stripe_svc.construct_webhook_event(
            body, stripe_signature, settings.stripe_webhook_secret
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid webhook signature: {e!s}") from e

    process_stripe_event(db, event, stripe_svc)
    return {"received": True}
