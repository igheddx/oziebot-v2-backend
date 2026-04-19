from fastapi import APIRouter

from oziebot_api.api.v1 import (
    admin,
    admin_platform,
    allocations,
    alerts,
    auth,
    backtests,
    billing,
    health,
    integrations_coinbase,
    logs,
    me,
    tenants,
    tokens,
    strategies,
)
from oziebot_api.api.v1.webhooks import stripe as stripe_wh

api_router = APIRouter(prefix="/v1")
api_router.include_router(health.router)
api_router.include_router(auth.router)
api_router.include_router(me.router)
api_router.include_router(alerts.router)
api_router.include_router(backtests.router)
api_router.include_router(billing.router)
api_router.include_router(integrations_coinbase.router)
api_router.include_router(logs.router)
api_router.include_router(admin.router)
api_router.include_router(admin_platform.router)
api_router.include_router(tenants.router)
api_router.include_router(tokens.router)
api_router.include_router(strategies.router)
api_router.include_router(allocations.router)
api_router.include_router(stripe_wh.router)
