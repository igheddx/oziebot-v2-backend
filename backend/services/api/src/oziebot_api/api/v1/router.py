from fastapi import APIRouter

from oziebot_api.api.v1 import (
    admin,
    admin_ai_diagnostics,
    admin_strategy_lifecycle,
    admin_trading_diagnostics,
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
    teacher_assist,
    teacher_assist_pacing_guides,
    teacher_assist_time_savings,
    teacher_assist_instructional_weeks,
    teacher_assist_instructional_loop,
    teacher_assist_copilot,
    teacher_assist_pilot,
    teacher_assist_catalog,
    teacher_assist_education_catalog,
    teacher_assist_v2,
    strategic_aggressive_allocation,
    volatility_harvest,
    tenants,
    tokens,
    strategies,
)
from oziebot_api.api.v1.webhooks import stripe as stripe_wh

api_router = APIRouter(prefix="/v1")
api_router.include_router(health.router)
api_router.include_router(auth.router)
api_router.include_router(me.router)
api_router.include_router(teacher_assist.router)
api_router.include_router(teacher_assist_pacing_guides.router)
api_router.include_router(teacher_assist_time_savings.router)
api_router.include_router(teacher_assist_instructional_weeks.router)
api_router.include_router(teacher_assist_instructional_loop.router)
api_router.include_router(teacher_assist_copilot.router)
api_router.include_router(teacher_assist_pilot.router)
api_router.include_router(teacher_assist_catalog.router)
api_router.include_router(teacher_assist_education_catalog.router)
api_router.include_router(teacher_assist_v2.router)
api_router.include_router(strategic_aggressive_allocation.router)
api_router.include_router(volatility_harvest.router)
api_router.include_router(alerts.router)
api_router.include_router(backtests.router)
api_router.include_router(billing.router)
api_router.include_router(integrations_coinbase.router)
api_router.include_router(logs.router)
api_router.include_router(admin.router)
api_router.include_router(admin_platform.router)
api_router.include_router(admin_ai_diagnostics.router)
api_router.include_router(admin_strategy_lifecycle.router)
api_router.include_router(admin_trading_diagnostics.router)
api_router.include_router(tenants.router)
api_router.include_router(tokens.router)
api_router.include_router(strategies.router)
api_router.include_router(allocations.router)
api_router.include_router(stripe_wh.router)
