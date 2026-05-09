from __future__ import annotations

from fastapi import APIRouter, Query

from oziebot_api.deps import DbSession
from oziebot_api.deps.auth import RootAdminUser
from oziebot_api.schemas.platform_admin import (
    StrategyLifecycleDiagnosticsResponse,
    StrategyLifecycleTraceListResponse,
)
from oziebot_api.services.admin_strategy_lifecycle import (
    build_strategy_lifecycle_diagnostics,
    list_strategy_lifecycle_traces,
)
from oziebot_api.services.admin_trading_diagnostics import TradingDiagnosticsFilters

router = APIRouter(
    prefix="/admin/strategy-lifecycle-diagnostics", tags=["admin-strategy-lifecycle"]
)


def _filters(
    *,
    days: int,
    token: str | None,
    strategy: str | None,
    trading_mode: str | None,
    limit: int,
) -> TradingDiagnosticsFilters:
    return TradingDiagnosticsFilters(
        days=max(1, min(days, 365)),
        token=token,
        strategy=strategy,
        trading_mode=trading_mode,
        limit=max(1, min(limit, 100)),
    )


@router.get("", response_model=StrategyLifecycleDiagnosticsResponse)
def read_strategy_lifecycle_diagnostics(
    _admin: RootAdminUser,
    db: DbSession,
    days: int = Query(default=7, ge=1, le=365),
    token: str | None = None,
    strategy: str | None = None,
    trading_mode: str | None = Query(default=None, pattern="^(paper|live)$"),
    limit: int = Query(default=25, ge=1, le=100),
) -> dict:
    return build_strategy_lifecycle_diagnostics(
        db,
        filters=_filters(
            days=days,
            token=token,
            strategy=strategy,
            trading_mode=trading_mode,
            limit=limit,
        ),
    )


@router.get("/traces", response_model=StrategyLifecycleTraceListResponse)
def read_strategy_lifecycle_traces(
    _admin: RootAdminUser,
    db: DbSession,
    days: int = Query(default=7, ge=1, le=365),
    token: str | None = None,
    strategy: str | None = None,
    trading_mode: str | None = Query(default=None, pattern="^(paper|live)$"),
    limit: int = Query(default=50, ge=1, le=100),
) -> dict:
    return list_strategy_lifecycle_traces(
        db,
        filters=_filters(
            days=days,
            token=token,
            strategy=strategy,
            trading_mode=trading_mode,
            limit=limit,
        ),
    )
