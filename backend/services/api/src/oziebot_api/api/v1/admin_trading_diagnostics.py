from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import JSONResponse, StreamingResponse

from oziebot_api.config import Settings
from oziebot_api.deps import DbSession, settings_dep
from oziebot_api.deps.auth import RootAdminUser
from oziebot_api.schemas.platform_admin import TradingDiagnosticsResponse
from oziebot_api.services.admin_trading_diagnostics import (
    TradingDiagnosticsFilters,
    build_trading_diagnostics_report,
    render_trading_diagnostics_csv,
)

router = APIRouter(prefix="/admin/trading-diagnostics", tags=["admin-trading-diagnostics"])


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


@router.get("", response_model=TradingDiagnosticsResponse)
def read_trading_diagnostics(
    _admin: RootAdminUser,
    db: DbSession,
    app_settings: Settings = Depends(settings_dep),
    days: int = Query(default=7, ge=1, le=365),
    token: str | None = None,
    strategy: str | None = None,
    trading_mode: str | None = Query(default=None, pattern="^(paper|live)$"),
    limit: int = Query(default=100, ge=1, le=100),
) -> dict:
    return build_trading_diagnostics_report(
        db,
        app_settings,
        filters=_filters(
            days=days,
            token=token,
            strategy=strategy,
            trading_mode=trading_mode,
            limit=limit,
        ),
    )


@router.get("/export")
def export_trading_diagnostics(
    _admin: RootAdminUser,
    db: DbSession,
    app_settings: Settings = Depends(settings_dep),
    format: str = Query(default="json"),
    days: int = Query(default=7, ge=1, le=365),
    token: str | None = None,
    strategy: str | None = None,
    trading_mode: str | None = Query(default=None, pattern="^(paper|live)$"),
    limit: int = Query(default=100, ge=1, le=100),
):
    fmt = format.strip().lower()
    if fmt not in {"json", "csv"}:
        raise HTTPException(status_code=400, detail="format must be json or csv")

    report = build_trading_diagnostics_report(
        db,
        app_settings,
        filters=_filters(
            days=days,
            token=token,
            strategy=strategy,
            trading_mode=trading_mode,
            limit=limit,
        ),
    )
    if fmt == "json":
        return JSONResponse(report)

    filename = f'trading-diagnostics-{report["generated_at"].replace(":", "-")}.csv'
    return StreamingResponse(
        iter([render_trading_diagnostics_csv(report)]),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
