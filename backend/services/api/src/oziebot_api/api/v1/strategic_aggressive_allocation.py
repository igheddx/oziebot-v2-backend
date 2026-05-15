from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException

from oziebot_api.config import Settings
from oziebot_api.deps import DbSession, settings_dep
from oziebot_api.deps.auth import CurrentUser
from oziebot_api.schemas.strategic_aggressive_allocation import (
    StrategicAggressiveAllocationConfigPayload,
    StrategicAggressiveAllocationEnablePayload,
    StrategicAggressiveAllocationProfitEventOut,
    StrategicAggressiveAllocationRebalancePayload,
)
from oziebot_api.services.strategic_aggressive_allocation import (
    StrategicAggressiveAllocationError,
    build_rebalance_preview,
    execute_rebalance,
    get_config,
    get_performance,
    get_profit_history,
    list_positions,
    set_enabled,
    upsert_config,
)

router = APIRouter(
    prefix="/me/strategies/strategic-aggressive-allocation",
    tags=["strategic-aggressive-allocation"],
)


@router.get("/config")
def get_strategy_config(
    trading_mode: str,
    user: CurrentUser,
    db: DbSession,
) -> dict:
    return get_config(db, user=user, trading_mode=trading_mode)


@router.post("/config")
@router.put("/config")
def put_strategy_config(
    body: StrategicAggressiveAllocationConfigPayload,
    user: CurrentUser,
    db: DbSession,
    settings: Annotated[Settings, Depends(settings_dep)],
) -> dict:
    try:
        return upsert_config(db, user=user, settings=settings, payload=body.model_dump())
    except StrategicAggressiveAllocationError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/enable")
def enable_strategy(
    body: StrategicAggressiveAllocationEnablePayload,
    user: CurrentUser,
    db: DbSession,
) -> dict:
    return set_enabled(db, user=user, trading_mode=body.trading_mode, enabled=True)


@router.post("/disable")
def disable_strategy(
    body: StrategicAggressiveAllocationEnablePayload,
    user: CurrentUser,
    db: DbSession,
) -> dict:
    return set_enabled(db, user=user, trading_mode=body.trading_mode, enabled=False)


@router.get("/positions")
def get_strategy_positions(
    trading_mode: str,
    user: CurrentUser,
    db: DbSession,
) -> dict:
    return list_positions(db, user=user, trading_mode=trading_mode)


@router.get("/performance")
def get_strategy_performance(
    trading_mode: str,
    user: CurrentUser,
    db: DbSession,
) -> dict:
    return get_performance(db, user=user, trading_mode=trading_mode)


@router.get("/profit-taking-history")
def get_strategy_profit_history(
    trading_mode: str,
    user: CurrentUser,
    db: DbSession,
) -> list[StrategicAggressiveAllocationProfitEventOut]:
    rows = get_profit_history(db, user=user, trading_mode=trading_mode)
    return [
        StrategicAggressiveAllocationProfitEventOut(
            id=row.id,
            symbol=row.symbol,
            bucket_id=row.bucket_id,
            event_type=row.event_type,
            status=row.status,
            quantity=row.quantity,
            trigger_price=row.trigger_price,
            realized_pnl_cents=row.realized_pnl_cents,
            signal_id=row.signal_id,
            correlation_id=row.correlation_id,
            metadata=row.metadata_json,
            occurred_at=row.occurred_at,
        )
        for row in rows
    ]


@router.post("/rebalance-preview")
def rebalance_preview(
    body: StrategicAggressiveAllocationRebalancePayload,
    user: CurrentUser,
    db: DbSession,
) -> dict:
    return build_rebalance_preview(
        db,
        user=user,
        trading_mode=body.trading_mode,
        aggressive_rebalance=body.aggressive_rebalance,
    )


@router.post("/rebalance-execute")
def rebalance_execute(
    body: StrategicAggressiveAllocationRebalancePayload,
    user: CurrentUser,
    db: DbSession,
) -> dict:
    try:
        return execute_rebalance(
            db,
            user=user,
            trading_mode=body.trading_mode,
            aggressive_rebalance=body.aggressive_rebalance,
        )
    except StrategicAggressiveAllocationError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/backtest-preview")
def backtest_preview(
    body: StrategicAggressiveAllocationRebalancePayload,
    user: CurrentUser,
    db: DbSession,
) -> dict:
    preview = build_rebalance_preview(
        db,
        user=user,
        trading_mode=body.trading_mode,
        aggressive_rebalance=body.aggressive_rebalance,
    )
    return {
        "strategy_id": preview["strategy_id"],
        "trading_mode": preview["trading_mode"],
        "selected_symbols": sorted((preview["plan"].get("symbol_contexts") or {}).keys()),
        "bucket_summary": preview["plan"].get("bucket_plans") or {},
        "note": "Preview only. No trades were executed.",
    }
