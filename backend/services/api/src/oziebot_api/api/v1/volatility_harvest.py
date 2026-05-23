from __future__ import annotations

from fastapi import APIRouter, HTTPException

from oziebot_api.deps import DbSession
from oziebot_api.deps.auth import CurrentUser, RootAdminUser
from oziebot_api.schemas.volatility_harvest import (
    VolatilityHarvestAdminDefaultsPayload,
    VolatilityHarvestConfigPayload,
    VolatilityHarvestCyclePayload,
    VolatilityHarvestTogglePayload,
    VolatilityHarvestTransactionOut,
)
from oziebot_api.services.volatility_harvest import (
    VolatilityHarvestError,
    backtest_preview,
    build_cycle_preview,
    execute_cycle,
    get_accumulation_chart,
    get_admin_defaults,
    get_config,
    get_harvest_activity,
    get_metrics,
    get_overview,
    get_positions,
    get_rebuy_history,
    set_enabled,
    set_admin_defaults,
    upsert_config,
)

router = APIRouter(prefix="/me/strategies/volatility-harvest", tags=["volatility-harvest"])


@router.get("/config")
def get_strategy_config(trading_mode: str, user: CurrentUser, db: DbSession) -> dict:
    return get_config(db, user=user, trading_mode=trading_mode)


@router.post("/config")
@router.put("/config")
def put_strategy_config(
    body: VolatilityHarvestConfigPayload,
    user: CurrentUser,
    db: DbSession,
) -> dict:
    try:
        return upsert_config(db, user=user, payload=body.model_dump())
    except VolatilityHarvestError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/enable")
def enable_strategy(body: VolatilityHarvestTogglePayload, user: CurrentUser, db: DbSession) -> dict:
    return set_enabled(db, user=user, trading_mode=body.trading_mode, enabled=True)


@router.post("/disable")
def disable_strategy(
    body: VolatilityHarvestTogglePayload, user: CurrentUser, db: DbSession
) -> dict:
    return set_enabled(db, user=user, trading_mode=body.trading_mode, enabled=False)


@router.get("/overview")
def get_strategy_overview(trading_mode: str, user: CurrentUser, db: DbSession) -> dict:
    return get_overview(db, user=user, trading_mode=trading_mode)


@router.get("/positions")
def get_strategy_positions(trading_mode: str, user: CurrentUser, db: DbSession) -> dict:
    return get_positions(db, user=user, trading_mode=trading_mode)


@router.get("/harvest-activity")
def get_strategy_harvest_activity(
    trading_mode: str, user: CurrentUser, db: DbSession
) -> list[VolatilityHarvestTransactionOut]:
    rows = get_harvest_activity(db, user=user, trading_mode=trading_mode)
    return [VolatilityHarvestTransactionOut.model_validate(row) for row in rows]


@router.get("/rebuy-history")
def get_strategy_rebuy_history(
    trading_mode: str, user: CurrentUser, db: DbSession
) -> list[VolatilityHarvestTransactionOut]:
    rows = get_rebuy_history(db, user=user, trading_mode=trading_mode)
    return [VolatilityHarvestTransactionOut.model_validate(row) for row in rows]


@router.get("/accumulation-chart")
def get_strategy_accumulation_chart(trading_mode: str, user: CurrentUser, db: DbSession) -> dict:
    return get_accumulation_chart(db, user=user, trading_mode=trading_mode)


@router.get("/metrics")
def get_strategy_metrics(trading_mode: str, user: CurrentUser, db: DbSession) -> dict:
    return get_metrics(db, user=user, trading_mode=trading_mode)


@router.post("/cycle-preview")
def cycle_preview(body: VolatilityHarvestCyclePayload, user: CurrentUser, db: DbSession) -> dict:
    return build_cycle_preview(db, user=user, trading_mode=body.trading_mode)


@router.post("/cycle-execute")
def cycle_execute(body: VolatilityHarvestCyclePayload, user: CurrentUser, db: DbSession) -> dict:
    try:
        return execute_cycle(db, user=user, trading_mode=body.trading_mode)
    except VolatilityHarvestError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/backtest-preview")
def backtest_cycle(body: VolatilityHarvestCyclePayload, user: CurrentUser, db: DbSession) -> dict:
    return backtest_preview(db, user=user, trading_mode=body.trading_mode)


@router.get("/admin-defaults")
def admin_defaults(_user: RootAdminUser, db: DbSession) -> dict:
    return get_admin_defaults(db)


@router.put("/admin-defaults")
def update_admin_defaults(
    body: VolatilityHarvestAdminDefaultsPayload,
    user: RootAdminUser,
    db: DbSession,
) -> dict:
    return set_admin_defaults(db, user=user, payload=body.model_dump())
