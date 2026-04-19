from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

from oziebot_api.config import Settings
from oziebot_api.deps import DbSession, settings_dep
from oziebot_api.deps.auth import CurrentUser
from oziebot_api.schemas.allocations import (
    GuidedAllocationUpsert,
    LockCapitalRequest,
    ManualAllocationUpsert,
    ReleaseCapitalRequest,
    ReserveCapitalRequest,
    SettleCapitalRequest,
    StrategyBucketResponse,
    StrategyBucketsResponse,
    UnrealizedPnlRequest,
)
from oziebot_api.services.strategy_allocation import (
    AllocationInput,
    InsufficientBuyingPowerError,
    StrategyAllocationError,
    StrategyAllocationService,
)
from oziebot_api.services.live_coinbase import load_live_coinbase_accounts, sum_coinbase_cash_cents

router = APIRouter(prefix="/me/allocations", tags=["allocations"])


def _bucket_out(bucket) -> StrategyBucketResponse:
    return StrategyBucketResponse(
        strategy_id=bucket.strategy_id,
        trading_mode=bucket.trading_mode,
        assigned_capital_cents=bucket.assigned_capital_cents,
        available_cash_cents=bucket.available_cash_cents,
        reserved_cash_cents=bucket.reserved_cash_cents,
        locked_capital_cents=bucket.locked_capital_cents,
        realized_pnl_cents=bucket.realized_pnl_cents,
        unrealized_pnl_cents=bucket.unrealized_pnl_cents,
        available_buying_power_cents=bucket.available_buying_power_cents,
        version=bucket.version,
        updated_at=bucket.updated_at,
    )


def _plan_out(plan) -> dict:
    return {
        "trading_mode": plan.trading_mode,
        "allocation_mode": plan.allocation_mode,
        "preset_name": plan.preset_name,
        "total_capital_cents": plan.total_capital_cents,
        "items": [
            {
                "strategy_id": i.strategy_id,
                "allocation_bps": i.allocation_bps,
                "assigned_capital_cents": i.assigned_capital_cents,
            }
            for i in sorted(plan.items, key=lambda z: z.strategy_id)
        ],
    }


def _live_total_capital_cents(
    db: DbSession,
    *,
    user: CurrentUser,
    settings: Settings,
) -> int | None:
    accounts = load_live_coinbase_accounts(db, user=user, settings=settings)
    if accounts is None:
        return None
    return sum_coinbase_cash_cents(accounts, include_hold=False)


def _sync_live_plan_if_available(
    db: DbSession,
    *,
    user: CurrentUser,
    settings: Settings,
):
    live_total_capital_cents = _live_total_capital_cents(db, user=user, settings=settings)
    if live_total_capital_cents is None:
        return None

    try:
        allocations = StrategyAllocationService.derive_live_allocations(db, user_id=user.id)
    except StrategyAllocationError:
        return None

    current = StrategyAllocationService.get_plan(db, user_id=user.id, trading_mode="live")
    current_bps = (
        {item.strategy_id: item.allocation_bps for item in current.items}
        if current is not None
        else {}
    )
    target_bps = {alloc.strategy_id: alloc.allocation_bps for alloc in allocations}
    needs_sync = (
        current is None
        or current.total_capital_cents != live_total_capital_cents
        or current_bps != target_bps
    )
    if not needs_sync:
        return current

    plan = StrategyAllocationService.apply_allocations(
        db,
        user_id=user.id,
        trading_mode="live",
        total_capital_cents=live_total_capital_cents,
        allocation_mode=(
            current.allocation_mode
            if current is not None and current.allocation_mode in {"manual", "guided"}
            else "manual"
        ),
        allocations=allocations,
        preset_name=current.preset_name if current is not None else None,
    )
    db.commit()
    db.refresh(plan)
    return plan


@router.get("/presets")
def get_presets() -> dict[str, dict[str, int]]:
    return {
        "conservative": {"dca": 6000, "momentum": 2500, "day_trading": 1500},
        "balanced": {"dca": 4000, "momentum": 3500, "day_trading": 2500},
        "aggressive": {"dca": 2500, "momentum": 3000, "day_trading": 4500},
    }


@router.put("/{trading_mode}/manual")
def upsert_manual_allocations(
    trading_mode: str,
    body: ManualAllocationUpsert,
    user: CurrentUser,
    db: DbSession,
    settings: Settings = Depends(settings_dep),
) -> dict:
    try:
        total_capital_cents = body.total_capital_cents
        if trading_mode == "live":
            live_total_capital_cents = _live_total_capital_cents(db, user=user, settings=settings)
            if live_total_capital_cents is not None:
                total_capital_cents = live_total_capital_cents
        plan = StrategyAllocationService.apply_allocations(
            db,
            user_id=user.id,
            trading_mode=trading_mode,
            total_capital_cents=total_capital_cents,
            allocation_mode="manual",
            allocations=[
                AllocationInput(strategy_id=x.strategy_id, allocation_bps=x.allocation_bps)
                for x in body.allocations
            ],
            preset_name=None,
        )
        db.commit()
        db.refresh(plan)
    except StrategyAllocationError as e:
        raise HTTPException(status_code=400, detail=str(e))

    return _plan_out(plan)


@router.put("/{trading_mode}/guided")
def upsert_guided_allocations(
    trading_mode: str,
    body: GuidedAllocationUpsert,
    user: CurrentUser,
    db: DbSession,
    settings: Settings = Depends(settings_dep),
) -> dict:
    enabled = (
        [
            s.strategy_id
            for s in user.strategies  # type: ignore[attr-defined]
            if s.is_enabled
        ]
        if hasattr(user, "strategies")
        else []
    )

    if not enabled:
        from oziebot_api.models.user_strategy import UserStrategy

        enabled = [
            row.strategy_id
            for row in db.query(UserStrategy)
            .filter(UserStrategy.user_id == user.id, UserStrategy.is_enabled == True)  # noqa: E712
            .all()
        ]

    try:
        allocations = StrategyAllocationService.guided_preset_allocations(
            body.preset_name,
            enabled,
        )
        total_capital_cents = body.total_capital_cents
        if trading_mode == "live":
            live_total_capital_cents = _live_total_capital_cents(db, user=user, settings=settings)
            if live_total_capital_cents is not None:
                total_capital_cents = live_total_capital_cents
        plan = StrategyAllocationService.apply_allocations(
            db,
            user_id=user.id,
            trading_mode=trading_mode,
            total_capital_cents=total_capital_cents,
            allocation_mode="guided",
            allocations=allocations,
            preset_name=body.preset_name,
        )
        db.commit()
        db.refresh(plan)
    except StrategyAllocationError as e:
        raise HTTPException(status_code=400, detail=str(e))

    return _plan_out(plan)


@router.get("/{trading_mode}")
def get_allocation_plan(
    trading_mode: str,
    user: CurrentUser,
    db: DbSession,
    settings: Settings = Depends(settings_dep),
) -> dict:
    try:
        if trading_mode == "live":
            synced_plan = _sync_live_plan_if_available(db, user=user, settings=settings)
            if synced_plan is not None:
                return _plan_out(synced_plan)
        plan = StrategyAllocationService.get_plan(
            db,
            user_id=user.id,
            trading_mode=trading_mode,
        )
    except StrategyAllocationError as e:
        raise HTTPException(status_code=400, detail=str(e))

    if plan is None:
        raise HTTPException(status_code=404, detail="Allocation plan not found")

    return _plan_out(plan)


@router.get("/{trading_mode}/buckets", response_model=StrategyBucketsResponse)
def list_buckets(
    trading_mode: str,
    user: CurrentUser,
    db: DbSession,
    settings: Settings = Depends(settings_dep),
) -> StrategyBucketsResponse:
    try:
        if trading_mode == "live":
            _sync_live_plan_if_available(db, user=user, settings=settings)
        rows = StrategyAllocationService.list_buckets(
            db, user_id=user.id, trading_mode=trading_mode
        )
    except StrategyAllocationError as e:
        raise HTTPException(status_code=400, detail=str(e))

    return StrategyBucketsResponse(
        trading_mode=trading_mode,
        buckets=[_bucket_out(b) for b in rows],
    )


@router.post("/{trading_mode}/reserve", response_model=StrategyBucketResponse)
def reserve_capital(
    trading_mode: str,
    body: ReserveCapitalRequest,
    user: CurrentUser,
    db: DbSession,
) -> StrategyBucketResponse:
    try:
        bucket = StrategyAllocationService.reserve_capital(
            db,
            user_id=user.id,
            strategy_id=body.strategy_id,
            trading_mode=trading_mode,
            amount_cents=body.amount_cents,
            reference_id=body.reference_id,
        )
        db.commit()
        db.refresh(bucket)
    except InsufficientBuyingPowerError as e:
        raise HTTPException(status_code=409, detail=str(e))
    except StrategyAllocationError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return _bucket_out(bucket)


@router.post("/{trading_mode}/release", response_model=StrategyBucketResponse)
def release_capital(
    trading_mode: str,
    body: ReleaseCapitalRequest,
    user: CurrentUser,
    db: DbSession,
) -> StrategyBucketResponse:
    try:
        bucket = StrategyAllocationService.release_reserved_capital(
            db,
            user_id=user.id,
            strategy_id=body.strategy_id,
            trading_mode=trading_mode,
            amount_cents=body.amount_cents,
            reference_id=body.reference_id,
        )
        db.commit()
        db.refresh(bucket)
    except StrategyAllocationError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return _bucket_out(bucket)


@router.post("/{trading_mode}/lock", response_model=StrategyBucketResponse)
def lock_capital(
    trading_mode: str,
    body: LockCapitalRequest,
    user: CurrentUser,
    db: DbSession,
) -> StrategyBucketResponse:
    try:
        bucket = StrategyAllocationService.lock_reserved_capital(
            db,
            user_id=user.id,
            strategy_id=body.strategy_id,
            trading_mode=trading_mode,
            amount_cents=body.amount_cents,
            reference_id=body.reference_id,
        )
        db.commit()
        db.refresh(bucket)
    except StrategyAllocationError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return _bucket_out(bucket)


@router.post("/{trading_mode}/settle", response_model=StrategyBucketResponse)
def settle_capital(
    trading_mode: str,
    body: SettleCapitalRequest,
    user: CurrentUser,
    db: DbSession,
) -> StrategyBucketResponse:
    try:
        bucket = StrategyAllocationService.settle_position(
            db,
            user_id=user.id,
            strategy_id=body.strategy_id,
            trading_mode=trading_mode,
            released_locked_cents=body.released_locked_cents,
            realized_pnl_delta_cents=body.realized_pnl_delta_cents,
            reference_id=body.reference_id,
        )
        db.commit()
        db.refresh(bucket)
    except StrategyAllocationError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return _bucket_out(bucket)


@router.post("/{trading_mode}/mark-unrealized", response_model=StrategyBucketResponse)
def mark_unrealized_pnl(
    trading_mode: str,
    body: UnrealizedPnlRequest,
    user: CurrentUser,
    db: DbSession,
) -> StrategyBucketResponse:
    try:
        bucket = StrategyAllocationService.mark_unrealized_pnl(
            db,
            user_id=user.id,
            strategy_id=body.strategy_id,
            trading_mode=trading_mode,
            unrealized_pnl_cents=body.unrealized_pnl_cents,
            reference_id=body.reference_id,
        )
        db.commit()
        db.refresh(bucket)
    except StrategyAllocationError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return _bucket_out(bucket)
