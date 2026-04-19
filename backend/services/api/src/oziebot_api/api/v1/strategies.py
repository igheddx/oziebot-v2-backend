"""Strategy management API - configure and monitor user strategies."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from fastapi import APIRouter, HTTPException
from sqlalchemy import select

from oziebot_api.deps import DbSession
from oziebot_api.deps.auth import CurrentUser
from oziebot_api.models.platform_strategy import PlatformStrategy
from oziebot_api.models.strategy_signal_pipeline import StrategySignalRecord
from oziebot_api.models.user_strategy import (
    StrategyPerformance,
    UserStrategy,
    UserStrategyState,
)
from oziebot_api.schemas.strategies import (
    AvailableStrategiesResponse,
    StrategyMetadata,
    StrategyPerformanceListResponse,
    StrategyPerformanceResponse,
    StrategySignalResponse,
    UserStrategyCreate,
    UserStrategyResponse,
    UserStrategyStateResponse,
    UserStrategyStateUpsert,
    UserStrategyUpdate,
    UserStrategiesListResponse,
)
from oziebot_api.services.entitlements import has_strategy_entitlement
from oziebot_api.services.strategy_catalog import ensure_platform_strategy_catalog
from oziebot_api.services.tenant_scope import primary_tenant_id

router = APIRouter(prefix="/me/strategies", tags=["strategies"])


# ============================================================================
# Available Strategies - Read-Only
# ============================================================================


@router.get("/available")
def list_available_strategies(db: DbSession) -> AvailableStrategiesResponse:
    """
    List all available strategies on the platform.

    Returns metadata about each strategy including configuration schema
    for frontend to build dynamic UI.
    """
    # Import registry here to avoid circular imports
    from oziebot_strategy_engine.registry import StrategyRegistry

    strategies_list = StrategyRegistry.list_strategies()

    return AvailableStrategiesResponse(
        total=len(strategies_list),
        strategies=[StrategyMetadata(**s) for s in strategies_list],
    )


@router.get("/catalog")
def list_effective_strategy_catalog(user: CurrentUser, db: DbSession) -> dict[str, Any]:
    ensure_platform_strategy_catalog(db)
    rows = db.scalars(
        select(PlatformStrategy).order_by(PlatformStrategy.sort_order, PlatformStrategy.slug)
    ).all()
    tenant_id = primary_tenant_id(db, user)
    uses_tenant_scope = tenant_id is not None
    configured = {
        row.strategy_id: row
        for row in db.query(UserStrategy)
        .filter(UserStrategy.user_id == user.id)
        .order_by(UserStrategy.strategy_id)
        .all()
    }
    strategies: list[dict[str, Any]] = []
    for row in rows:
        assigned = (
            bool(tenant_id and has_strategy_entitlement(db, tenant_id, row.slug))
            if uses_tenant_scope
            else user.is_root_admin
        )
        if not assigned:
            continue
        configured_row = configured.get(row.slug)
        strategies.append(
            {
                "strategy_id": row.slug,
                "display_name": row.display_name,
                "description": row.description,
                "is_platform_enabled": row.is_enabled,
                "is_assigned": assigned,
                "is_user_enabled": (
                    configured_row.is_enabled
                    if configured_row is not None
                    else bool(user.is_root_admin and not uses_tenant_scope)
                ),
                "configured": configured_row is not None,
                "config_schema": row.config_schema,
                "sort_order": row.sort_order,
            }
        )
    return {"total": len(strategies), "strategies": strategies}


# ============================================================================
# User Strategy Management
# ============================================================================


@router.get("")
def list_my_strategies(user: CurrentUser, db: DbSession) -> UserStrategiesListResponse:
    """List all strategies configured by user."""
    strategies = (
        db.query(UserStrategy)
        .filter(UserStrategy.user_id == user.id)
        .order_by(UserStrategy.strategy_id)
        .all()
    )

    enabled_count = sum(1 for s in strategies if s.is_enabled)

    return UserStrategiesListResponse(
        total=len(strategies),
        enabled_count=enabled_count,
        strategies=[UserStrategyResponse.model_validate(s) for s in strategies],
    )


@router.post("")
def create_strategy(
    body: UserStrategyCreate,
    user: CurrentUser,
    db: DbSession,
) -> UserStrategyResponse:
    """
    Add a new strategy for the user.

    Configuration will be validated against strategy schema.
    """
    # Import registry here
    from oziebot_strategy_engine.registry import StrategyRegistry

    # Verify strategy exists
    if not StrategyRegistry.strategy_exists(body.strategy_id):
        raise HTTPException(status_code=404, detail=f"Strategy '{body.strategy_id}' not found")

    ensure_platform_strategy_catalog(db)

    tenant_id = primary_tenant_id(db, user)
    if tenant_id is None:
        if user.is_root_admin:
            raise HTTPException(
                status_code=400,
                detail="Root admin accounts need a tenant membership to configure strategies",
            )
        raise HTTPException(status_code=400, detail="No tenant membership")
    if not has_strategy_entitlement(db, tenant_id, body.strategy_id):
        raise HTTPException(status_code=403, detail="Strategy is not assigned to this tenant")

    # Verify not already configured
    existing = (
        db.query(UserStrategy)
        .filter(
            UserStrategy.user_id == user.id,
            UserStrategy.strategy_id == body.strategy_id,
        )
        .first()
    )

    if existing:
        raise HTTPException(
            status_code=409, detail=f"Strategy '{body.strategy_id}' already configured for user"
        )

    # Validate config if provided
    if body.config:
        try:
            strategy = StrategyRegistry.get_strategy(body.strategy_id)
            strategy.validate_config(body.config)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))

    # Create strategy record
    now = datetime.now(UTC)
    db_strategy = UserStrategy(
        user_id=user.id,
        strategy_id=body.strategy_id,
        is_enabled=body.is_enabled,
        config=body.config or {},
        created_at=now,
        updated_at=now,
    )
    db.add(db_strategy)
    db.commit()
    db.refresh(db_strategy)

    return UserStrategyResponse.model_validate(db_strategy)


@router.get("/{strategy_id}")
def get_strategy(
    strategy_id: str,
    user: CurrentUser,
    db: DbSession,
) -> UserStrategyResponse:
    """Get a user's specific strategy configuration."""
    strategy = (
        db.query(UserStrategy)
        .filter(
            UserStrategy.user_id == user.id,
            UserStrategy.strategy_id == strategy_id,
        )
        .first()
    )

    if not strategy:
        raise HTTPException(status_code=404, detail="Strategy not configured")

    return UserStrategyResponse.model_validate(strategy)


@router.patch("/{strategy_id}")
def update_strategy(
    strategy_id: str,
    body: UserStrategyUpdate,
    user: CurrentUser,
    db: DbSession,
) -> UserStrategyResponse:
    """Update strategy configuration."""
    strategy = (
        db.query(UserStrategy)
        .filter(
            UserStrategy.user_id == user.id,
            UserStrategy.strategy_id == strategy_id,
        )
        .first()
    )

    ensure_platform_strategy_catalog(db)

    if not strategy:
        tenant_id = primary_tenant_id(db, user)
        if tenant_id is not None:
            if not has_strategy_entitlement(db, tenant_id, strategy_id):
                raise HTTPException(
                    status_code=403, detail="Strategy is not assigned to this tenant"
                )
        elif user.is_root_admin:
            # Root admin: just verify the strategy exists in the platform catalog
            platform_strat = db.scalars(
                select(PlatformStrategy).where(PlatformStrategy.slug == strategy_id)
            ).first()
            if not platform_strat:
                raise HTTPException(status_code=404, detail=f"Strategy '{strategy_id}' not found")
        else:
            raise HTTPException(status_code=400, detail="No tenant membership")
        now = datetime.now(UTC)
        strategy = UserStrategy(
            user_id=user.id,
            strategy_id=strategy_id,
            is_enabled=body.is_enabled if body.is_enabled is not None else False,
            config=body.config or {},
            created_at=now,
            updated_at=now,
        )

    # Validate new config if provided
    if body.config is not None:
        from oziebot_strategy_engine.registry import StrategyRegistry

        try:
            strategy_impl = StrategyRegistry.get_strategy(strategy_id)
            strategy_impl.validate_config(body.config)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))

    # Update fields
    if body.is_enabled is not None:
        strategy.is_enabled = body.is_enabled
    if body.config is not None:
        strategy.config = body.config

    strategy.updated_at = datetime.now(UTC)
    db.add(strategy)
    db.commit()
    db.refresh(strategy)

    return UserStrategyResponse.model_validate(strategy)


@router.delete("/{strategy_id}")
def delete_strategy(
    strategy_id: str,
    user: CurrentUser,
    db: DbSession,
) -> dict[str, str]:
    """Remove a strategy from user's configuration."""
    strategy = (
        db.query(UserStrategy)
        .filter(
            UserStrategy.user_id == user.id,
            UserStrategy.strategy_id == strategy_id,
        )
        .first()
    )

    if not strategy:
        raise HTTPException(status_code=404, detail="Strategy not configured")

    db.delete(strategy)
    db.commit()

    return {"status": "deleted", "strategy_id": strategy_id}


# ============================================================================
# Strategy Performance Tracking
# ============================================================================


@router.get("/{strategy_id}/performance")
def get_strategy_performance(
    strategy_id: str,
    user: CurrentUser,
    db: DbSession,
    trading_mode: str | None = None,
) -> StrategyPerformanceListResponse:
    """Get performance metrics for a strategy."""
    query = db.query(StrategyPerformance).filter(
        StrategyPerformance.user_id == user.id,
        StrategyPerformance.strategy_id == strategy_id,
    )

    if trading_mode:
        query = query.filter(StrategyPerformance.trading_mode == trading_mode)

    performance_records = query.all()

    return StrategyPerformanceListResponse(
        strategies=[StrategyPerformanceResponse.model_validate(p) for p in performance_records]
    )


@router.get("/{strategy_id}/signals")
def get_strategy_signals(
    strategy_id: str,
    user: CurrentUser,
    db: DbSession,
    limit: int = 100,
    offset: int = 0,
) -> dict[str, Any]:
    """Get recent signals from a strategy."""
    signals = (
        db.query(StrategySignalRecord)
        .filter(
            StrategySignalRecord.user_id == user.id,
            StrategySignalRecord.strategy_name == strategy_id,
        )
        .order_by(StrategySignalRecord.timestamp.desc())
        .offset(offset)
        .limit(limit)
        .all()
    )

    return {
        "strategy_id": strategy_id,
        "total_fetched": len(signals),
        "limit": limit,
        "offset": offset,
        "signals": [
            StrategySignalResponse(
                id=signal.signal_id,
                strategy_id=signal.strategy_name,
                signal_type=signal.action,
                trading_mode=signal.trading_mode,
                symbol=signal.symbol,
                confidence=signal.confidence,
                reason=str(signal.reasoning_metadata.get("reason") or ""),
                created_at=signal.timestamp,
            )
            for signal in signals
        ],
    }


@router.get("/{strategy_id}/state")
def get_strategy_state(
    strategy_id: str,
    user: CurrentUser,
    db: DbSession,
    trading_mode: str,
) -> UserStrategyStateResponse:
    """Get persisted runtime state for a strategy in PAPER or LIVE mode."""
    if trading_mode not in {"paper", "live"}:
        raise HTTPException(status_code=400, detail="trading_mode must be 'paper' or 'live'")

    row = (
        db.query(UserStrategyState)
        .filter(
            UserStrategyState.user_id == user.id,
            UserStrategyState.strategy_id == strategy_id,
            UserStrategyState.trading_mode == trading_mode,
        )
        .first()
    )
    if row is None:
        raise HTTPException(status_code=404, detail="Strategy state not found")

    return UserStrategyStateResponse(
        strategy_id=row.strategy_id,
        trading_mode=row.trading_mode,
        state=row.state,
        updated_at=row.updated_at,
    )


@router.put("/{strategy_id}/state")
def upsert_strategy_state(
    strategy_id: str,
    body: UserStrategyStateUpsert,
    user: CurrentUser,
    db: DbSession,
) -> UserStrategyStateResponse:
    """Create or update runtime state for a strategy in PAPER or LIVE mode."""
    configured = (
        db.query(UserStrategy)
        .filter(UserStrategy.user_id == user.id, UserStrategy.strategy_id == strategy_id)
        .first()
    )
    if configured is None:
        raise HTTPException(status_code=404, detail="Strategy not configured")

    row = (
        db.query(UserStrategyState)
        .filter(
            UserStrategyState.user_id == user.id,
            UserStrategyState.strategy_id == strategy_id,
            UserStrategyState.trading_mode == body.trading_mode,
        )
        .first()
    )

    now = datetime.now(UTC)
    if row is None:
        row = UserStrategyState(
            user_id=user.id,
            strategy_id=strategy_id,
            trading_mode=body.trading_mode,
            state=body.state,
            created_at=now,
            updated_at=now,
        )
    else:
        row.state = body.state
        row.updated_at = now

    db.add(row)
    db.commit()
    db.refresh(row)

    return UserStrategyStateResponse(
        strategy_id=row.strategy_id,
        trading_mode=row.trading_mode,
        state=row.state,
        updated_at=row.updated_at,
    )
