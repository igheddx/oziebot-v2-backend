"""Tenant-safe trading query scope: always filter by tenant + user.current_trading_mode."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated

from fastapi import Depends, HTTPException

from oziebot_api.deps.auth import require_user
from oziebot_api.deps import DbSession
from oziebot_api.models.user import User
from oziebot_api.services.entitlements import can_use_trading_for_mode
from oziebot_api.services.platform_management import is_trading_globally_paused
from oziebot_api.services.tenant_scope import primary_tenant_id
from oziebot_domain.trading_mode import TradingMode


@dataclass(frozen=True)
class TradingDataScope:
    """Use for all trading reads/writes: `WHERE tenant_id = scope.tenant_id AND trading_mode = scope.trading_mode`."""

    tenant_id: str
    trading_mode: TradingMode


def get_trading_data_scope(
    db: DbSession,
    user: User = Depends(require_user),
) -> TradingDataScope:
    if is_trading_globally_paused(db):
        raise HTTPException(
            status_code=503,
            detail="Trading is globally paused by platform administrators",
        )
    tid = primary_tenant_id(db, user)
    if tid is None:
        if user.is_root_admin:
            raise HTTPException(
                status_code=400,
                detail="Use a tenant-backed account for trading endpoints",
            )
        raise HTTPException(status_code=400, detail="No tenant membership")
    try:
        mode = TradingMode(user.current_trading_mode)
    except ValueError as e:
        raise HTTPException(status_code=500, detail="Invalid user trading mode") from e
    ok, err = can_use_trading_for_mode(db, tenant_id=tid, trading_mode=mode)
    if not ok:
        raise HTTPException(status_code=403, detail=err or "Trading not permitted")
    return TradingDataScope(tenant_id=str(tid), trading_mode=mode)


TradingScope = Annotated[TradingDataScope, Depends(get_trading_data_scope)]
