"""Enforce trading mode prerequisites (billing + Coinbase for LIVE)."""

from __future__ import annotations

import uuid

from sqlalchemy.orm import Session

from oziebot_api.services.entitlements import can_use_trading_for_mode
from oziebot_domain.trading_mode import TradingMode


def can_set_trading_mode(
    db: Session,
    *,
    tenant_id: uuid.UUID | None,
    new_mode: TradingMode,
) -> tuple[bool, str | None]:
    if tenant_id is None:
        return False, "Tenant context required for trading mode changes"
    return can_use_trading_for_mode(db, tenant_id=tenant_id, trading_mode=new_mode)
