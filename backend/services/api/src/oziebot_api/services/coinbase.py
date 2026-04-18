"""Coinbase connection readiness for LIVE trading (stored credentials + legacy admin flag)."""

from __future__ import annotations

import uuid

from sqlalchemy import select
from sqlalchemy.orm import Session

from oziebot_api.models.exchange_connection import ExchangeConnection
from oziebot_api.models.tenant_integration import TenantIntegration


def coinbase_valid_for_live_trading(db: Session, tenant_id: uuid.UUID) -> bool:
    """
    True when the tenant has a healthy, validated Coinbase CDP connection with
    trading + balance permissions, OR legacy admin-marked integration (tests / migration).
    PAPER trading does not require any exchange connection.
    """
    ec = db.scalars(
        select(ExchangeConnection).where(
            ExchangeConnection.tenant_id == tenant_id,
            ExchangeConnection.provider == "coinbase",
        )
    ).first()
    if ec is not None:
        return bool(
            ec.validation_status == "valid"
            and ec.health_status == "healthy"
            and ec.can_trade is True
            and ec.can_read_balances is True
        )
    row = db.scalars(
        select(TenantIntegration).where(TenantIntegration.tenant_id == tenant_id)
    ).one_or_none()
    return bool(row and row.coinbase_connected)


def coinbase_connected_for_tenant(db: Session, tenant_id: uuid.UUID) -> bool:
    """Backward-compatible name: LIVE trading requires a valid Coinbase connection."""
    return coinbase_valid_for_live_trading(db, tenant_id)
