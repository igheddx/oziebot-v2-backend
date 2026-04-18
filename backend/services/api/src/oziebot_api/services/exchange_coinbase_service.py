"""Create / validate / health for tenant Coinbase connections (tenant-isolated)."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from oziebot_api.models.exchange_connection import ExchangeConnection
from oziebot_api.models.tenant_integration import TenantIntegration
from oziebot_api.services.coinbase_client import (
    CoinbaseValidationResult,
    validate_coinbase_credentials,
)
from oziebot_api.services.credential_crypto import CredentialCrypto


def mask_api_key_name(name: str) -> str:
    n = name.strip()
    if len(n) <= 8:
        return "***"
    return f"{n[:4]}…{n[-4:]}"


def _get_integration(db: Session, tenant_id: uuid.UUID) -> TenantIntegration:
    row = db.scalars(
        select(TenantIntegration).where(TenantIntegration.tenant_id == tenant_id)
    ).one_or_none()
    if row is None:
        now = datetime.now(UTC)
        row = TenantIntegration(
            tenant_id=tenant_id,
            coinbase_connected=False,
            updated_at=now,
        )
        db.add(row)
        db.flush()
    return row


def _apply_validation(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    row: ExchangeConnection,
    vr: CoinbaseValidationResult,
    now: datetime,
) -> None:
    if vr.ok:
        row.validation_status = "valid"
        row.health_status = "healthy"
        row.can_trade = vr.can_trade
        row.can_read_balances = vr.can_read_balances
        row.last_error = None
    else:
        row.validation_status = "invalid"
        row.health_status = "unhealthy"
        row.can_trade = False
        row.can_read_balances = False
        row.last_error = vr.message
    row.last_validated_at = now
    row.last_health_check_at = now
    row.updated_at = now

    ti = _get_integration(db, tenant_id)
    live_ok = bool(
        vr.ok and vr.can_trade and vr.can_read_balances and row.validation_status == "valid"
    )
    ti.coinbase_connected = live_ok
    ti.coinbase_health_status = row.health_status
    ti.coinbase_last_check_at = now
    ti.coinbase_last_error = row.last_error
    ti.updated_at = now


def get_coinbase_connection(db: Session, tenant_id: uuid.UUID) -> ExchangeConnection | None:
    return db.scalars(
        select(ExchangeConnection).where(
            ExchangeConnection.tenant_id == tenant_id,
            ExchangeConnection.provider == "coinbase",
        )
    ).first()


def connection_public_dict(row: ExchangeConnection) -> dict[str, Any]:
    return {
        "id": str(row.id),
        "provider": row.provider,
        "api_key_name_masked": mask_api_key_name(row.api_key_name),
        "validation_status": row.validation_status,
        "health_status": row.health_status,
        "last_validated_at": row.last_validated_at.isoformat() if row.last_validated_at else None,
        "last_health_check_at": row.last_health_check_at.isoformat()
        if row.last_health_check_at
        else None,
        "last_error": row.last_error,
        "can_trade": row.can_trade,
        "can_read_balances": row.can_read_balances,
        "created_at": row.created_at.isoformat(),
        "updated_at": row.updated_at.isoformat(),
    }


def create_coinbase_connection(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    api_key_name: str,
    api_secret_pem: str,
    crypto: CredentialCrypto,
    coinbase_base_url: str,
    coinbase_force_ipv4: bool = False,
) -> tuple[ExchangeConnection | None, CoinbaseValidationResult]:
    if get_coinbase_connection(db, tenant_id) is not None:
        raise ValueError("connection_exists")
    if not crypto.configured:
        raise ValueError("encryption_not_configured")

    vr = validate_coinbase_credentials(
        api_key_name,
        api_secret_pem,
        base_url=coinbase_base_url,
        force_ipv4=coinbase_force_ipv4,
    )
    if not vr.ok:
        return None, vr

    now = datetime.now(UTC)
    enc = crypto.encrypt(api_secret_pem.encode("utf-8"))
    row = ExchangeConnection(
        id=uuid.uuid4(),
        tenant_id=tenant_id,
        provider="coinbase",
        api_key_name=api_key_name.strip(),
        encrypted_secret=enc,
        secret_ciphertext_version=1,
        validation_status="never_validated",
        created_at=now,
        updated_at=now,
    )
    db.add(row)
    db.flush()
    _apply_validation(db, tenant_id=tenant_id, row=row, vr=vr, now=now)
    db.flush()
    return row, vr


def update_coinbase_secret(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    api_key_name: str | None,
    api_secret_pem: str | None,
    crypto: CredentialCrypto,
    coinbase_base_url: str,
    coinbase_force_ipv4: bool = False,
) -> tuple[ExchangeConnection, CoinbaseValidationResult]:
    if not crypto.configured:
        raise ValueError("encryption_not_configured")
    row = get_coinbase_connection(db, tenant_id)
    if row is None:
        raise ValueError("not_found")

    if api_secret_pem is not None:
        name = (api_key_name or row.api_key_name).strip()
        vr = validate_coinbase_credentials(
            name,
            api_secret_pem,
            base_url=coinbase_base_url,
            force_ipv4=coinbase_force_ipv4,
        )
        if not vr.ok:
            return row, vr
        row.api_key_name = name
        row.encrypted_secret = crypto.encrypt(api_secret_pem.encode("utf-8"))
        row.updated_at = datetime.now(UTC)
    elif api_key_name is not None:
        row.api_key_name = api_key_name.strip()
        pem = crypto.decrypt(row.encrypted_secret).decode("utf-8")
        vr = validate_coinbase_credentials(
            row.api_key_name,
            pem,
            base_url=coinbase_base_url,
            force_ipv4=coinbase_force_ipv4,
        )
    else:
        pem = crypto.decrypt(row.encrypted_secret).decode("utf-8")
        vr = validate_coinbase_credentials(
            row.api_key_name,
            pem,
            base_url=coinbase_base_url,
            force_ipv4=coinbase_force_ipv4,
        )

    now = datetime.now(UTC)
    _apply_validation(db, tenant_id=tenant_id, row=row, vr=vr, now=now)
    db.flush()
    return row, vr


def validate_existing_connection(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    crypto: CredentialCrypto,
    coinbase_base_url: str,
    coinbase_force_ipv4: bool = False,
) -> tuple[ExchangeConnection, CoinbaseValidationResult]:
    if not crypto.configured:
        raise ValueError("encryption_not_configured")
    row = get_coinbase_connection(db, tenant_id)
    if row is None:
        raise ValueError("not_found")
    pem = crypto.decrypt(row.encrypted_secret).decode("utf-8")
    vr = validate_coinbase_credentials(
        row.api_key_name,
        pem,
        base_url=coinbase_base_url,
        force_ipv4=coinbase_force_ipv4,
    )
    now = datetime.now(UTC)
    _apply_validation(db, tenant_id=tenant_id, row=row, vr=vr, now=now)
    db.flush()
    return row, vr


def delete_coinbase_connection(db: Session, *, tenant_id: uuid.UUID) -> bool:
    row = get_coinbase_connection(db, tenant_id)
    if row is None:
        return False
    db.delete(row)
    db.flush()
    ti = db.scalars(
        select(TenantIntegration).where(TenantIntegration.tenant_id == tenant_id)
    ).one_or_none()
    now = datetime.now(UTC)
    if ti is not None:
        ti.coinbase_connected = False
        ti.coinbase_health_status = "unknown"
        ti.coinbase_last_error = None
        ti.coinbase_last_check_at = now
        ti.updated_at = now
    db.flush()
    return True


def run_health_check(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    crypto: CredentialCrypto,
    coinbase_base_url: str,
    coinbase_force_ipv4: bool = False,
) -> tuple[ExchangeConnection, CoinbaseValidationResult]:
    """Alias for validate_existing — suitable for cron / worker."""
    return validate_existing_connection(
        db,
        tenant_id=tenant_id,
        crypto=crypto,
        coinbase_base_url=coinbase_base_url,
        coinbase_force_ipv4=coinbase_force_ipv4,
    )
