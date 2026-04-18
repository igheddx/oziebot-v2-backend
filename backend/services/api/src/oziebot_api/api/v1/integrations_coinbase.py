"""Tenant-scoped Coinbase CDP credentials (encrypted); no secret material in responses."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

from oziebot_api.config import Settings
from oziebot_api.deps import DbSession, settings_dep
from oziebot_api.deps.auth import CurrentUser
from oziebot_api.schemas.integrations_coinbase import (
    CoinbaseConnectionCreate,
    CoinbaseConnectionOut,
    CoinbaseConnectionPatch,
    CoinbaseConnectionStatusOut,
)
from oziebot_api.services.coinbase_client import CoinbaseValidationResult
from oziebot_api.services.credential_crypto import CredentialCrypto
from oziebot_api.services.exchange_coinbase_service import (
    connection_public_dict,
    create_coinbase_connection,
    delete_coinbase_connection,
    get_coinbase_connection,
    update_coinbase_secret,
    validate_existing_connection,
)
from oziebot_api.services.tenant_scope import primary_tenant_id

router = APIRouter(prefix="/integrations/coinbase", tags=["integrations-coinbase"])


def _crypto_dep(settings: Settings = Depends(settings_dep)) -> CredentialCrypto:
    return CredentialCrypto(settings.exchange_credentials_encryption_key)


def _validation_http_error(vr: CoinbaseValidationResult) -> HTTPException:
    code = vr.error_code or "validation_failed"
    if vr.http_status == 403 or code == "insufficient_permissions":
        status = 403
    elif code in ("invalid_credentials", "invalid_key_material"):
        status = 400
    elif code == "network_error":
        status = 502
    else:
        status = 400
    return HTTPException(
        status_code=status,
        detail={"code": code, "message": vr.message or "Validation failed"},
    )


@router.get("", response_model=CoinbaseConnectionOut)
def get_coinbase(
    user: CurrentUser,
    db: DbSession,
) -> CoinbaseConnectionOut:
    tid = primary_tenant_id(db, user)
    if tid is None:
        raise HTTPException(status_code=400, detail="No tenant membership")
    row = get_coinbase_connection(db, tid)
    if row is None:
        raise HTTPException(status_code=404, detail="No Coinbase connection for this tenant")
    return CoinbaseConnectionOut(**connection_public_dict(row))


@router.get("/status", response_model=CoinbaseConnectionStatusOut)
def get_coinbase_status(
    user: CurrentUser,
    db: DbSession,
) -> CoinbaseConnectionStatusOut:
    tid = primary_tenant_id(db, user)
    if tid is None:
        raise HTTPException(status_code=400, detail="No tenant membership")
    row = get_coinbase_connection(db, tid)
    if row is None:
        return CoinbaseConnectionStatusOut(connected=False)
    return CoinbaseConnectionStatusOut(connected=True, **connection_public_dict(row))


@router.post("", response_model=CoinbaseConnectionOut, status_code=201)
def create_coinbase(
    body: CoinbaseConnectionCreate,
    user: CurrentUser,
    db: DbSession,
    settings: Settings = Depends(settings_dep),
    crypto: CredentialCrypto = Depends(_crypto_dep),
) -> CoinbaseConnectionOut:
    tid = primary_tenant_id(db, user)
    if tid is None:
        raise HTTPException(status_code=400, detail="No tenant membership")
    if not crypto.configured:
        raise HTTPException(
            status_code=503,
            detail="Credential encryption is not configured on the server",
        )
    try:
        row, vr = create_coinbase_connection(
            db,
            tenant_id=tid,
            api_key_name=body.api_key_name,
            api_secret_pem=body.api_secret_pem,
            crypto=crypto,
            coinbase_base_url=settings.coinbase_api_base_url,
            coinbase_force_ipv4=settings.coinbase_force_ipv4,
        )
    except ValueError as e:
        if str(e) == "connection_exists":
            raise HTTPException(
                status_code=409,
                detail="A Coinbase connection already exists; use PATCH to rotate credentials",
            ) from e
        raise HTTPException(status_code=400, detail=str(e)) from e
    if row is None:
        raise _validation_http_error(vr)
    return CoinbaseConnectionOut(**connection_public_dict(row))


@router.patch("", response_model=CoinbaseConnectionOut)
def patch_coinbase(
    body: CoinbaseConnectionPatch,
    user: CurrentUser,
    db: DbSession,
    settings: Settings = Depends(settings_dep),
    crypto: CredentialCrypto = Depends(_crypto_dep),
) -> CoinbaseConnectionOut:
    tid = primary_tenant_id(db, user)
    if tid is None:
        raise HTTPException(status_code=400, detail="No tenant membership")
    if not crypto.configured:
        raise HTTPException(status_code=503, detail="Credential encryption is not configured")
    if body.api_key_name is None and body.api_secret_pem is None:
        raise HTTPException(status_code=400, detail="Provide api_key_name and/or api_secret_pem")
    try:
        row, vr = update_coinbase_secret(
            db,
            tenant_id=tid,
            api_key_name=body.api_key_name,
            api_secret_pem=body.api_secret_pem,
            crypto=crypto,
            coinbase_base_url=settings.coinbase_api_base_url,
            coinbase_force_ipv4=settings.coinbase_force_ipv4,
        )
    except ValueError as e:
        if str(e) == "not_found":
            raise HTTPException(status_code=404, detail="No Coinbase connection") from e
        raise HTTPException(status_code=400, detail=str(e)) from e
    if not vr.ok:
        raise _validation_http_error(vr)
    return CoinbaseConnectionOut(**connection_public_dict(row))


@router.post("/validate", response_model=CoinbaseConnectionOut)
@router.post("/reconnect", response_model=CoinbaseConnectionOut)
def post_validate(
    user: CurrentUser,
    db: DbSession,
    settings: Settings = Depends(settings_dep),
    crypto: CredentialCrypto = Depends(_crypto_dep),
) -> CoinbaseConnectionOut:
    """Re-run validation / health check (same as reconnect) using stored ciphertext."""
    tid = primary_tenant_id(db, user)
    if tid is None:
        raise HTTPException(status_code=400, detail="No tenant membership")
    if not crypto.configured:
        raise HTTPException(status_code=503, detail="Credential encryption is not configured")
    try:
        row, vr = validate_existing_connection(
            db,
            tenant_id=tid,
            crypto=crypto,
            coinbase_base_url=settings.coinbase_api_base_url,
            coinbase_force_ipv4=settings.coinbase_force_ipv4,
        )
    except ValueError as e:
        if str(e) == "not_found":
            raise HTTPException(status_code=404, detail="No Coinbase connection") from e
        raise HTTPException(status_code=400, detail=str(e)) from e
    if not vr.ok:
        raise _validation_http_error(vr)
    return CoinbaseConnectionOut(**connection_public_dict(row))


@router.delete("", status_code=204)
def delete_coinbase(
    user: CurrentUser,
    db: DbSession,
) -> None:
    tid = primary_tenant_id(db, user)
    if tid is None:
        raise HTTPException(status_code=400, detail="No tenant membership")
    if not delete_coinbase_connection(db, tenant_id=tid):
        raise HTTPException(status_code=404, detail="No Coinbase connection")
