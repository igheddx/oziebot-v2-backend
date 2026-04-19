from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from oziebot_api.config import Settings
from oziebot_api.models.exchange_connection import ExchangeConnection
from oziebot_api.models.user import User
from oziebot_api.services.coinbase_client import list_coinbase_accounts
from oziebot_api.services.credential_crypto import CredentialCrypto
from oziebot_api.services.tenant_scope import primary_tenant_id

CASH_EQUIVALENT_CURRENCIES = {"USD", "USDC", "USDT"}


def _to_decimal(value: str | None) -> Decimal:
    if value is None:
        return Decimal("0")
    try:
        return Decimal(value)
    except (InvalidOperation, ValueError):
        return Decimal("0")


def _cents(amount: Decimal) -> int:
    return int((amount * Decimal("100")).quantize(Decimal("1")))


def load_live_coinbase_accounts(
    db: Session,
    *,
    user: User,
    settings: Settings,
) -> list[dict[str, Any]] | None:
    tenant_id = primary_tenant_id(db, user)
    if tenant_id is None:
        return None

    connection = db.scalar(
        select(ExchangeConnection).where(
            ExchangeConnection.tenant_id == tenant_id,
            ExchangeConnection.provider == "coinbase",
        )
    )
    if (
        connection is None
        or connection.validation_status != "valid"
        or connection.health_status != "healthy"
        or connection.can_read_balances is not True
    ):
        return None

    crypto = CredentialCrypto(settings.exchange_credentials_encryption_key)
    if not crypto.configured:
        return None

    try:
        private_key_pem = crypto.decrypt(connection.encrypted_secret).decode("utf-8")
        return list_coinbase_accounts(
            connection.api_key_name,
            private_key_pem,
            base_url=settings.coinbase_api_base_url,
            force_ipv4=settings.coinbase_force_ipv4,
        )
    except Exception:
        return None


def sum_coinbase_cash_cents(
    accounts: list[dict[str, Any]],
    *,
    include_hold: bool,
) -> int:
    total_cents = 0
    for account in accounts:
        currency = str(
            account.get("currency")
            or (account.get("available_balance") or {}).get("currency")
            or ""
        ).upper()
        if currency not in CASH_EQUIVALENT_CURRENCIES:
            continue
        available = _to_decimal((account.get("available_balance") or {}).get("value"))
        hold = _to_decimal((account.get("hold") or {}).get("value"))
        total = available + hold if include_hold else available
        if total <= 0:
            continue
        total_cents += _cents(total)
    return total_cents
