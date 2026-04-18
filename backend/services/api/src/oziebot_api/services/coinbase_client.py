"""Coinbase Advanced Trade REST client — JWT (ES256) auth (CDP API keys)."""

from __future__ import annotations

import json
import time
from secrets import token_hex
from typing import Any

import httpx
import jwt
from cryptography.hazmat.primitives import serialization
from pydantic import BaseModel, Field

DEFAULT_BASE_URL = "https://api.coinbase.com"
ACCOUNTS_PATH = "/api/v3/brokerage/accounts"


class CoinbaseValidationResult(BaseModel):
    ok: bool
    error_code: str | None = None
    message: str | None = None
    can_trade: bool = False
    can_read_balances: bool = False
    http_status: int | None = None
    raw_body_preview: str | None = Field(default=None, repr=False)


def _host_from_base(base_url: str) -> str:
    u = base_url.strip().rstrip("/")
    for prefix in ("https://", "http://"):
        if u.startswith(prefix):
            u = u[len(prefix) :]
    return u.split("/")[0]


def _error_hint(raw_body: str) -> str | None:
    text = raw_body.strip()
    if not text:
        return None
    try:
        payload = json.loads(text)
    except Exception:
        return text[:180]
    if isinstance(payload, dict):
        for key in ("message", "error", "error_description", "details"):
            value = payload.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()[:180]
    return text[:180]


def build_cdp_jwt(
    *,
    method: str,
    request_path: str,
    host: str,
    api_key_name: str,
    private_key_pem: str,
) -> str:
    """Build short-lived JWT for Coinbase CDP / Advanced Trade REST (ES256)."""
    uri = f"{method.upper()} {host}{request_path}"
    now = int(time.time())
    payload: dict[str, Any] = {
        "iss": "cdp",
        "sub": api_key_name,
        "nbf": now,
        "exp": now + 120,
        "uri": uri,
    }
    key = serialization.load_pem_private_key(private_key_pem.encode(), password=None)
    if key is None:
        raise ValueError("Invalid private key")
    return jwt.encode(
        payload,
        key,
        algorithm="ES256",
        headers={"kid": api_key_name, "nonce": token_hex()},
    )


def validate_coinbase_credentials(
    api_key_name: str,
    private_key_pem: str,
    *,
    base_url: str = DEFAULT_BASE_URL,
    timeout: float = 20.0,
    force_ipv4: bool = False,
) -> CoinbaseValidationResult:
    """
    Verify credentials against Coinbase Advanced Trade (brokerage accounts list).

    Expects a PEM ECDSA private key (Coinbase CDP API secret). On success, assumes
    balance read + trade capability for this key (refine with additional probes later).
    """
    host = _host_from_base(base_url)
    try:
        token = build_cdp_jwt(
            method="GET",
            request_path=ACCOUNTS_PATH,
            host=host,
            api_key_name=api_key_name.strip(),
            private_key_pem=private_key_pem,
        )
    except Exception as e:
        return CoinbaseValidationResult(
            ok=False,
            error_code="invalid_key_material",
            message=str(e)[:500],
        )

    url = base_url.rstrip("/") + ACCOUNTS_PATH
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    try:
        client_kwargs: dict[str, Any] = {"timeout": timeout}
        if force_ipv4:
            # Bind to IPv4 local address so outbound socket resolution prefers IPv4.
            client_kwargs["transport"] = httpx.HTTPTransport(local_address="0.0.0.0")
        with httpx.Client(**client_kwargs) as client:
            r = client.get(url, headers=headers)
    except httpx.HTTPError as e:
        return CoinbaseValidationResult(
            ok=False,
            error_code="network_error",
            message=str(e)[:500],
        )

    body_preview = (r.text or "")[:500]
    hint = _error_hint(body_preview)
    if r.status_code == 200:
        return CoinbaseValidationResult(
            ok=True,
            can_trade=True,
            can_read_balances=True,
            http_status=200,
            raw_body_preview=body_preview,
        )

    if r.status_code == 401:
        detail = (
            "Coinbase API key/secret were rejected by Coinbase (upstream 401). "
            "Use a Coinbase CDP / Advanced Trade API key pair, not a legacy Exchange key."
        )
        if hint:
            detail = f"{detail}: {hint}"
        return CoinbaseValidationResult(
            ok=False,
            error_code="invalid_credentials",
            message=detail,
            http_status=401,
            raw_body_preview=body_preview,
        )

    if r.status_code == 403:
        detail = "Coinbase denied access — check API key permissions for trading and balances"
        if hint:
            detail = f"{detail}: {hint}"
        return CoinbaseValidationResult(
            ok=False,
            error_code="insufficient_permissions",
            message=detail,
            can_trade=False,
            can_read_balances=False,
            http_status=403,
            raw_body_preview=body_preview,
        )

    detail = f"Unexpected HTTP {r.status_code}"
    if hint:
        detail = f"{detail}: {hint}"
    return CoinbaseValidationResult(
        ok=False,
        error_code="unexpected_response",
        message=detail,
        http_status=r.status_code,
        raw_body_preview=body_preview,
    )
