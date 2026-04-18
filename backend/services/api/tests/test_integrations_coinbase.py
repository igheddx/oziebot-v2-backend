"""Coinbase integration API: masking, validation, LIVE gating."""

from __future__ import annotations

from unittest.mock import patch

import pytest
from sqlalchemy import select

from oziebot_api.models.exchange_connection import ExchangeConnection
from oziebot_api.services.coinbase_client import CoinbaseValidationResult
from oziebot_api.services.exchange_coinbase_service import mask_api_key_name


def test_mask_api_key_name_masks_midsection():
    assert mask_api_key_name("short") == "***"
    assert "…" in mask_api_key_name("organizations/abc-123-key-id")


@patch("oziebot_api.services.exchange_coinbase_service.validate_coinbase_credentials")
def test_create_masks_key_and_never_returns_secret(mock_val, client):
    mock_val.return_value = CoinbaseValidationResult(
        ok=True,
        can_trade=True,
        can_read_balances=True,
        http_status=200,
    )
    client.post(
        "/v1/auth/register",
        json={
            "email": "cb@example.com",
            "password": "password-123",
            "tenant_name": "CB",
        },
    )
    r = client.post(
        "/v1/auth/login",
        json={"email": "cb@example.com", "password": "password-123"},
    )
    token = r.json()["access_token"]
    r = client.post(
        "/v1/integrations/coinbase",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "api_key_name": "organizations/00-test-key",
            "api_secret_pem": "-----BEGIN EC PRIVATE KEY-----\nFAKE\n-----END EC PRIVATE KEY-----\n",
        },
    )
    assert r.status_code == 201, r.text
    body = r.json()
    assert "api_secret" not in body
    assert "BEGIN" not in str(body)
    assert body["api_key_name_masked"] == "orga…-key"
    assert body["validation_status"] == "valid"

    r2 = client.get(
        "/v1/integrations/coinbase",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert r2.status_code == 200
    assert "api_secret" not in r2.json()
    assert r2.json()["api_key_name_masked"] == body["api_key_name_masked"]


@patch("oziebot_api.services.exchange_coinbase_service.validate_coinbase_credentials")
def test_root_admin_with_tenant_membership_can_manage_coinbase_connection(
    mock_val,
    client,
    tenant_root_user_and_token,
):
    mock_val.return_value = CoinbaseValidationResult(
        ok=True,
        can_trade=True,
        can_read_balances=True,
        http_status=200,
    )
    _, token = tenant_root_user_and_token
    r = client.post(
        "/v1/integrations/coinbase",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "api_key_name": "organizations/00-test-key",
            "api_secret_pem": "-----BEGIN EC PRIVATE KEY-----\nFAKE\n-----END EC PRIVATE KEY-----\n",
        },
    )
    assert r.status_code == 201, r.text

    status = client.get(
        "/v1/integrations/coinbase/status",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert status.status_code == 200, status.text
    assert status.json()["connected"] is True


@patch("oziebot_api.services.exchange_coinbase_service.validate_coinbase_credentials")
def test_create_rejects_invalid_credentials(mock_val, client, db_session):
    mock_val.return_value = CoinbaseValidationResult(
        ok=False,
        error_code="invalid_credentials",
        message="bad",
        http_status=401,
    )
    client.post(
        "/v1/auth/register",
        json={
            "email": "bad@example.com",
            "password": "password-123",
            "tenant_name": "Bad",
        },
    )
    r = client.post(
        "/v1/auth/login",
        json={"email": "bad@example.com", "password": "password-123"},
    )
    token = r.json()["access_token"]
    r = client.post(
        "/v1/integrations/coinbase",
        headers={"Authorization": f"Bearer {token}"},
        json={"api_key_name": "k", "api_secret_pem": "x"},
    )
    assert r.status_code == 400
    assert r.json()["detail"]["code"] == "invalid_credentials"
    n = db_session.scalars(select(ExchangeConnection)).all()
    assert len(n) == 0


@patch("oziebot_api.services.exchange_coinbase_service.validate_coinbase_credentials")
def test_insufficient_permissions_returns_403(mock_val, client):
    mock_val.return_value = CoinbaseValidationResult(
        ok=False,
        error_code="insufficient_permissions",
        message="Forbidden",
        http_status=403,
    )
    client.post(
        "/v1/auth/register",
        json={
            "email": "perm@example.com",
            "password": "password-123",
            "tenant_name": "Perm",
        },
    )
    r = client.post(
        "/v1/auth/login",
        json={"email": "perm@example.com", "password": "password-123"},
    )
    token = r.json()["access_token"]
    r = client.post(
        "/v1/integrations/coinbase",
        headers={"Authorization": f"Bearer {token}"},
        json={"api_key_name": "k", "api_secret_pem": "pem"},
    )
    assert r.status_code == 403


@patch("oziebot_api.services.exchange_coinbase_service.validate_coinbase_credentials")
def test_live_blocked_without_valid_connection(mock_val, client):
    mock_val.return_value = CoinbaseValidationResult(
        ok=True,
        can_trade=True,
        can_read_balances=True,
        http_status=200,
    )
    client.post(
        "/v1/auth/register",
        json={
            "email": "livegate@example.com",
            "password": "password-123",
            "tenant_name": "Gate",
        },
    )
    r = client.post(
        "/v1/auth/login",
        json={"email": "livegate@example.com", "password": "password-123"},
    )
    token = r.json()["access_token"]
    r = client.patch(
        "/v1/me/trading-mode",
        headers={"Authorization": f"Bearer {token}"},
        json={"trading_mode": "live"},
    )
    assert r.status_code == 403

    r = client.post(
        "/v1/integrations/coinbase",
        headers={"Authorization": f"Bearer {token}"},
        json={"api_key_name": "kid", "api_secret_pem": "pem"},
    )
    assert r.status_code == 201

    r = client.patch(
        "/v1/me/trading-mode",
        headers={"Authorization": f"Bearer {token}"},
        json={"trading_mode": "live"},
    )
    assert r.status_code == 200
    assert r.json()["current_trading_mode"] == "live"


@pytest.mark.parametrize(
    ("path", "email"),
    [
        ("/v1/integrations/coinbase/validate", "recon-val@example.com"),
        ("/v1/integrations/coinbase/reconnect", "recon-rec@example.com"),
    ],
)
@patch("oziebot_api.services.exchange_coinbase_service.validate_coinbase_credentials")
def test_validate_and_reconnect_routes(mock_val, client, path, email):
    mock_val.return_value = CoinbaseValidationResult(
        ok=True,
        can_trade=True,
        can_read_balances=True,
        http_status=200,
    )
    client.post(
        "/v1/auth/register",
        json={
            "email": email,
            "password": "password-123",
            "tenant_name": "R",
        },
    )
    r = client.post(
        "/v1/auth/login",
        json={"email": email, "password": "password-123"},
    )
    token = r.json()["access_token"]
    client.post(
        "/v1/integrations/coinbase",
        headers={"Authorization": f"Bearer {token}"},
        json={"api_key_name": "kid", "api_secret_pem": "pem"},
    )
    r = client.post(path, headers={"Authorization": f"Bearer {token}"})
    assert r.status_code == 200
    assert r.json()["validation_status"] == "valid"
