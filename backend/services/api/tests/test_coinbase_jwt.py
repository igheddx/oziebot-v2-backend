from __future__ import annotations

from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives import serialization
import jwt
import pytest

from oziebot_api.services import coinbase_client


def _pem_private_key() -> str:
    key = ec.generate_private_key(ec.SECP256R1())
    return key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode()


def test_build_cdp_jwt_matches_coinbase_sdk_shape(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(coinbase_client.time, "time", lambda: 1_700_000_000)
    monkeypatch.setattr(coinbase_client, "token_hex", lambda: "fixednonce")

    token = coinbase_client.build_cdp_jwt(
        method="GET",
        request_path="/api/v3/brokerage/accounts",
        host="api.coinbase.com",
        api_key_name="organizations/test/apiKeys/test-key",
        private_key_pem=_pem_private_key(),
    )

    header = jwt.get_unverified_header(token)
    payload = jwt.decode(token, options={"verify_signature": False})

    assert header["kid"] == "organizations/test/apiKeys/test-key"
    assert header["nonce"] == "fixednonce"
    assert header["alg"] == "ES256"
    assert header["typ"] == "JWT"

    assert payload == {
        "iss": "cdp",
        "sub": "organizations/test/apiKeys/test-key",
        "nbf": 1_700_000_000,
        "exp": 1_700_000_120,
        "uri": "GET api.coinbase.com/api/v3/brokerage/accounts",
    }
