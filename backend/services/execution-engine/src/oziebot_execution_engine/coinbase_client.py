from __future__ import annotations

import time
from decimal import Decimal
from secrets import token_hex
from typing import Any, Protocol

import httpx
import jwt
from cryptography.hazmat.primitives import serialization

from oziebot_domain.execution import (
    ExecutionFill,
    ExecutionOrderStatus,
    ExecutionRequest,
    ExecutionSubmission,
)
from oziebot_domain.trading import Venue

ORDERS_PATH = "/api/v3/brokerage/orders"


def _host_from_base(base_url: str) -> str:
    value = base_url.strip().rstrip("/")
    for prefix in ("https://", "http://"):
        if value.startswith(prefix):
            value = value[len(prefix) :]
    return value.split("/")[0]


def build_cdp_jwt(
    *,
    method: str,
    request_path: str,
    host: str,
    api_key_name: str,
    private_key_pem: str,
) -> str:
    now = int(time.time())
    payload = {
        "iss": "cdp",
        "nbf": now,
        "exp": now + 120,
        "sub": api_key_name,
        "uri": f"{method.upper()} {host}{request_path}",
    }
    key = serialization.load_pem_private_key(private_key_pem.encode(), password=None)
    return jwt.encode(
        payload,
        key,
        algorithm="ES256",
        headers={"kid": api_key_name, "nonce": token_hex()},
    )


class CoinbaseExecutionClient(Protocol):
    def place_order(
        self, request: ExecutionRequest, *, api_key_name: str, private_key_pem: str
    ) -> ExecutionSubmission: ...

    def cancel_order(
        self, venue_order_id: str, *, api_key_name: str, private_key_pem: str
    ) -> dict[str, Any]: ...

    def list_balances(
        self, *, api_key_name: str, private_key_pem: str
    ) -> list[dict[str, Any]]: ...

    def list_open_orders(
        self, *, api_key_name: str, private_key_pem: str
    ) -> list[dict[str, Any]]: ...

    def list_fills(
        self, *, api_key_name: str, private_key_pem: str, product_id: str | None = None
    ) -> list[dict[str, Any]]: ...


class HttpCoinbaseExecutionClient:
    def __init__(self, base_url: str, timeout: float = 20.0) -> None:
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout

    def place_order(
        self, request: ExecutionRequest, *, api_key_name: str, private_key_pem: str
    ) -> ExecutionSubmission:
        body = {
            "client_order_id": request.client_order_id,
            "product_id": request.symbol,
            "side": request.side.value.upper(),
            "order_configuration": {
                "market_market_ioc": {
                    "base_size": str(request.quantity),
                }
            },
        }
        host = _host_from_base(self._base_url)
        token = build_cdp_jwt(
            method="POST",
            request_path=ORDERS_PATH,
            host=host,
            api_key_name=api_key_name,
            private_key_pem=private_key_pem,
        )
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
        with httpx.Client(timeout=self._timeout) as client:
            response = client.post(
                f"{self._base_url}{ORDERS_PATH}", headers=headers, json=body
            )
        payload = response.json() if response.content else {}
        if response.status_code >= 400:
            return ExecutionSubmission(
                status=ExecutionOrderStatus.FAILED,
                venue=Venue.COINBASE,
                raw_payload=payload,
                failure_code=str(response.status_code),
                failure_detail=(payload.get("error_response", {}) or {}).get("message")
                or response.text[:400],
            )
        success = bool(payload.get("success", True))
        order_id = payload.get("success_response", {}).get("order_id") or payload.get(
            "order_id"
        )
        if not success and order_id is None:
            return ExecutionSubmission(
                status=ExecutionOrderStatus.FAILED,
                venue=Venue.COINBASE,
                raw_payload=payload,
                failure_code="coinbase_rejected",
                failure_detail=(payload.get("error_response", {}) or {}).get("message")
                or "Coinbase rejected order",
            )
        status = ExecutionOrderStatus.PENDING
        fills: list[ExecutionFill] = []
        completion = (payload.get("success_response", {}) or {}).get(
            "completion_percentage"
        )
        if completion == "100":
            filled_price = Decimal(
                str(
                    (payload.get("success_response", {}) or {}).get(
                        "average_filled_price", request.price_hint or "0"
                    )
                )
            )
            if filled_price > 0:
                fills.append(
                    ExecutionFill(
                        fill_id=str(order_id or request.client_order_id),
                        quantity=request.quantity,
                        price=filled_price,
                        fee=Decimal("0"),
                        liquidity="unknown",
                        raw_payload=payload,
                    )
                )
                status = ExecutionOrderStatus.FILLED
        return ExecutionSubmission(
            status=status,
            venue=Venue.COINBASE,
            venue_order_id=str(order_id) if order_id else None,
            fills=fills,
            raw_payload=payload,
        )

    def list_balances(
        self, *, api_key_name: str, private_key_pem: str
    ) -> list[dict[str, Any]]:
        payload = self._request(
            "GET",
            "/api/v3/brokerage/accounts",
            api_key_name=api_key_name,
            private_key_pem=private_key_pem,
        )
        return list(payload.get("accounts", []))

    def list_open_orders(
        self, *, api_key_name: str, private_key_pem: str
    ) -> list[dict[str, Any]]:
        payload = self._request(
            "GET",
            f"{ORDERS_PATH}/historical/batch?order_status=OPEN",
            api_key_name=api_key_name,
            private_key_pem=private_key_pem,
        )
        return list(payload.get("orders", []))

    def list_fills(
        self, *, api_key_name: str, private_key_pem: str, product_id: str | None = None
    ) -> list[dict[str, Any]]:
        path = "/api/v3/brokerage/orders/historical/fills"
        if product_id:
            path = f"{path}?product_id={product_id}"
        payload = self._request(
            "GET", path, api_key_name=api_key_name, private_key_pem=private_key_pem
        )
        return list(payload.get("fills", []))

    def cancel_order(
        self, venue_order_id: str, *, api_key_name: str, private_key_pem: str
    ) -> dict[str, Any]:
        host = _host_from_base(self._base_url)
        path = f"{ORDERS_PATH}/batch_cancel"
        token = build_cdp_jwt(
            method="POST",
            request_path=path,
            host=host,
            api_key_name=api_key_name,
            private_key_pem=private_key_pem,
        )
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
        body = {"order_ids": [venue_order_id]}
        with httpx.Client(timeout=self._timeout) as client:
            response = client.post(
                f"{self._base_url}{path}", headers=headers, json=body
            )
        return (
            response.json()
            if response.content
            else {"status_code": response.status_code}
        )

    def _request(
        self, method: str, path: str, *, api_key_name: str, private_key_pem: str
    ) -> dict[str, Any]:
        host = _host_from_base(self._base_url)
        token = build_cdp_jwt(
            method=method,
            request_path=path,
            host=host,
            api_key_name=api_key_name,
            private_key_pem=private_key_pem,
        )
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
        with httpx.Client(timeout=self._timeout) as client:
            response = client.request(
                method, f"{self._base_url}{path}", headers=headers
            )
        response.raise_for_status()
        return response.json() if response.content else {}
