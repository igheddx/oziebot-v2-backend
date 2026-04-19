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
from oziebot_domain.trading import OrderType, Side, Venue

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

    def create_convert_quote(
        self,
        *,
        from_account: str,
        to_account: str,
        amount: str,
        api_key_name: str,
        private_key_pem: str,
    ) -> dict[str, Any]: ...

    def commit_convert_trade(
        self,
        trade_id: str,
        *,
        from_account: str,
        to_account: str,
        api_key_name: str,
        private_key_pem: str,
    ) -> dict[str, Any]: ...


class HttpCoinbaseExecutionClient:
    def __init__(self, base_url: str, timeout: float = 20.0) -> None:
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout

    def place_order(
        self, request: ExecutionRequest, *, api_key_name: str, private_key_pem: str
    ) -> ExecutionSubmission:
        body = self._order_body(request)
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
                fee = self._extract_fill_fee(payload)
                liquidity = self._extract_fill_liquidity(payload, request)
                slippage_bps = self._realized_slippage_bps(
                    request=request,
                    filled_price=filled_price,
                )
                fills.append(
                    ExecutionFill(
                        fill_id=str(order_id or request.client_order_id),
                        quantity=request.quantity,
                        price=filled_price,
                        fee=fee,
                        liquidity=liquidity,
                        slippage_bps=slippage_bps,
                        raw_payload={
                            **payload,
                            "execution_quality": self._execution_quality_payload(
                                request=request,
                                filled_price=filled_price,
                                fee=fee,
                            ),
                        },
                    )
                )
                status = ExecutionOrderStatus.FILLED
        return ExecutionSubmission(
            status=status,
            venue=Venue.COINBASE,
            venue_order_id=str(order_id) if order_id else None,
            fills=fills,
            raw_payload=payload,
            actual_fill_type=fills[0].liquidity if fills else None,
        )

    def _order_body(self, request: ExecutionRequest) -> dict[str, Any]:
        body = {
            "client_order_id": request.client_order_id,
            "product_id": request.symbol,
            "side": request.side.value.upper(),
        }
        if request.order_type == OrderType.LIMIT:
            limit_price = self._limit_price(request)
            body["order_configuration"] = {
                "limit_limit_gtc": {
                    "base_size": str(request.quantity),
                    "limit_price": str(limit_price),
                    "post_only": request.execution_preference == "maker_preferred",
                }
            }
            return body
        body["order_configuration"] = {
            "market_market_ioc": {
                "base_size": str(request.quantity),
            }
        }
        return body

    def _limit_price(self, request: ExecutionRequest) -> Decimal:
        reference = request.price_hint or Decimal("0")
        offset = Decimal(str(request.limit_price_offset_bps)) / Decimal("10000")
        if reference <= 0:
            return Decimal("0")
        if request.side == Side.BUY:
            return (reference * (Decimal("1") - offset)).quantize(Decimal("0.01"))
        return (reference * (Decimal("1") + offset)).quantize(Decimal("0.01"))

    @staticmethod
    def _to_decimal(value: Any) -> Decimal | None:
        if value in (None, ""):
            return None
        try:
            return Decimal(str(value))
        except Exception:
            return None

    def _extract_fill_fee(self, payload: dict[str, Any]) -> Decimal:
        success = payload.get("success_response", {}) or {}
        candidates = (
            success.get("total_fees"),
            success.get("total_fee"),
            success.get("filled_fees"),
            success.get("commission"),
            payload.get("total_fees"),
            payload.get("fee"),
        )
        for candidate in candidates:
            fee = self._to_decimal(candidate)
            if fee is not None and fee >= 0:
                return fee
        nested_fee = self._sum_fill_fees(success.get("fills")) or self._sum_fill_fees(
            payload.get("fills")
        )
        if nested_fee is not None:
            return nested_fee
        return Decimal("0")

    def _sum_fill_fees(self, fills: Any) -> Decimal | None:
        if not isinstance(fills, list):
            return None
        total = Decimal("0")
        found = False
        for fill in fills:
            if not isinstance(fill, dict):
                continue
            for field in ("commission", "fee", "filled_fees", "total_fees"):
                fee = self._to_decimal(fill.get(field))
                if fee is not None and fee >= 0:
                    total += fee
                    found = True
                    break
        return total if found else None

    def _execution_quality_payload(
        self,
        *,
        request: ExecutionRequest,
        filled_price: Decimal,
        fee: Decimal,
    ) -> dict[str, Any]:
        reference_price = request.price_hint or filled_price
        slippage_pct = Decimal("0")
        if reference_price > 0:
            if request.side == Side.BUY:
                slippage_pct = max(
                    Decimal("0"),
                    (filled_price - reference_price) / reference_price,
                )
            else:
                slippage_pct = max(
                    Decimal("0"),
                    (reference_price - filled_price) / reference_price,
                )
        return {
            "price_hint": str(reference_price),
            "filled_price": str(filled_price),
            "realized_slippage_pct": str(slippage_pct.quantize(Decimal("0.00000001"))),
            "realized_slippage_bps": str(
                (slippage_pct * Decimal("10000")).quantize(Decimal("0.01"))
            ),
            "fee": str(fee),
        }

    def _realized_slippage_bps(
        self,
        *,
        request: ExecutionRequest,
        filled_price: Decimal,
    ) -> Decimal:
        reference_price = request.price_hint or filled_price
        if reference_price <= 0:
            return Decimal("0")
        if request.side == Side.BUY:
            slippage_pct = max(
                Decimal("0"), (filled_price - reference_price) / reference_price
            )
        else:
            slippage_pct = max(
                Decimal("0"), (reference_price - filled_price) / reference_price
            )
        return (slippage_pct * Decimal("10000")).quantize(Decimal("0.01"))

    def _extract_fill_liquidity(
        self,
        payload: dict[str, Any],
        request: ExecutionRequest,
    ) -> str:
        success = payload.get("success_response", {}) or {}
        fills = success.get("fills") or payload.get("fills") or []
        if isinstance(fills, list):
            values = []
            for fill in fills:
                if not isinstance(fill, dict):
                    continue
                indicator = str(
                    fill.get("liquidity_indicator")
                    or fill.get("liquidity")
                    or fill.get("liquidity_type")
                    or ""
                ).lower()
                if "maker" in indicator:
                    values.append("maker")
                elif "taker" in indicator:
                    values.append("taker")
            if values:
                return values[0] if len(set(values)) == 1 else "mixed"
        if (
            request.order_type == OrderType.LIMIT
            and request.execution_preference == "maker_preferred"
        ):
            return "maker"
        return "taker" if request.order_type == OrderType.MARKET else "unknown"

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

    def create_convert_quote(
        self,
        *,
        from_account: str,
        to_account: str,
        amount: str,
        api_key_name: str,
        private_key_pem: str,
    ) -> dict[str, Any]:
        return self._post(
            "/api/v3/brokerage/convert/quote",
            {
                "from_account": from_account,
                "to_account": to_account,
                "amount": amount,
            },
            api_key_name=api_key_name,
            private_key_pem=private_key_pem,
        )

    def commit_convert_trade(
        self,
        trade_id: str,
        *,
        from_account: str,
        to_account: str,
        api_key_name: str,
        private_key_pem: str,
    ) -> dict[str, Any]:
        return self._post(
            f"/api/v3/brokerage/convert/trade/{trade_id}",
            {
                "from_account": from_account,
                "to_account": to_account,
            },
            api_key_name=api_key_name,
            private_key_pem=private_key_pem,
        )

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

    def _post(
        self,
        path: str,
        body: dict[str, Any],
        *,
        api_key_name: str,
        private_key_pem: str,
    ) -> dict[str, Any]:
        host = _host_from_base(self._base_url)
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
        with httpx.Client(timeout=self._timeout) as client:
            response = client.post(f"{self._base_url}{path}", headers=headers, json=body)
        payload = response.json() if response.content else {}
        if response.status_code >= 400:
            return {
                "success": False,
                "error_response": {
                    "message": (payload.get("error_response", {}) or {}).get("message")
                    or response.text[:400]
                },
                "status_code": response.status_code,
            }
        return payload
