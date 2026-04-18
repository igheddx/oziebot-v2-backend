from __future__ import annotations

import json
from decimal import Decimal, ROUND_HALF_UP
from typing import Protocol

from oziebot_domain.execution import (
    ExecutionFill,
    ExecutionOrderStatus,
    ExecutionRequest,
    ExecutionSubmission,
)
from oziebot_domain.trading import Side, Venue

from oziebot_execution_engine.coinbase_client import CoinbaseExecutionClient


def _quantize_money(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


class ExecutionAdapter(Protocol):
    def submit(self, request: ExecutionRequest) -> ExecutionSubmission: ...


class PaperExecutionAdapter:
    def __init__(self, redis_client, *, fee_bps: int, slippage_bps: int) -> None:
        self._redis = redis_client
        self._fee_bps = Decimal(fee_bps) / Decimal(10000)
        self._slippage_bps = Decimal(slippage_bps) / Decimal(10000)

    def submit(self, request: ExecutionRequest) -> ExecutionSubmission:
        raw = self._redis.get(f"oziebot:md:bbo:{request.symbol}")
        payload = json.loads(raw) if raw else {}
        if request.side == Side.BUY:
            base_price = Decimal(
                str(payload.get("best_ask_price", request.price_hint or "0"))
            )
            slip_multiplier = Decimal("1") + self._slippage_bps
        else:
            base_price = Decimal(
                str(payload.get("best_bid_price", request.price_hint or "0"))
            )
            slip_multiplier = Decimal("1") - self._slippage_bps
        if base_price <= 0:
            return ExecutionSubmission(
                status=ExecutionOrderStatus.FAILED,
                venue=Venue.COINBASE,
                raw_payload=payload,
                failure_code="missing_market_data",
                failure_detail="No executable market data available for paper fill",
            )
        fill_price = _quantize_money(base_price * slip_multiplier)
        fee = _quantize_money(fill_price * request.quantity * self._fee_bps)
        fill = ExecutionFill(
            fill_id=f"paper-{request.client_order_id}",
            quantity=request.quantity,
            price=fill_price,
            fee=fee,
            liquidity="simulated",
            raw_payload=payload,
        )
        return ExecutionSubmission(
            status=ExecutionOrderStatus.FILLED,
            venue=Venue.COINBASE,
            venue_order_id=request.client_order_id,
            fills=[fill],
            raw_payload={"paper": True, "market_data": payload},
        )


class LiveCoinbaseExecutionAdapter:
    def __init__(self, client: CoinbaseExecutionClient, credential_loader) -> None:
        self._client = client
        self._credential_loader = credential_loader

    def submit(self, request: ExecutionRequest) -> ExecutionSubmission:
        api_key_name, private_key_pem = self._credential_loader(request.tenant_id)
        return self._client.place_order(
            request, api_key_name=api_key_name, private_key_pem=private_key_pem
        )
