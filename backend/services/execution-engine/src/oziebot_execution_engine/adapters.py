from __future__ import annotations

import json
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP
from typing import Protocol

from oziebot_domain.execution import (
    ExecutionFill,
    ExecutionOrderStatus,
    ExecutionRequest,
    ExecutionSubmission,
)
from oziebot_domain.trading import OrderType, Side, Venue

from oziebot_execution_engine.coinbase_client import CoinbaseExecutionClient


def _quantize_money(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


class ExecutionAdapter(Protocol):
    def submit(self, request: ExecutionRequest) -> ExecutionSubmission: ...


class PaperExecutionAdapter:
    def __init__(self, redis_client, *, fee_bps: int, slippage_bps: int) -> None:
        self._redis = redis_client
        self._default_fee_bps = fee_bps
        self._default_slippage_bps = slippage_bps

    def submit(self, request: ExecutionRequest) -> ExecutionSubmission:
        raw = self._redis.get(f"oziebot:md:bbo:{request.symbol}")
        payload = json.loads(raw) if raw else {}
        if request.side == Side.BUY:
            base_price = Decimal(
                str(payload.get("best_ask_price", request.price_hint or "0"))
            )
        else:
            base_price = Decimal(
                str(payload.get("best_bid_price", request.price_hint or "0"))
            )
        if base_price <= 0:
            return ExecutionSubmission(
                status=ExecutionOrderStatus.FAILED,
                venue=Venue.COINBASE,
                raw_payload=payload,
                failure_code="missing_market_data",
                failure_detail="No executable market data available for paper fill",
            )
        profile = dict(request.fee_profile or {})
        maker_fee_bps = Decimal(
            str(profile.get("maker_fee_bps", self._default_fee_bps))
        )
        taker_fee_bps = Decimal(
            str(profile.get("taker_fee_bps", self._default_fee_bps))
        )
        slippage_bps = Decimal(
            str(profile.get("estimated_slippage_bps", self._default_slippage_bps))
        )
        limit_offset_bps = Decimal(str(request.limit_price_offset_bps))
        market_payload = {
            "paper": True,
            "market_data": payload,
            "execution_preference": request.execution_preference,
            "fallback_behavior": request.fallback_behavior,
        }

        if (
            request.order_type == OrderType.MARKET
            or request.execution_preference == "taker_only"
        ):
            fill = self._build_fill(
                request=request,
                fill_id=f"paper-{request.client_order_id}",
                quantity=request.quantity,
                base_price=base_price,
                fill_type="taker",
                fee_bps=taker_fee_bps,
                slippage_bps=slippage_bps,
                payload=payload,
            )
            return ExecutionSubmission(
                status=ExecutionOrderStatus.FILLED,
                venue=Venue.COINBASE,
                venue_order_id=request.client_order_id,
                fills=[fill],
                raw_payload=market_payload,
                actual_fill_type="taker",
            )

        if request.fallback_behavior == "cancel":
            return ExecutionSubmission(
                status=ExecutionOrderStatus.CANCELLED,
                venue=Venue.COINBASE,
                venue_order_id=request.client_order_id,
                raw_payload={**market_payload, "cancelled_after_timeout": True},
                failure_code="maker_timeout_cancelled",
                failure_detail="Maker order cancelled after timeout in paper simulation",
                fallback_triggered=True,
            )

        if request.fallback_behavior == "reprice":
            repriced_fill = self._build_fill(
                request=request,
                fill_id=f"paper-{request.client_order_id}-repriced",
                quantity=request.quantity,
                base_price=base_price,
                fill_type="maker",
                fee_bps=maker_fee_bps,
                slippage_bps=Decimal("0"),
                payload={**payload, "repriced": True},
                limit_offset_bps=limit_offset_bps / Decimal("2"),
            )
            return ExecutionSubmission(
                status=ExecutionOrderStatus.FILLED,
                venue=Venue.COINBASE,
                venue_order_id=request.client_order_id,
                fills=[repriced_fill],
                raw_payload={**market_payload, "repriced_after_timeout": True},
                fallback_triggered=True,
                actual_fill_type="maker",
            )

        maker_qty = (request.quantity * Decimal("0.5")).quantize(
            Decimal("0.00000001"),
            rounding=ROUND_DOWN,
        )
        if maker_qty <= 0 or maker_qty >= request.quantity:
            maker_fill = self._build_fill(
                request=request,
                fill_id=f"paper-{request.client_order_id}-maker",
                quantity=request.quantity,
                base_price=base_price,
                fill_type="maker",
                fee_bps=maker_fee_bps,
                slippage_bps=Decimal("0"),
                payload=payload,
                limit_offset_bps=limit_offset_bps,
            )
            return ExecutionSubmission(
                status=ExecutionOrderStatus.FILLED,
                venue=Venue.COINBASE,
                venue_order_id=request.client_order_id,
                fills=[maker_fill],
                raw_payload=market_payload,
                actual_fill_type="maker",
            )

        taker_qty = (request.quantity - maker_qty).quantize(
            Decimal("0.00000001"),
            rounding=ROUND_HALF_UP,
        )
        maker_fill = self._build_fill(
            request=request,
            fill_id=f"paper-{request.client_order_id}-maker",
            quantity=maker_qty,
            base_price=base_price,
            fill_type="maker",
            fee_bps=maker_fee_bps,
            slippage_bps=Decimal("0"),
            payload=payload,
            limit_offset_bps=limit_offset_bps,
        )
        taker_fill = self._build_fill(
            request=request,
            fill_id=f"paper-{request.client_order_id}-taker",
            quantity=taker_qty,
            base_price=base_price,
            fill_type="taker",
            fee_bps=taker_fee_bps,
            slippage_bps=slippage_bps,
            payload={**payload, "fallback_triggered": True},
        )
        return ExecutionSubmission(
            status=ExecutionOrderStatus.FILLED,
            venue=Venue.COINBASE,
            venue_order_id=request.client_order_id,
            fills=[maker_fill, taker_fill],
            raw_payload={**market_payload, "fallback_triggered": True},
            fallback_triggered=True,
            actual_fill_type="mixed",
        )

    def _build_fill(
        self,
        *,
        request: ExecutionRequest,
        fill_id: str,
        quantity: Decimal,
        base_price: Decimal,
        fill_type: str,
        fee_bps: Decimal,
        slippage_bps: Decimal,
        payload: dict,
        limit_offset_bps: Decimal = Decimal("0"),
    ) -> ExecutionFill:
        if request.side == Side.BUY:
            if fill_type == "maker":
                fill_price = _quantize_money(
                    base_price * (Decimal("1") - (limit_offset_bps / Decimal("10000")))
                )
            else:
                fill_price = _quantize_money(
                    base_price * (Decimal("1") + (slippage_bps / Decimal("10000")))
                )
        else:
            if fill_type == "maker":
                fill_price = _quantize_money(
                    base_price * (Decimal("1") + (limit_offset_bps / Decimal("10000")))
                )
            else:
                fill_price = _quantize_money(
                    base_price * (Decimal("1") - (slippage_bps / Decimal("10000")))
                )
        fee = _quantize_money(fill_price * quantity * (fee_bps / Decimal("10000")))
        return ExecutionFill(
            fill_id=fill_id,
            quantity=quantity,
            price=fill_price,
            fee=fee,
            liquidity=fill_type,
            slippage_bps=slippage_bps,
            raw_payload=payload,
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
