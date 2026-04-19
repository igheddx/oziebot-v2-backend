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
        funding_payload = self._ensure_usd_funding(
            request,
            api_key_name=api_key_name,
            private_key_pem=private_key_pem,
        )
        if funding_payload is not None and funding_payload.get("status") == "failed":
            return ExecutionSubmission(
                status=ExecutionOrderStatus.FAILED,
                venue=Venue.COINBASE,
                raw_payload={"funding_conversion": funding_payload},
                failure_code=str(
                    funding_payload.get("failure_code") or "usd_funding_conversion_failed"
                ),
                failure_detail=str(
                    funding_payload.get("failure_detail")
                    or "Unable to secure USD funding for order"
                ),
            )
        submission = self._client.place_order(
            request, api_key_name=api_key_name, private_key_pem=private_key_pem
        )
        if funding_payload is None:
            return submission
        raw_payload = dict(submission.raw_payload or {})
        raw_payload["funding_conversion"] = funding_payload
        return submission.model_copy(update={"raw_payload": raw_payload})

    def _ensure_usd_funding(
        self,
        request: ExecutionRequest,
        *,
        api_key_name: str,
        private_key_pem: str,
    ) -> dict | None:
        if request.side != Side.BUY or not request.symbol.upper().endswith("-USD"):
            return None

        required_usd = self._required_usd_amount(request)
        if required_usd <= 0:
            return None

        accounts = self._client.list_balances(
            api_key_name=api_key_name,
            private_key_pem=private_key_pem,
        )
        usd_account = self._find_account(accounts, "USD")
        usdc_account = self._find_account(accounts, "USDC")
        usd_available = self._available_balance(usd_account)
        usdc_available = self._available_balance(usdc_account)
        shortfall = _quantize_money(required_usd - usd_available)
        if shortfall <= 0:
            return None
        if usd_account is None or usdc_account is None:
            return {
                "status": "failed",
                "failure_code": "insufficient_quote_balance",
                "failure_detail": (
                    f"Need ${shortfall} more USD for {request.symbol}, but required Coinbase "
                    "USD/USDC funding accounts were not available"
                ),
                "required_usd": str(required_usd),
                "usd_available_before": str(usd_available),
                "usdc_available_before": str(usdc_available),
            }
        if usdc_available < shortfall:
            return {
                "status": "failed",
                "failure_code": "insufficient_quote_balance",
                "failure_detail": (
                    f"Need ${shortfall} more USD for {request.symbol}, but only ${usdc_available} "
                    "USDC was available to convert"
                ),
                "required_usd": str(required_usd),
                "usd_available_before": str(usd_available),
                "usdc_available_before": str(usdc_available),
            }

        from_account = self._account_id(usdc_account)
        to_account = self._account_id(usd_account)
        if not from_account or not to_account:
            return {
                "status": "failed",
                "failure_code": "usd_funding_conversion_failed",
                "failure_detail": "Coinbase USD/USDC account identifiers were missing",
                "required_usd": str(required_usd),
                "usd_available_before": str(usd_available),
                "usdc_available_before": str(usdc_available),
            }

        quote = self._client.create_convert_quote(
            from_account=from_account,
            to_account=to_account,
            amount=str(shortfall),
            api_key_name=api_key_name,
            private_key_pem=private_key_pem,
        )
        trade_id = self._trade_id(quote)
        if not trade_id:
            return {
                "status": "failed",
                "failure_code": "usd_funding_conversion_failed",
                "failure_detail": str(
                    (quote.get("error_response", {}) or {}).get("message")
                    or "Coinbase did not return a convert trade identifier"
                ),
                "required_usd": str(required_usd),
                "usd_available_before": str(usd_available),
                "usdc_available_before": str(usdc_available),
                "quote_response": quote,
            }
        commit = self._client.commit_convert_trade(
            trade_id,
            from_account=from_account,
            to_account=to_account,
            api_key_name=api_key_name,
            private_key_pem=private_key_pem,
        )
        commit_trade_id = self._trade_id(commit)
        if not commit_trade_id:
            return {
                "status": "failed",
                "failure_code": "usd_funding_conversion_failed",
                "failure_detail": str(
                    (commit.get("error_response", {}) or {}).get("message")
                    or "Coinbase did not confirm the USD funding conversion"
                ),
                "required_usd": str(required_usd),
                "usd_available_before": str(usd_available),
                "usdc_available_before": str(usdc_available),
                "quote_response": quote,
                "commit_response": commit,
            }
        return {
            "status": "converted",
            "required_usd": str(required_usd),
            "usd_available_before": str(usd_available),
            "usdc_available_before": str(usdc_available),
            "converted_amount": str(shortfall),
            "from_currency": "USDC",
            "to_currency": "USD",
            "trade_id": commit_trade_id,
            "quote_response": quote,
            "commit_response": commit,
        }

    @staticmethod
    def _required_usd_amount(request: ExecutionRequest) -> Decimal:
        price = request.price_hint or Decimal("0")
        if price <= 0:
            return Decimal("0")
        return _quantize_money(request.quantity * price)

    @staticmethod
    def _find_account(accounts: list[dict], currency: str) -> dict | None:
        for account in accounts:
            if LiveCoinbaseExecutionAdapter._account_currency(account) == currency:
                return account
        return None

    @staticmethod
    def _account_currency(account: dict | None) -> str:
        if not account:
            return ""
        return str(
            (account.get("available_balance") or {}).get("currency")
            or account.get("currency")
            or ""
        ).upper()

    @staticmethod
    def _available_balance(account: dict | None) -> Decimal:
        if not account:
            return Decimal("0")
        return _quantize_money(
            Decimal(
                str(
                    (account.get("available_balance") or {}).get("value")
                    or account.get("available")
                    or "0"
                )
            )
        )

    @staticmethod
    def _account_id(account: dict) -> str | None:
        for field in ("uuid", "account_uuid", "id"):
            value = account.get(field)
            if value:
                return str(value)
        return None

    @staticmethod
    def _trade_id(payload: dict) -> str | None:
        trade = payload.get("trade")
        if isinstance(trade, dict) and trade.get("id"):
            return str(trade["id"])
        for field in ("trade_id", "quote_id", "id"):
            value = payload.get(field)
            if value:
                return str(value)
        return None
