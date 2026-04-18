from __future__ import annotations

from decimal import Decimal
from uuid import uuid4

import httpx

from oziebot_domain.execution import ExecutionRequest
from oziebot_domain.risk import RiskDecision, RiskOutcome
from oziebot_domain.trading import OrderType, Side
from oziebot_domain.trading_mode import TradingMode
from oziebot_execution_engine.coinbase_client import HttpCoinbaseExecutionClient


def _request() -> ExecutionRequest:
    return ExecutionRequest(
        intent_id=uuid4(),
        trace_id="trace",
        user_id=uuid4(),
        risk=RiskDecision(
            outcome=RiskOutcome.APPROVE,
            approved=True,
            signal_id=uuid4(),
            run_id=uuid4(),
            user_id=uuid4(),
            strategy_name="momentum",
            symbol="BTC-USD",
            original_size="0.5",
            final_size="0.5",
            trading_mode=TradingMode.LIVE,
            trace_id="trace",
        ),
        tenant_id=uuid4(),
        trading_mode=TradingMode.LIVE,
        strategy_id="momentum",
        symbol="BTC-USD",
        side=Side.BUY,
        order_type=OrderType.MARKET,
        quantity=Decimal("0.5"),
        price_hint=Decimal("50000"),
        idempotency_key="idem-key-12345678",
        client_order_id="client-order-12345678",
        intent_payload={},
    )


class _StubResponse:
    def __init__(self, payload: dict):
        self._payload = payload
        self.status_code = 200
        self.content = b'{"ok":true}'
        self.text = '{"ok":true}'

    def json(self) -> dict:
        return self._payload


class _StubClient:
    def __init__(self, response: _StubResponse):
        self._response = response

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def post(self, url: str, *, headers: dict, json: dict) -> _StubResponse:
        return self._response


def test_coinbase_client_extracts_fee_and_slippage(monkeypatch):
    payload = {
        "success": True,
        "success_response": {
            "order_id": "venue-order-1",
            "completion_percentage": "100",
            "average_filled_price": "50025",
            "total_fees": "1.25",
        },
    }
    monkeypatch.setattr(
        "oziebot_execution_engine.coinbase_client.build_cdp_jwt",
        lambda **_: "token",
    )
    monkeypatch.setattr(
        httpx,
        "Client",
        lambda timeout: _StubClient(_StubResponse(payload)),
    )

    client = HttpCoinbaseExecutionClient("https://api.coinbase.com")
    submission = client.place_order(
        _request(),
        api_key_name="api-key",
        private_key_pem="-----BEGIN PRIVATE KEY-----\nkey\n-----END PRIVATE KEY-----",
    )

    assert submission.status.value == "filled"
    assert submission.fills[0].fee == Decimal("1.25")
    assert (
        submission.fills[0].raw_payload["execution_quality"]["realized_slippage_pct"]
        == "0.00050000"
    )


def test_coinbase_client_sums_nested_fill_fees(monkeypatch):
    payload = {
        "success": True,
        "success_response": {
            "order_id": "venue-order-2",
            "completion_percentage": "100",
            "average_filled_price": "50010",
            "fills": [{"commission": "0.40"}, {"fee": "0.35"}],
        },
    }
    monkeypatch.setattr(
        "oziebot_execution_engine.coinbase_client.build_cdp_jwt",
        lambda **_: "token",
    )
    monkeypatch.setattr(
        httpx,
        "Client",
        lambda timeout: _StubClient(_StubResponse(payload)),
    )

    client = HttpCoinbaseExecutionClient("https://api.coinbase.com")
    submission = client.place_order(
        _request(),
        api_key_name="api-key",
        private_key_pem="-----BEGIN PRIVATE KEY-----\nkey\n-----END PRIVATE KEY-----",
    )

    assert submission.status.value == "filled"
    assert submission.fills[0].fee == Decimal("0.75")
