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

    def raise_for_status(self) -> None:
        return None


class _StubClient:
    def __init__(self, response: _StubResponse):
        self._response = response
        self.last_json: dict | None = None
        self.last_request: dict | None = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def post(self, url: str, *, headers: dict, json: dict) -> _StubResponse:
        self.last_json = json
        return self._response

    def request(
        self, method: str, url: str, *, headers: dict, params: dict | None = None
    ) -> _StubResponse:
        self.last_request = {
            "method": method,
            "url": url,
            "headers": headers,
            "params": params,
        }
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


def test_coinbase_client_builds_limit_body_for_maker_preference(monkeypatch):
    payload = {
        "success": True,
        "success_response": {
            "order_id": "venue-order-3",
            "completion_percentage": "0",
        },
    }
    stub_client = _StubClient(_StubResponse(payload))
    monkeypatch.setattr(
        "oziebot_execution_engine.coinbase_client.build_cdp_jwt",
        lambda **_: "token",
    )
    monkeypatch.setattr(httpx, "Client", lambda timeout: stub_client)

    request = _request().model_copy(
        update={
            "order_type": OrderType.LIMIT,
            "execution_preference": "maker_preferred",
            "limit_price_offset_bps": 2,
        }
    )
    client = HttpCoinbaseExecutionClient("https://api.coinbase.com")
    submission = client.place_order(
        request,
        api_key_name="api-key",
        private_key_pem="-----BEGIN PRIVATE KEY-----\nkey\n-----END PRIVATE KEY-----",
    )

    assert submission.status.value == "pending"
    assert stub_client.last_json is not None
    limit_body = stub_client.last_json["order_configuration"]["limit_limit_gtc"]
    assert limit_body["post_only"] is True
    assert limit_body["limit_price"] == "49990.00"


def test_coinbase_client_signs_open_orders_without_query_string(monkeypatch):
    stub_client = _StubClient(_StubResponse({"orders": []}))
    jwt_calls: list[dict] = []

    def _fake_jwt(**kwargs):
        jwt_calls.append(kwargs)
        return "token"

    monkeypatch.setattr(
        "oziebot_execution_engine.coinbase_client.build_cdp_jwt",
        _fake_jwt,
    )
    monkeypatch.setattr(httpx, "Client", lambda timeout: stub_client)

    client = HttpCoinbaseExecutionClient("https://api.coinbase.com")
    result = client.list_open_orders(api_key_name="api-key", private_key_pem="pem")

    assert result == []
    assert jwt_calls == [
        {
            "method": "GET",
            "request_path": "/api/v3/brokerage/orders/historical/batch",
            "host": "api.coinbase.com",
            "api_key_name": "api-key",
            "private_key_pem": "pem",
        }
    ]
    assert stub_client.last_request is not None
    assert stub_client.last_request["params"] == {"order_status": "OPEN"}


def test_coinbase_client_signs_fills_without_query_string(monkeypatch):
    stub_client = _StubClient(_StubResponse({"fills": []}))
    jwt_calls: list[dict] = []

    def _fake_jwt(**kwargs):
        jwt_calls.append(kwargs)
        return "token"

    monkeypatch.setattr(
        "oziebot_execution_engine.coinbase_client.build_cdp_jwt",
        _fake_jwt,
    )
    monkeypatch.setattr(httpx, "Client", lambda timeout: stub_client)

    client = HttpCoinbaseExecutionClient("https://api.coinbase.com")
    result = client.list_fills(
        api_key_name="api-key",
        private_key_pem="pem",
        product_id="BTC-USD",
    )

    assert result == []
    assert jwt_calls == [
        {
            "method": "GET",
            "request_path": "/api/v3/brokerage/orders/historical/fills",
            "host": "api.coinbase.com",
            "api_key_name": "api-key",
            "private_key_pem": "pem",
        }
    ]
    assert stub_client.last_request is not None
    assert stub_client.last_request["params"] == {"product_id": "BTC-USD"}
