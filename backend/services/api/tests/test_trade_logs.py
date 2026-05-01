from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import patch

from sqlalchemy.orm import Session

from oziebot_api.models.market_data import MarketDataBboSnapshot, MarketDataTradeSnapshot
from oziebot_common.trade_log import append_trade_log_event
from oziebot_common.trade_log_intelligence import write_trade_log_summary


def test_trade_log_endpoint_returns_recent_events(
    client, regular_user_and_token, db_session: Session
):
    _, token = regular_user_and_token
    bind = db_session.get_bind()

    now = datetime.now(UTC)
    append_trade_log_event(
        bind,
        symbol="BTC-USD",
        event_type="market_snapshot",
        message="BTC-USD market snapshot pulled",
        timestamp=now - timedelta(seconds=30),
    )
    append_trade_log_event(
        bind,
        symbol="ETH-USD",
        event_type="bbo_update",
        message="ETH-USD BBO updated",
        timestamp=now - timedelta(seconds=5),
        details={
            "best_bid": Decimal("2450.10"),
            "best_ask": Decimal("2451.25"),
            "spread_pct": Decimal("0.0469"),
        },
    )
    write_trade_log_summary(
        bind,
        symbol="ETH-USD",
        summary={
            "timestamp": now.isoformat(),
            "symbol": "ETH-USD",
            "summary_line": "Trend: UP | Volatility: MEDIUM | Liquidity: HIGH | Bias: BUY",
            "market_state": {
                "trend": "UP",
                "volatility": "MEDIUM",
                "liquidity": "HIGH",
                "trade_bias": "BUY",
            },
            "signal_quality_score": 78,
            "signal_quality_label": "HIGH",
            "raw_metrics": {"spread_pct": "0.0469"},
        },
    )

    response = client.get(
        "/v1/logs/trade?window_seconds=120&limit=200&symbol=ETH-USD&event_type=bbo_update",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["count"] == 1
    assert payload["symbol"] == "ETH-USD"
    assert payload["event_type"] == "bbo_update"
    assert payload["events"][0]["message"] == "ETH-USD BBO updated"
    assert payload["events"][0]["source"] == "coinbase"
    assert payload["events"][0]["details"] == {
        "best_bid": "2450.1",
        "best_ask": "2451.25",
        "spread_pct": "0.0469",
    }
    assert payload["available_symbols"] == ["ETH-USD"]
    assert payload["available_event_types"] == ["bbo_update"]
    assert payload["summaries"][0]["signal_quality_score"] == 78


@patch("oziebot_api.api.v1.logs.read_trade_log_events")
def test_trade_log_endpoint_returns_503_when_store_read_fails(
    mock_read, client, regular_user_and_token
):
    mock_read.side_effect = RuntimeError("db down")
    _, token = regular_user_and_token

    response = client.get(
        "/v1/logs/trade?window_seconds=120&limit=200",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == 503, response.text
    assert response.json()["detail"] == "Trade log temporarily unavailable"


def test_trade_log_endpoint_falls_back_to_db_market_data_when_trade_log_window_empty(
    client, regular_user_and_token, db_session: Session
):
    _, token = regular_user_and_token
    now = datetime.now(UTC)

    db_session.add(
        MarketDataBboSnapshot(
            id=uuid.uuid4(),
            source="coinbase",
            product_id="BTC-USD",
            best_bid_price=64250.10,
            best_bid_size=1.25,
            best_ask_price=64252.35,
            best_ask_size=1.10,
            event_time=now - timedelta(seconds=15),
            ingest_time=now - timedelta(seconds=15),
        )
    )
    db_session.add(
        MarketDataTradeSnapshot(
            id=uuid.uuid4(),
            source="coinbase",
            product_id="BTC-USD",
            trade_id="trade-fallback-1",
            side="buy",
            price=64251.40,
            size=0.12,
            event_time=now - timedelta(seconds=10),
            ingest_time=now - timedelta(seconds=10),
        )
    )
    db_session.commit()

    response = client.get(
        "/v1/logs/trade?window_seconds=120&limit=50&symbol=BTC-USD",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["symbol"] == "BTC-USD"
    assert payload["count"] == 2
    assert payload["available_symbols"] == ["BTC-USD"]
    assert set(payload["available_event_types"]) == {"bbo_update", "trade_tick"}
    assert {event["event_type"] for event in payload["events"]} == {"bbo_update", "trade_tick"}
    assert payload["summaries"][0]["symbol"] == "BTC-USD"
