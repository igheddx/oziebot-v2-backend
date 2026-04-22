from __future__ import annotations

from datetime import UTC, datetime


def simulated_coinbase_messages(product_id: str) -> list[dict]:
    """Generate deterministic sample messages for local simulation/testing."""
    now = datetime.now(UTC).isoformat()
    return [
        {
            "channel": "market_trades",
            "timestamp": now,
            "events": [
                {
                    "type": "snapshot",
                    "trades": [
                        {
                            "product_id": product_id,
                            "trade_id": 1,
                            "side": "BUY",
                            "price": "50000.00",
                            "size": "0.010",
                            "time": now,
                        }
                    ],
                }
            ],
        },
        {
            "channel": "ticker",
            "timestamp": now,
            "events": [
                {
                    "type": "snapshot",
                    "tickers": [
                        {
                            "type": "ticker",
                            "product_id": product_id,
                            "best_bid": "49999.50",
                            "best_bid_quantity": "1.25",
                            "best_ask": "50000.50",
                            "best_ask_quantity": "0.80",
                            "price": "50000.00",
                        }
                    ],
                }
            ],
        },
        {
            "channel": "level2",
            "timestamp": now,
            "events": [
                {
                    "type": "snapshot",
                    "product_id": product_id,
                    "updates": [
                        {
                            "side": "bid",
                            "price_level": "49999.00",
                            "new_quantity": "1.00",
                            "event_time": now,
                        },
                        {
                            "side": "offer",
                            "price_level": "50001.00",
                            "new_quantity": "1.10",
                            "event_time": now,
                        },
                    ],
                }
            ],
        },
    ]
