from __future__ import annotations

from datetime import UTC, datetime


def simulated_coinbase_messages(product_id: str) -> list[dict]:
    """Generate deterministic sample messages for local simulation/testing."""
    now = datetime.now(UTC).isoformat()
    return [
        {
            "type": "match",
            "product_id": product_id,
            "trade_id": 1,
            "side": "buy",
            "price": "50000.00",
            "size": "0.010",
            "time": now,
        },
        {
            "type": "ticker",
            "product_id": product_id,
            "best_bid": "49999.50",
            "best_bid_size": "1.25",
            "best_ask": "50000.50",
            "best_ask_size": "0.80",
            "time": now,
        },
        {
            "type": "snapshot",
            "product_id": product_id,
            "bids": [["49999.00", "1.00"], ["49998.50", "2.00"]],
            "asks": [["50001.00", "1.10"], ["50002.00", "1.20"]],
            "time": now,
        },
    ]
