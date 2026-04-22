from oziebot_market_data_ingestor.coinbase_client import _flatten_ws_message


def test_flatten_ws_message_flattens_market_trades_payload():
    flattened = _flatten_ws_message(
        {
            "channel": "market_trades",
            "timestamp": "2026-04-22T02:00:01Z",
            "events": [
                {
                    "type": "update",
                    "trades": [
                        {
                            "product_id": "BTC-USD",
                            "trade_id": "1",
                            "price": "76331.97",
                            "size": "0.000015",
                            "time": "2026-04-22T02:00:01.319381Z",
                            "side": "SELL",
                        }
                    ],
                }
            ],
        }
    )
    assert flattened == [
        {
            "type": "match",
            "product_id": "BTC-USD",
            "trade_id": "1",
            "side": "SELL",
            "price": "76331.97",
            "size": "0.000015",
            "time": "2026-04-22T02:00:01.319381Z",
        }
    ]


def test_flatten_ws_message_flattens_ticker_payload():
    flattened = _flatten_ws_message(
        {
            "channel": "ticker",
            "timestamp": "2026-04-22T02:00:01Z",
            "events": [
                {
                    "type": "update",
                    "tickers": [
                        {
                            "product_id": "BTC-USD",
                            "best_bid": "76331.96",
                            "best_bid_quantity": "0.54",
                            "best_ask": "76331.97",
                            "best_ask_quantity": "0.000015",
                        }
                    ],
                }
            ],
        }
    )
    assert flattened == [
        {
            "type": "ticker",
            "product_id": "BTC-USD",
            "best_bid": "76331.96",
            "best_bid_quantity": "0.54",
            "best_ask": "76331.97",
            "best_ask_quantity": "0.000015",
            "time": "2026-04-22T02:00:01Z",
        }
    ]


def test_flatten_ws_message_flattens_level2_payload():
    flattened = _flatten_ws_message(
        {
            "channel": "level2",
            "timestamp": "2026-04-22T02:00:01Z",
            "events": [
                {
                    "type": "snapshot",
                    "product_id": "BTC-USD",
                    "updates": [
                        {
                            "side": "bid",
                            "price_level": "76331.96",
                            "new_quantity": "0.54",
                        },
                        {
                            "side": "offer",
                            "price_level": "76331.97",
                            "new_quantity": "0.000015",
                        },
                    ],
                }
            ],
        }
    )
    assert flattened == [
        {
            "type": "snapshot",
            "product_id": "BTC-USD",
            "bids": [["76331.96", "0.54"]],
            "asks": [["76331.97", "0.000015"]],
            "time": "2026-04-22T02:00:01Z",
        }
    ]
