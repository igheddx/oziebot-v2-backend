from decimal import Decimal

from oziebot_market_data_ingestor.normalizer import (
    normalize_bbo,
    normalize_orderbook_top,
    normalize_trade,
)


def test_normalize_trade_basic_fields():
    msg = {
        "type": "match",
        "product_id": "BTC-USD",
        "trade_id": 123,
        "side": "buy",
        "price": "50000.12",
        "size": "0.01",
        "time": "2026-04-12T10:00:00Z",
    }
    out = normalize_trade(msg)
    assert out.product_id == "BTC-USD"
    assert out.trade_id == "123"
    assert out.price == Decimal("50000.12")


def test_normalize_bbo_basic_fields():
    msg = {
        "type": "ticker",
        "product_id": "ETH-USD",
        "best_bid": "2999.50",
        "best_bid_size": "1.2",
        "best_ask": "3000.10",
        "best_ask_size": "1.1",
        "time": "2026-04-12T10:00:00Z",
    }
    out = normalize_bbo(msg)
    assert out.product_id == "ETH-USD"
    assert out.best_bid_price == Decimal("2999.50")
    assert out.best_ask_price == Decimal("3000.10")


def test_normalize_orderbook_top_depth_limit():
    msg = {
        "type": "snapshot",
        "product_id": "SOL-USD",
        "bids": [["100", "2"], ["99", "1"], ["98", "1"]],
        "asks": [["101", "2"], ["102", "1"], ["103", "1"]],
        "time": "2026-04-12T10:00:00Z",
    }
    out = normalize_orderbook_top(msg, depth=2)
    assert len(out.bids) == 2
    assert len(out.asks) == 2
