from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from oziebot_domain.market_data import (
    NormalizedBestBidAsk,
    NormalizedCandle,
    NormalizedOrderBookTop,
    NormalizedTrade,
)


def _parse_dt(ts: str | int | float | None) -> datetime:
    if ts is None or ts == "":
        return datetime.now(UTC)
    if isinstance(ts, (int, float)):
        return datetime.fromtimestamp(ts, tz=UTC)
    return datetime.fromisoformat(str(ts).replace("Z", "+00:00"))


def normalize_trade(msg: dict) -> NormalizedTrade:
    return NormalizedTrade(
        product_id=msg["product_id"],
        trade_id=str(msg.get("trade_id") or msg.get("sequence") or "0"),
        side=str(msg.get("side", "buy")).lower(),
        price=Decimal(str(msg["price"])),
        size=Decimal(str(msg.get("size") or msg.get("last_size") or "0")),
        event_time=_parse_dt(msg.get("time")),
        ingest_time=datetime.now(UTC),
    )


def normalize_bbo(msg: dict) -> NormalizedBestBidAsk:
    pricebook = msg.get("pricebook") if isinstance(msg.get("pricebook"), dict) else None
    bids = pricebook.get("bids") if pricebook is not None else None
    asks = pricebook.get("asks") if pricebook is not None else None
    best_bid = msg.get("best_bid")
    best_ask = msg.get("best_ask")
    best_bid_size = msg.get("best_bid_size") or msg.get("best_bid_quantity")
    best_ask_size = msg.get("best_ask_size") or msg.get("best_ask_quantity")
    product_id = msg.get("product_id") or (pricebook or {}).get("product_id")
    if pricebook is not None:
        if bids:
            best_bid = bids[0].get("price") or best_bid
            best_bid_size = bids[0].get("size") or best_bid_size
        if asks:
            best_ask = asks[0].get("price") or best_ask
            best_ask_size = asks[0].get("size") or best_ask_size
    return NormalizedBestBidAsk(
        product_id=str(product_id),
        best_bid_price=Decimal(str(best_bid or "0")),
        best_bid_size=Decimal(str(best_bid_size or "0")),
        best_ask_price=Decimal(str(best_ask or "0")),
        best_ask_size=Decimal(str(best_ask_size or "0")),
        event_time=_parse_dt(msg.get("time") or (pricebook or {}).get("time")),
        ingest_time=datetime.now(UTC),
    )


def normalize_candle(msg: dict, granularity_sec: int) -> NormalizedCandle:
    return NormalizedCandle(
        product_id=msg["product_id"],
        granularity_sec=granularity_sec,
        bucket_start=_parse_dt(msg["start"]),
        open=Decimal(str(msg["open"])),
        high=Decimal(str(msg["high"])),
        low=Decimal(str(msg["low"])),
        close=Decimal(str(msg["close"])),
        volume=Decimal(str(msg.get("volume") or "0")),
        event_time=_parse_dt(msg.get("time") or msg.get("start")),
        ingest_time=datetime.now(UTC),
    )


def normalize_orderbook_top(msg: dict, depth: int) -> NormalizedOrderBookTop:
    bids = [
        (Decimal(str(p)), Decimal(str(s))) for p, s in (msg.get("bids") or [])[:depth]
    ]
    asks = [
        (Decimal(str(p)), Decimal(str(s))) for p, s in (msg.get("asks") or [])[:depth]
    ]
    return NormalizedOrderBookTop(
        product_id=msg["product_id"],
        depth=depth,
        bids=bids,
        asks=asks,
        event_time=_parse_dt(msg.get("time")),
        ingest_time=datetime.now(UTC),
    )
