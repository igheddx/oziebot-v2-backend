from __future__ import annotations

import asyncio
import json
import logging
import ssl
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from typing import Any

import certifi
import httpx
import websockets

log = logging.getLogger("market-data-ingestor.coinbase")

GRANULARITY_MAP = {
    60: "ONE_MINUTE",
    300: "FIVE_MINUTE",
    900: "FIFTEEN_MINUTE",
    1800: "THIRTY_MINUTE",
    3600: "ONE_HOUR",
    7200: "TWO_HOUR",
    14400: "FOUR_HOUR",
    21600: "SIX_HOUR",
    86400: "ONE_DAY",
}


def _flatten_ws_message(msg: dict[str, Any]) -> list[dict[str, Any]]:
    channel = str(msg.get("channel") or "")
    timestamp = msg.get("timestamp")
    events = msg.get("events") or []
    if channel in {"subscriptions", "heartbeats"}:
        return []

    flattened: list[dict[str, Any]] = []
    for event in events:
        if not isinstance(event, dict):
            continue
        if channel == "market_trades":
            for trade in event.get("trades") or []:
                if not isinstance(trade, dict):
                    continue
                flattened.append(
                    {
                        "type": "match",
                        "product_id": trade.get("product_id"),
                        "trade_id": trade.get("trade_id"),
                        "side": trade.get("side"),
                        "price": trade.get("price"),
                        "size": trade.get("size"),
                        "time": trade.get("time") or timestamp,
                    }
                )
        elif channel == "ticker":
            for ticker in event.get("tickers") or []:
                if not isinstance(ticker, dict):
                    continue
                flattened.append(
                    {
                        "type": "ticker",
                        "product_id": ticker.get("product_id"),
                        "best_bid": ticker.get("best_bid"),
                        "best_bid_quantity": ticker.get("best_bid_quantity"),
                        "best_ask": ticker.get("best_ask"),
                        "best_ask_quantity": ticker.get("best_ask_quantity"),
                        "time": timestamp,
                    }
                )
        elif channel in {"level2", "l2_data"}:
            updates = event.get("updates") or []
            bids = [
                [
                    str(update.get("price_level") or "0"),
                    str(update.get("new_quantity") or "0"),
                ]
                for update in updates
                if isinstance(update, dict)
                and str(update.get("side") or "").lower() == "bid"
            ]
            asks = [
                [
                    str(update.get("price_level") or "0"),
                    str(update.get("new_quantity") or "0"),
                ]
                for update in updates
                if isinstance(update, dict)
                and str(update.get("side") or "").lower() == "offer"
            ]
            flattened.append(
                {
                    "type": str(event.get("type") or "snapshot"),
                    "product_id": event.get("product_id"),
                    "bids": bids,
                    "asks": asks,
                    "time": timestamp,
                }
            )
        elif channel == "candles":
            for candle in event.get("candles") or []:
                if not isinstance(candle, dict):
                    continue
                flattened.append(
                    {
                        "type": "candle",
                        "product_id": candle.get("product_id"),
                        "start": candle.get("start"),
                        "high": candle.get("high"),
                        "low": candle.get("low"),
                        "open": candle.get("open"),
                        "close": candle.get("close"),
                        "volume": candle.get("volume"),
                        "time": timestamp,
                    }
                )
    return flattened


class CoinbaseRestClient:
    """REST fallback/recovery client for candles and simple snapshots."""

    def __init__(self, base_url: str):
        self._base = base_url.rstrip("/")
        self._client = httpx.AsyncClient(timeout=15.0)

    async def close(self) -> None:
        await self._client.aclose()

    async def get_candles(
        self, product_id: str, granularity_sec: int, limit: int = 50
    ) -> list[dict]:
        granularity = GRANULARITY_MAP.get(granularity_sec)
        if granularity is None:
            raise ValueError(f"Unsupported candle granularity: {granularity_sec}")
        end = int(datetime.now(UTC).timestamp())
        start = end - (granularity_sec * max(limit, 1))
        resp = await self._client.get(
            f"{self._base}/market/products/{product_id}/candles",
            params={
                "start": start,
                "end": end,
                "granularity": granularity,
                "limit": limit,
            },
        )
        resp.raise_for_status()
        rows = resp.json().get("candles") or []
        out: list[dict] = []
        for r in rows:
            out.append(
                {
                    "product_id": product_id,
                    "start": r["start"],
                    "low": str(r["low"]),
                    "high": str(r["high"]),
                    "open": str(r["open"]),
                    "close": str(r["close"]),
                    "volume": str(r["volume"]),
                }
            )
        return out

    async def get_recent_trades(self, product_id: str, limit: int = 20) -> list[dict]:
        resp = await self._client.get(
            f"{self._base}/market/products/{product_id}/ticker",
            params={"limit": limit},
        )
        resp.raise_for_status()
        rows = resp.json().get("trades") or []
        out: list[dict] = []
        for r in rows[:limit]:
            out.append(
                {
                    "product_id": product_id,
                    "trade_id": str(r.get("trade_id") or "0"),
                    "side": str(r.get("side") or "buy").lower(),
                    "price": str(r.get("price") or "0"),
                    "size": str(r.get("size") or "0"),
                    "time": str(r.get("time") or datetime.now(UTC).isoformat()),
                }
            )
        return out

    async def get_ticker(self, product_id: str) -> dict:
        resp = await self._client.get(
            f"{self._base}/market/product_book",
            params={"product_id": product_id, "limit": 1},
        )
        resp.raise_for_status()
        row = resp.json()
        return {
            "product_id": product_id,
            "pricebook": row.get("pricebook") or {},
            "time": ((row.get("pricebook") or {}).get("time"))
            or datetime.now(UTC).isoformat(),
        }


class CoinbaseWsClient:
    """WebSocket abstraction with reconnect-friendly message iteration."""

    def __init__(self, ws_url: str):
        self._url = ws_url

    async def subscribe_and_stream(
        self,
        product_ids: list[str],
        channels: list[str],
    ) -> AsyncIterator[dict]:
        ssl_ctx = ssl.create_default_context(cafile=certifi.where())
        backoff = 1.0
        while True:
            try:
                log.info(
                    "coinbase_ws_connecting url=%s product_count=%s channels=%s",
                    self._url,
                    len(product_ids),
                    channels,
                )
                async with websockets.connect(
                    self._url,
                    ssl=ssl_ctx,
                    ping_interval=20,
                    ping_timeout=20,
                    max_size=None,
                ) as ws:
                    for channel in channels:
                        payload = {"type": "subscribe", "channel": channel}
                        if channel != "heartbeats":
                            payload["product_ids"] = product_ids
                        await ws.send(json.dumps(payload))
                    log.info(
                        "coinbase_ws_subscribed url=%s product_count=%s channels=%s",
                        self._url,
                        len(product_ids),
                        channels,
                    )
                    backoff = 1.0
                    while True:
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=1.0)
                        except asyncio.TimeoutError:
                            yield {"type": "__tick__"}
                            continue
                        msg = json.loads(raw)
                        if isinstance(msg, dict):
                            flattened = _flatten_ws_message(msg)
                            if not flattened:
                                continue
                            for item in flattened:
                                yield item
            except Exception as exc:
                log.warning(
                    "coinbase_ws_stream_failed url=%s err=%s reconnect_backoff_seconds=%.1f",
                    self._url,
                    exc,
                    backoff,
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30.0)
