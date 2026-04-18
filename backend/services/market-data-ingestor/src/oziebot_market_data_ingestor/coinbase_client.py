from __future__ import annotations

import asyncio
import json
import ssl
from collections.abc import AsyncIterator
from datetime import UTC, datetime

import certifi
import httpx
import websockets


class CoinbaseRestClient:
    """REST fallback/recovery client for candles and simple snapshots."""

    def __init__(self, base_url: str):
        self._base = base_url.rstrip("/")
        self._client = httpx.AsyncClient(timeout=15.0)

    async def close(self) -> None:
        await self._client.aclose()

    async def get_candles(self, product_id: str, granularity_sec: int, limit: int = 50) -> list[dict]:
        resp = await self._client.get(
            f"{self._base}/products/{product_id}/candles",
            params={"granularity": granularity_sec, "limit": limit},
        )
        resp.raise_for_status()
        rows = resp.json()
        out: list[dict] = []
        for r in rows:
            out.append(
                {
                    "product_id": product_id,
                    "start": r[0],  # Unix timestamp int
                    "low": str(r[1]),
                    "high": str(r[2]),
                    "open": str(r[3]),
                    "close": str(r[4]),
                    "volume": str(r[5]),
                }
            )
        return out

    async def get_recent_trades(self, product_id: str, limit: int = 20) -> list[dict]:
        resp = await self._client.get(f"{self._base}/products/{product_id}/trades")
        resp.raise_for_status()
        rows = resp.json()
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
        resp = await self._client.get(f"{self._base}/products/{product_id}/ticker")
        resp.raise_for_status()
        row = resp.json()
        return {
            "product_id": product_id,
            "best_bid": str(row.get("bid") or "0"),
            "best_bid_size": str(row.get("bid_size") or "0"),
            "best_ask": str(row.get("ask") or "0"),
            "best_ask_size": str(row.get("ask_size") or "0"),
            "time": datetime.now(UTC).isoformat(),
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
                async with websockets.connect(self._url, ssl=ssl_ctx, ping_interval=20, ping_timeout=20) as ws:
                    payload = {
                        "type": "subscribe",
                        "product_ids": product_ids,
                        "channels": channels,
                    }
                    await ws.send(json.dumps(payload))
                    backoff = 1.0
                    while True:
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=1.0)
                        except asyncio.TimeoutError:
                            yield {"type": "__tick__"}
                            continue
                        msg = json.loads(raw)
                        if isinstance(msg, dict):
                            yield msg
            except Exception:
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30.0)
