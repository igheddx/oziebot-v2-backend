from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from oziebot_market_data_ingestor import __main__ as ingestor_main


@pytest.mark.anyio
async def test_seed_market_cache_refreshes_trades_and_bbo_after_candles(monkeypatch):
    calls: list[str] = []

    async def fake_candles(*args, **kwargs):  # noqa: ARG001
        calls.append("candles")

    async def fake_trades(*args, **kwargs):  # noqa: ARG001
        calls.append("trades")

    async def fake_bbo(*args, **kwargs):  # noqa: ARG001
        calls.append("bbo")

    monkeypatch.setattr(ingestor_main, "_reconcile_candles", fake_candles)
    monkeypatch.setattr(ingestor_main, "_reconcile_trades", fake_trades)
    monkeypatch.setattr(ingestor_main, "_reconcile_bbo", fake_bbo)

    touches: list[str] = []
    health = SimpleNamespace(touch=lambda: touches.append("touch"))

    await ingestor_main._seed_market_cache(
        rest=object(),  # type: ignore[arg-type]
        store=object(),  # type: ignore[arg-type]
        cache=object(),  # type: ignore[arg-type]
        log_client=object(),
        stale=object(),  # type: ignore[arg-type]
        products=["BTC-USD"],
        trade_limit=25,
        granularity_sec=60,
        signal_panel=object(),  # type: ignore[arg-type]
        health=health,
    )

    assert calls == ["candles", "trades", "bbo"]
    assert touches == ["touch", "touch", "touch"]


def test_refresh_targets_prefers_stale_subset() -> None:
    assert ingestor_main._refresh_targets(["BTC-USD"], ["BTC-USD", "ETH-USD"]) == [
        "BTC-USD"
    ]


def test_refresh_targets_uses_all_products_when_none_are_stale() -> None:
    assert ingestor_main._refresh_targets([], ["BTC-USD", "ETH-USD"]) == [
        "BTC-USD",
        "ETH-USD",
    ]


def test_refresh_product_universe_returns_delta_and_prunes_removed_symbols() -> None:
    stale = SimpleNamespace(pruned=None)

    def _prune(products):
        stale.pruned = list(products)

    stale.prune = _prune
    universe = SimpleNamespace(list_active_product_ids=lambda: ["ETH-USD", "SOL-USD"])

    delta = ingestor_main._refresh_product_universe(
        universe, stale, ["BTC-USD", "ETH-USD"]
    )

    assert delta == ingestor_main.ProductUniverseChange(
        products=["ETH-USD", "SOL-USD"],
        added=["SOL-USD"],
        removed=["BTC-USD"],
    )
    assert stale.pruned == ["ETH-USD", "SOL-USD"]


@pytest.mark.anyio
async def test_reconcile_bbo_refreshes_products_concurrently() -> None:
    active = 0
    max_active = 0

    class FakeRest:
        async def get_ticker(self, product_id: str) -> dict[str, str]:
            nonlocal active, max_active
            active += 1
            max_active = max(max_active, active)
            await asyncio.sleep(0.01)
            active -= 1
            return {
                "product_id": product_id,
                "best_bid": "100",
                "best_bid_quantity": "1",
                "best_ask": "101",
                "best_ask_quantity": "1",
                "time": "2026-01-01T00:00:00+00:00",
            }

    seen: list[str] = []
    cache = SimpleNamespace(put_bbo=lambda item: seen.append(item.product_id))
    store = SimpleNamespace(insert_bbo_snapshot=lambda item: None)
    stale = SimpleNamespace(mark_bbo=lambda product_id, at: None)

    await ingestor_main._reconcile_bbo(
        FakeRest(),
        store,
        cache,
        object(),
        stale,
        ["BTC-USD", "ETH-USD", "SOL-USD"],
        max_concurrency=3,
    )

    assert sorted(seen) == ["BTC-USD", "ETH-USD", "SOL-USD"]
    assert max_active > 1
