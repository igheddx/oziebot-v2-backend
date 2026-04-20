from __future__ import annotations

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
