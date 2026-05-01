from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from oziebot_common.trade_log import build_trade_log_event
from oziebot_market_data_ingestor.normalizer import normalize_bbo, normalize_trade
from oziebot_market_data_ingestor.signal_panel import SignalPanelEmitter


@pytest.fixture
def patched_trade_pipeline(monkeypatch):
    samples: list[dict] = []
    summaries: dict[str, dict] = {}
    events: list[dict] = []

    def append_sample(_engine, *, symbol, sample, timestamp=None, retention_seconds=60):  # noqa: ARG001
        t = (timestamp or datetime.now(UTC)).astimezone(UTC)
        row = {
            "timestamp": t.isoformat(),
            "symbol": str(symbol).upper(),
            "sample": dict(sample),
        }
        samples.append(row)
        return row

    def read_samples(_engine, *, symbol, window_seconds=60, now=None):  # noqa: ARG001
        _ = window_seconds
        sym = str(symbol).upper()
        return [r for r in samples if r["symbol"] == sym]

    def write_summary(_engine, *, symbol, summary, retention_seconds=60):  # noqa: ARG001
        sym = str(symbol).upper()
        summaries[sym] = dict(summary)
        return summaries[sym]

    def append_event(_engine, **kwargs):  # noqa: ARG001
        ev = build_trade_log_event(**kwargs)
        events.append(ev)
        return ev

    def read_summaries(_engine, *, symbol=None):  # noqa: ARG001
        if symbol:
            s = summaries.get(str(symbol).upper())
            return [s] if s else []
        return sorted(
            summaries.values(),
            key=lambda item: (
                -int(item.get("signal_quality_score") or 0),
                str(item.get("symbol") or ""),
            ),
        )

    def read_events(
        _engine,
        *,
        window_seconds=120,
        limit=200,
        symbol=None,
        event_type=None,
        now=None,
    ):  # noqa: ARG001
        current = (now or datetime.now(UTC)).astimezone(UTC)
        cutoff = current - timedelta(seconds=window_seconds)
        out: list[dict] = []
        for ev in events:
            ts = datetime.fromisoformat(str(ev["timestamp"]).replace("Z", "+00:00"))
            if ts < cutoff:
                continue
            if symbol and str(ev.get("symbol") or "").upper() != str(symbol).upper():
                continue
            if (
                event_type
                and str(ev.get("event_type") or "").lower() != str(event_type).lower()
            ):
                continue
            out.append(ev)
        return out[-limit:]

    monkeypatch.setattr(
        "oziebot_market_data_ingestor.signal_panel.append_trade_log_sample",
        append_sample,
    )
    monkeypatch.setattr(
        "oziebot_market_data_ingestor.signal_panel.read_trade_log_samples",
        read_samples,
    )
    monkeypatch.setattr(
        "oziebot_market_data_ingestor.signal_panel.write_trade_log_summary",
        write_summary,
    )
    monkeypatch.setattr(
        "oziebot_market_data_ingestor.signal_panel.append_trade_log_event",
        append_event,
    )

    monkeypatch.setattr(
        "oziebot_common.trade_log.read_trade_log_events",
        read_events,
    )
    monkeypatch.setattr(
        "oziebot_common.trade_log_intelligence.read_trade_log_summaries",
        read_summaries,
    )

    return events


def test_signal_panel_emits_summary_and_market_snapshot_events(
    patched_trade_pipeline,
) -> None:  # noqa: ARG001
    emitter = SignalPanelEmitter(engine=None)
    start = datetime.now(UTC)

    first_bbo = normalize_bbo(
        {
            "product_id": "BTC-USD",
            "best_bid": "64000",
            "best_bid_size": "2.4",
            "best_ask": "64004",
            "best_ask_size": "2.2",
            "time": start.isoformat(),
        }
    )
    first_trade = normalize_trade(
        {
            "product_id": "BTC-USD",
            "trade_id": "1",
            "side": "buy",
            "price": "64003",
            "size": "0.8",
            "time": start.isoformat(),
        }
    )
    second_trade_at = start + timedelta(seconds=3)
    second_trade = normalize_trade(
        {
            "product_id": "BTC-USD",
            "trade_id": "2",
            "side": "buy",
            "price": "64060",
            "size": "1.2",
            "time": second_trade_at.isoformat(),
        }
    )

    emitter.observe_bbo(first_bbo)
    emitter.observe_trade(first_trade)
    emitter.observe_trade(second_trade)
    emitter.force_emit("BTC-USD", now=second_trade_at)

    from oziebot_common.trade_log import read_trade_log_events
    from oziebot_common.trade_log_intelligence import read_trade_log_summaries

    summaries = read_trade_log_summaries(None, symbol=None)
    assert len(summaries) == 1
    assert summaries[0]["symbol"] == "BTC-USD"
    assert summaries[0]["market_state"]["trend"] in {"UP", "FLAT"}

    events = read_trade_log_events(
        None,
        now=second_trade_at,
        window_seconds=120,
        limit=20,
    )
    event_types = [event["event_type"] for event in events]
    assert "market_snapshot" in event_types
    assert any("MARKET SNAPSHOT" in event["message"] for event in events)
