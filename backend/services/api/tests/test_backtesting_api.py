from __future__ import annotations

import json
from pathlib import Path


def _auth(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def _sample_payload() -> dict:
    fixture = Path(__file__).parent / "fixtures" / "backtest_sample_dataset.json"
    raw = json.loads(fixture.read_text())
    return {
        "strategy_id": "momentum",
        "trading_mode": "paper",
        "dataset_name": raw["dataset_name"],
        "timeframe": raw["timeframe"],
        "candles": raw["candles"],
        "config": {
            "initial_capital_cents": 200000,
            "benchmark_mode": True,
            "benchmark_namespace": "regression-v1",
            "entry_threshold_bps": 15,
            "take_profit_bps": 60,
            "stop_loss_bps": 80,
            "max_holding_bars": 3,
            "fee_bps": 8,
            "slippage_bps": 4,
            "per_trade_notional_cents": 20000,
        },
    }


def test_backtest_run_and_history_endpoints(client, regular_user_and_token):
    _, token = regular_user_and_token

    run = client.post(
        "/v1/me/backtests/run",
        headers=_auth(token),
        json=_sample_payload(),
    )
    assert run.status_code == 200, run.text
    payload = run.json()
    assert payload["strategy_id"] == "momentum"
    assert payload["trading_mode"] == "paper"
    assert payload["status"] == "completed"
    assert payload["benchmark_mode"] is True
    assert payload["deterministic_fingerprint"] is not None
    assert payload["summary_json"]["checksum"]
    assert payload["summary_json"]["total_trades"] >= 1
    assert len(payload["snapshots"]) >= 3  # user + strategy + at least one token
    assert len(payload["analytics_artifacts"]) >= 2

    run2 = client.post(
        "/v1/me/backtests/run",
        headers=_auth(token),
        json=_sample_payload(),
    )
    assert run2.status_code == 200, run2.text
    payload2 = run2.json()
    assert payload2["id"] == payload["id"]
    assert payload2["deterministic_fingerprint"] == payload["deterministic_fingerprint"]

    history = client.get("/v1/me/backtests/history", headers=_auth(token))
    assert history.status_code == 200, history.text
    h = history.json()
    assert h["total"] >= 1
    assert h["runs"][0]["id"] == payload["id"]


def test_backtest_performance_and_analytics_history_filters(client, regular_user_and_token):
    _, token = regular_user_and_token
    run = client.post("/v1/me/backtests/run", headers=_auth(token), json=_sample_payload())
    assert run.status_code == 200, run.text

    perf = client.get(
        "/v1/me/backtests/performance/history",
        headers=_auth(token),
        params={"strategy_id": "momentum", "scope": "token"},
    )
    assert perf.status_code == 200, perf.text
    p = perf.json()
    assert p["total"] >= 1
    assert all(row["scope"] == "token" for row in p["snapshots"])

    analytics = client.get(
        "/v1/me/backtests/analytics/history",
        headers=_auth(token),
        params={"strategy_id": "momentum"},
    )
    assert analytics.status_code == 200, analytics.text
    a = analytics.json()
    assert a["total"] >= 1
    assert all(row["strategy_id"] == "momentum" for row in a["artifacts"])

    perf_csv = client.get(
        "/v1/me/backtests/performance/history.csv",
        headers=_auth(token),
        params={"strategy_id": "momentum"},
    )
    assert perf_csv.status_code == 200, perf_csv.text
    assert "text/csv" in perf_csv.headers["content-type"]
    assert "avg_return_bps" in perf_csv.text

    analytics_csv = client.get(
        "/v1/me/backtests/analytics/history.csv",
        headers=_auth(token),
        params={"strategy_id": "momentum"},
    )
    assert analytics_csv.status_code == 200, analytics_csv.text
    assert "text/csv" in analytics_csv.headers["content-type"]
    assert "feature_vector_json" in analytics_csv.text
