from __future__ import annotations

import httpx

from oziebot_alerts_worker.adapters import SlackAdapter


class _Response:
    def raise_for_status(self) -> None:
        return None


class _CapturingClient:
    calls: list[tuple[str, dict]] = []

    def __init__(self, *args, **kwargs) -> None:
        pass

    def __enter__(self) -> _CapturingClient:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def post(self, url: str, json: dict) -> _Response:
        self.calls.append((url, json))
        return _Response()


def test_slack_adapter_uses_destination_webhook_url(monkeypatch):
    _CapturingClient.calls = []
    monkeypatch.setattr(httpx, "Client", _CapturingClient)

    adapter = SlackAdapter("https://hooks.slack.com/services/global/default")
    adapter.send(
        "https://hooks.slack.com/services/user/custom",
        "[PAPER] Trade opened: BTC-USD via momentum",
        {"symbol": "BTC-USD"},
    )

    assert _CapturingClient.calls == [
        (
            "https://hooks.slack.com/services/user/custom",
            {
                "text": "[PAPER] Trade opened: BTC-USD via momentum",
                "metadata": {"symbol": "BTC-USD"},
            },
        )
    ]


def test_slack_adapter_falls_back_to_global_webhook(monkeypatch):
    _CapturingClient.calls = []
    monkeypatch.setattr(httpx, "Client", _CapturingClient)

    adapter = SlackAdapter("https://hooks.slack.com/services/global/default")
    adapter.send("#desk", "[LIVE] Trade closed: ETH-USD via mean_reversion", {})

    assert _CapturingClient.calls == [
        (
            "https://hooks.slack.com/services/global/default",
            {
                "text": "[LIVE] Trade closed: ETH-USD via mean_reversion\nDestination: #desk",
                "metadata": {},
            },
        )
    ]
