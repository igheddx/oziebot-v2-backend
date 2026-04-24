from __future__ import annotations

import json

from oziebot_common.runtime_status import (
    publish_runtime_status,
    read_runtime_statuses,
    runtime_status_key,
)


class FakeRedis:
    def __init__(self) -> None:
        self.values: dict[str, str] = {}
        self.expiry: dict[str, int] = {}

    def set(self, key: str, value: str, ex: int) -> None:
        self.values[key] = value
        self.expiry[key] = ex

    def mget(self, keys: list[str]) -> list[str | None]:
        return [self.values.get(key) for key in keys]


def test_publish_runtime_status_persists_json_payload() -> None:
    client = FakeRedis()

    publish_runtime_status(
        client,
        {
            "service": "strategy-engine",
            "status": "ok",
            "ready": True,
        },
        ttl_seconds=25,
    )

    raw_payload = client.values[runtime_status_key("strategy-engine")]
    assert json.loads(raw_payload)["status"] == "ok"
    assert client.expiry[runtime_status_key("strategy-engine")] == 25


def test_read_runtime_statuses_returns_named_snapshots() -> None:
    client = FakeRedis()
    client.values[runtime_status_key("risk-engine")] = json.dumps(
        {"service": "risk-engine", "status": "degraded", "ready": False}
    )

    snapshots = read_runtime_statuses(client, ["risk-engine", "execution-engine"])

    assert snapshots["risk-engine"]["status"] == "degraded"
    assert "execution-engine" not in snapshots
