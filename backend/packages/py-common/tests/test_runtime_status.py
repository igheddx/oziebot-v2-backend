from __future__ import annotations

import pytest
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool

import oziebot_common.runtime_status as runtime_status_module
from oziebot_common.runtime_status import publish_runtime_status, read_runtime_statuses
from oziebot_common.sqlite_aux_schema import ensure_sqlite_aux_schema


class FakeObservabilityStore:
    def __init__(self) -> None:
        self.snapshots: dict[str, dict[str, object]] = {}

    def publish_runtime_status(self, snapshot: dict[str, object]) -> None:
        self.snapshots[str(snapshot["service"])] = snapshot

    def read_runtime_statuses(
        self,
        service_names: list[str],
    ) -> dict[str, dict[str, object]]:
        return {
            name: self.snapshots[name]
            for name in service_names
            if name in self.snapshots
        }


@pytest.fixture
def sqlite_engine():
    eng = create_engine(
        "sqlite+pysqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    ensure_sqlite_aux_schema(eng)
    return eng


def test_publish_runtime_status_persists_json_payload(sqlite_engine) -> None:
    publish_runtime_status(
        sqlite_engine,
        {
            "service": "strategy-engine",
            "status": "ok",
            "ready": True,
        },
        ttl_seconds=25,
    )

    snapshots = read_runtime_statuses(sqlite_engine, ["strategy-engine"])
    assert snapshots["strategy-engine"]["status"] == "ok"


def test_read_runtime_statuses_returns_named_snapshots(sqlite_engine) -> None:
    publish_runtime_status(
        sqlite_engine,
        {
            "service": "risk-engine",
            "status": "degraded",
            "ready": False,
        },
        ttl_seconds=60,
    )

    snapshots = read_runtime_statuses(
        sqlite_engine, ["risk-engine", "execution-engine"]
    )

    assert snapshots["risk-engine"]["status"] == "degraded"
    assert "execution-engine" not in snapshots


def test_runtime_status_can_use_s3_observability_store(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = FakeObservabilityStore()
    monkeypatch.setattr(
        runtime_status_module,
        "get_observability_store",
        lambda: store,
    )

    publish_runtime_status(
        None,
        {
            "service": "strategy-engine",
            "status": "ok",
            "ready": True,
        },
        ttl_seconds=25,
    )

    snapshots = read_runtime_statuses(None, ["strategy-engine", "risk-engine"])

    assert snapshots["strategy-engine"]["status"] == "ok"
    assert "risk-engine" not in snapshots
