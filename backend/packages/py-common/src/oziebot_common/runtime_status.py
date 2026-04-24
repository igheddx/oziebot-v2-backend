from __future__ import annotations

import json
from collections.abc import Iterable
from typing import Any

RUNTIME_STATUS_KEY_PREFIX = "oziebot:runtime:health:"


def runtime_status_key(service_name: str) -> str:
    return f"{RUNTIME_STATUS_KEY_PREFIX}{service_name}"


def publish_runtime_status(
    redis_client: Any,
    snapshot: dict[str, object],
    *,
    ttl_seconds: int,
) -> None:
    service_name = str(snapshot.get("service") or "").strip()
    if not service_name:
        raise ValueError("runtime status snapshot missing service name")
    redis_client.set(
        runtime_status_key(service_name),
        json.dumps(snapshot, default=str),
        ex=max(int(ttl_seconds), 1),
    )


def read_runtime_statuses(
    redis_client: Any,
    service_names: Iterable[str],
) -> dict[str, dict[str, object]]:
    names = [str(name).strip() for name in service_names if str(name).strip()]
    if not names:
        return {}
    raw_values = redis_client.mget([runtime_status_key(name) for name in names])
    snapshots: dict[str, dict[str, object]] = {}
    for service_name, raw_value in zip(names, raw_values, strict=False):
        if raw_value is None:
            continue
        payload = (
            raw_value.decode("utf-8") if isinstance(raw_value, bytes) else raw_value
        )
        snapshot = json.loads(payload)
        if isinstance(snapshot, dict):
            snapshots[service_name] = snapshot
    return snapshots
