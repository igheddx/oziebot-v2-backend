from __future__ import annotations

import json
from collections.abc import Iterable

from sqlalchemy.engine import Engine

from oziebot_common.postgres_runtime_kv import PostgresRuntimeKV
from oziebot_common.s3_observability import get_observability_store

RUNTIME_STATUS_KEY_PREFIX = "oziebot:runtime:health:"


def runtime_status_key(service_name: str) -> str:
    return f"{RUNTIME_STATUS_KEY_PREFIX}{service_name}"


def publish_runtime_status(
    engine: Engine | None,
    snapshot: dict[str, object],
    *,
    ttl_seconds: int,
) -> None:
    service_name = str(snapshot.get("service") or "").strip()
    if not service_name:
        raise ValueError("runtime status snapshot missing service name")
    store = get_observability_store()
    if store is not None:
        store.publish_runtime_status(snapshot)
        return
    if engine is None:
        return
    kv = PostgresRuntimeKV(engine)
    kv.setex(
        runtime_status_key(service_name),
        max(int(ttl_seconds), 1),
        json.dumps(snapshot, default=str),
    )


def read_runtime_statuses(
    engine: Engine | None,
    service_names: Iterable[str],
    *,
    use_observability_store: bool = True,
) -> dict[str, dict[str, object]]:
    """Load worker heartbeat payloads keyed by logical service name.

    When ``use_observability_store`` is True (default), an S3 observability backend
    (``OBSERVABILITY_S3_BUCKET``) wins if configured. Pass ``False`` to read only
    from Postgres ``runtime_kv`` using ``engine``.
    """
    names = [str(name).strip() for name in service_names if str(name).strip()]
    if not names:
        return {}
    if use_observability_store:
        store = get_observability_store()
        if store is not None:
            return store.read_runtime_statuses(names)
    if engine is None:
        return {}
    kv = PostgresRuntimeKV(engine)
    snapshots: dict[str, dict[str, object]] = {}
    for service_name in names:
        raw_value = kv.get(runtime_status_key(service_name))
        if raw_value is None:
            continue
        try:
            snapshot = json.loads(raw_value)
        except json.JSONDecodeError:
            continue
        if isinstance(snapshot, dict):
            snapshots[service_name] = snapshot
    return snapshots
