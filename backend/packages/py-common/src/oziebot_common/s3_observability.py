from __future__ import annotations

import json
import os
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from functools import lru_cache
from io import BytesIO
from typing import Any, Iterable


@dataclass(frozen=True)
class ObservabilityS3Config:
    bucket: str
    prefix: str = "observability"
    region: str | None = None


def get_observability_s3_config() -> ObservabilityS3Config | None:
    bucket = str(os.environ.get("OBSERVABILITY_S3_BUCKET") or "").strip()
    if not bucket:
        return None
    prefix = (
        str(os.environ.get("OBSERVABILITY_S3_PREFIX") or "observability")
        .strip()
        .strip("/")
    )
    region = str(os.environ.get("OBSERVABILITY_S3_REGION") or "").strip() or None
    return ObservabilityS3Config(
        bucket=bucket, prefix=prefix or "observability", region=region
    )


def reset_observability_store_cache() -> None:
    _store_from_env.cache_clear()


def get_observability_store() -> "S3ObservabilityStore | None":
    config = get_observability_s3_config()
    if config is None:
        return None
    return _store_from_env(config.bucket, config.prefix, config.region)


@lru_cache(maxsize=8)
def _store_from_env(
    bucket: str,
    prefix: str,
    region: str | None,
) -> "S3ObservabilityStore":
    return S3ObservabilityStore(
        client=_build_s3_client(region),
        bucket=bucket,
        prefix=prefix,
    )


def _build_s3_client(region: str | None) -> Any:
    import boto3

    return boto3.client("s3", region_name=region)


class S3ObservabilityStore:
    def __init__(
        self, *, client: Any, bucket: str, prefix: str = "observability"
    ) -> None:
        self._client = client
        self._bucket = bucket
        self._prefix = prefix.strip("/")

    def append_trade_event(self, event: dict[str, Any]) -> None:
        event_time = _parse_timestamp(event.get("timestamp"))
        symbol = str(event.get("symbol") or "UNKNOWN").upper()
        event_type = str(event.get("event_type") or "unknown").lower()
        self._put_json(
            self._history_key(
                "trade-events",
                event_time,
                suffix=f"{_ts_millis(event_time)}_{symbol}_{event_type}_{uuid.uuid4().hex}.json",
            ),
            event,
        )

    def read_trade_events(
        self,
        *,
        window_seconds: int,
        limit: int,
        symbol: str | None = None,
        event_type: str | None = None,
        now: datetime | None = None,
    ) -> list[dict[str, Any]]:
        current_time = (now or datetime.now(UTC)).astimezone(UTC)
        cutoff = current_time - timedelta(seconds=window_seconds)
        normalized_symbol = str(symbol or "").strip().upper()
        normalized_event_type = str(event_type or "").strip().lower()
        keys = self._list_recent_history_keys(
            "trade-events", cutoff=cutoff, now=current_time
        )
        events: list[dict[str, Any]] = []
        for key in sorted(keys, reverse=True):
            payload = self._get_json(key)
            if not isinstance(payload, dict):
                continue
            payload_time = _parse_timestamp(payload.get("timestamp"))
            if payload_time < cutoff:
                continue
            payload_symbol = str(payload.get("symbol") or "").upper()
            payload_event_type = str(payload.get("event_type") or "").lower()
            if normalized_symbol and payload_symbol != normalized_symbol:
                continue
            if normalized_event_type and payload_event_type != normalized_event_type:
                continue
            events.append(payload)
            if len(events) >= limit:
                break
        return list(reversed(events))

    def append_trade_sample(self, sample: dict[str, Any]) -> None:
        event_time = _parse_timestamp(sample.get("timestamp"))
        symbol = str(sample.get("symbol") or "UNKNOWN").upper()
        self._put_json(
            self._history_key(
                "trade-samples",
                event_time,
                leaf=symbol,
                suffix=f"{_ts_millis(event_time)}_{uuid.uuid4().hex}.json",
            ),
            sample,
        )

    def read_trade_samples(
        self,
        *,
        symbol: str,
        window_seconds: int,
        now: datetime | None = None,
    ) -> list[dict[str, Any]]:
        current_time = (now or datetime.now(UTC)).astimezone(UTC)
        cutoff = current_time - timedelta(seconds=window_seconds)
        symbol_value = str(symbol).upper()
        keys = self._list_recent_history_keys(
            "trade-samples",
            cutoff=cutoff,
            now=current_time,
            leaf=symbol_value,
        )
        samples: list[dict[str, Any]] = []
        for key in sorted(keys, reverse=True):
            payload = self._get_json(key)
            if not isinstance(payload, dict):
                continue
            payload_time = _parse_timestamp(payload.get("timestamp"))
            if payload_time < cutoff:
                continue
            if str(payload.get("symbol") or "").upper() != symbol_value:
                continue
            samples.append(payload)
        return list(reversed(samples))

    def write_trade_summary(self, summary: dict[str, Any]) -> None:
        event_time = _parse_timestamp(summary.get("timestamp"))
        symbol = str(summary.get("symbol") or "UNKNOWN").upper()
        self._put_json(self._latest_key("trade-summaries", symbol), summary)
        self._put_json(
            self._history_key(
                "trade-summaries",
                event_time,
                leaf=symbol,
                suffix=f"{_ts_millis(event_time)}_{uuid.uuid4().hex}.json",
            ),
            summary,
        )

    def read_trade_summaries(
        self, *, symbol: str | None = None
    ) -> list[dict[str, Any]]:
        if symbol:
            payload = self._get_json_if_exists(
                self._latest_key("trade-summaries", str(symbol).upper())
            )
            return [payload] if isinstance(payload, dict) else []

        keys = self._list_keys(self._latest_prefix("trade-summaries"))
        summaries: list[dict[str, Any]] = []
        for key in sorted(keys):
            payload = self._get_json(key)
            if isinstance(payload, dict):
                summaries.append(payload)
        return summaries

    def publish_runtime_status(self, snapshot: dict[str, Any]) -> None:
        event_time = _parse_timestamp(
            snapshot.get("last_heartbeat_at") or snapshot.get("started_at")
        )
        service_name = str(snapshot.get("service") or "").strip()
        if not service_name:
            raise ValueError("runtime status snapshot missing service name")
        self._put_json(self._latest_key("runtime-status", service_name), snapshot)
        self._put_json(
            self._history_key(
                "runtime-status",
                event_time,
                leaf=service_name,
                suffix=f"{_ts_millis(event_time)}_{uuid.uuid4().hex}.json",
            ),
            snapshot,
        )

    def read_runtime_statuses(
        self, service_names: Iterable[str]
    ) -> dict[str, dict[str, Any]]:
        snapshots: dict[str, dict[str, Any]] = {}
        for service_name in [
            str(name).strip() for name in service_names if str(name).strip()
        ]:
            payload = self._get_json_if_exists(
                self._latest_key("runtime-status", service_name)
            )
            if isinstance(payload, dict):
                snapshots[service_name] = payload
        return snapshots

    def _put_json(self, key: str, payload: dict[str, Any]) -> None:
        self._client.put_object(
            Bucket=self._bucket,
            Key=key,
            Body=json.dumps(payload, separators=(",", ":"), default=str).encode(
                "utf-8"
            ),
            ContentType="application/json",
        )

    def _get_json(self, key: str) -> Any:
        response = self._client.get_object(Bucket=self._bucket, Key=key)
        body = response.get("Body")
        if isinstance(body, BytesIO):
            raw = body.read()
        else:
            raw = body.read() if body is not None else b""
        return json.loads(raw.decode("utf-8"))

    def _get_json_if_exists(self, key: str) -> Any | None:
        try:
            return self._get_json(key)
        except Exception as exc:
            if _is_missing_key_error(exc):
                return None
            raise

    def _list_recent_history_keys(
        self,
        category: str,
        *,
        cutoff: datetime,
        now: datetime,
        leaf: str | None = None,
    ) -> list[str]:
        prefixes = [
            self._history_prefix(category, minute, leaf=leaf)
            for minute in _minute_range(cutoff, now)
        ]
        keys: list[str] = []
        for prefix in prefixes:
            keys.extend(self._list_keys(prefix))
        return keys

    def _list_keys(self, prefix: str) -> list[str]:
        continuation_token: str | None = None
        keys: list[str] = []
        while True:
            params = {"Bucket": self._bucket, "Prefix": prefix}
            if continuation_token:
                params["ContinuationToken"] = continuation_token
            response = self._client.list_objects_v2(**params)
            keys.extend(item["Key"] for item in response.get("Contents") or [])
            if not response.get("IsTruncated"):
                return keys
            continuation_token = response.get("NextContinuationToken")

    def _latest_prefix(self, category: str) -> str:
        return f"{self._prefix}/{category}/latest/"

    def _latest_key(self, category: str, leaf: str) -> str:
        return f"{self._latest_prefix(category)}{leaf}.json"

    def _history_prefix(
        self, category: str, timestamp: datetime, *, leaf: str | None = None
    ) -> str:
        path = f"{self._prefix}/{category}/history"
        if leaf:
            path = f"{path}/{leaf}"
        return f"{path}/{timestamp.astimezone(UTC):%Y/%m/%d/%H/%M}/"

    def _history_key(
        self,
        category: str,
        timestamp: datetime,
        *,
        suffix: str,
        leaf: str | None = None,
    ) -> str:
        return f"{self._history_prefix(category, timestamp, leaf=leaf)}{suffix}"


def _minute_range(start: datetime, end: datetime) -> list[datetime]:
    current = start.astimezone(UTC).replace(second=0, microsecond=0)
    final = end.astimezone(UTC).replace(second=0, microsecond=0)
    out: list[datetime] = []
    while current <= final:
        out.append(current)
        current += timedelta(minutes=1)
    return out


def _parse_timestamp(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value.astimezone(UTC)
    text = str(value or "").replace("Z", "+00:00")
    if not text:
        return datetime.now(UTC)
    return datetime.fromisoformat(text).astimezone(UTC)


def _ts_millis(value: datetime) -> str:
    return f"{int(value.astimezone(UTC).timestamp() * 1000):013d}"


def _is_missing_key_error(exc: Exception) -> bool:
    response = getattr(exc, "response", None)
    if isinstance(response, dict):
        error = response.get("Error") or {}
        code = str(error.get("Code") or "")
        return code in {"NoSuchKey", "404", "NotFound"}
    return False
