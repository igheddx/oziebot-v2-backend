from __future__ import annotations

import threading
from collections import deque
from collections.abc import Iterable
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class RouteSLODefinition:
    name: str
    path: str
    target_ms: int


def _percentile(values: Iterable[float], percentile: float) -> float:
    ordered = sorted(values)
    if not ordered:
        return 0.0
    if len(ordered) == 1:
        return round(float(ordered[0]), 1)
    index = max(0, min(len(ordered) - 1, int(round((len(ordered) - 1) * percentile))))
    return round(float(ordered[index]), 1)


class SLOMonitor:
    def __init__(
        self,
        *,
        definitions: list[RouteSLODefinition],
        sample_window: int,
        min_samples: int,
        breach_rate_warn_pct: float,
    ) -> None:
        self._definitions = sorted(definitions, key=lambda item: len(item.path), reverse=True)
        self._sample_window = sample_window
        self._min_samples = min_samples
        self._breach_rate_warn_pct = breach_rate_warn_pct
        self._lock = threading.Lock()
        self._samples: dict[str, deque[tuple[float, bool, bool]]] = {
            definition.name: deque(maxlen=sample_window) for definition in self._definitions
        }

    def observe(
        self, *, path: str, duration_ms: float, status_code: int
    ) -> dict[str, object] | None:
        definition = self._match(path)
        if definition is None:
            return None
        breached = duration_ms > definition.target_ms
        errored = status_code >= 500
        with self._lock:
            self._samples[definition.name].append((duration_ms, breached, errored))
            return self._snapshot_for(definition)

    def snapshot(self) -> dict[str, object]:
        with self._lock:
            routes = {
                definition.name: self._snapshot_for(definition) for definition in self._definitions
            }
        degraded = any(route["status"] == "breached" for route in routes.values())
        return {
            "sampleWindow": self._sample_window,
            "minSamples": self._min_samples,
            "breachRateWarnPct": self._breach_rate_warn_pct,
            "degraded": degraded,
            "routes": routes,
        }

    def _match(self, path: str) -> RouteSLODefinition | None:
        for definition in self._definitions:
            if path == definition.path:
                return definition
        return None

    def _snapshot_for(self, definition: RouteSLODefinition) -> dict[str, object]:
        samples = list(self._samples[definition.name])
        durations = [duration for duration, _breached, _errored in samples]
        breach_count = sum(1 for _duration, breached, _errored in samples if breached)
        error_count = sum(1 for _duration, _breached, errored in samples if errored)
        count = len(samples)
        breach_rate_pct = round((breach_count / count) * 100, 2) if count else 0.0
        error_rate_pct = round((error_count / count) * 100, 2) if count else 0.0
        if count < self._min_samples:
            status = "insufficient_data"
        elif (
            _percentile(durations, 0.95) > definition.target_ms
            or breach_rate_pct > self._breach_rate_warn_pct
        ):
            status = "breached"
        else:
            status = "ok"
        return {
            "path": definition.path,
            "targetMs": definition.target_ms,
            "count": count,
            "p50Ms": _percentile(durations, 0.50),
            "p95Ms": _percentile(durations, 0.95),
            "p99Ms": _percentile(durations, 0.99),
            "breachRatePct": breach_rate_pct,
            "errorRatePct": error_rate_pct,
            "status": status,
        }
