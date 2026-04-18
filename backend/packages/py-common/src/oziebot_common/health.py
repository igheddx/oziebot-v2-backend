from __future__ import annotations

import json
import logging
import os
import threading
from dataclasses import dataclass, field
from datetime import UTC, datetime
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

log = logging.getLogger("oziebot-health")


@dataclass(slots=True)
class HealthState:
    service_name: str
    stale_after_seconds: int = 90
    ready: bool = False
    started_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    last_heartbeat_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def touch(self) -> None:
        with self._lock:
            self.last_heartbeat_at = datetime.now(UTC)

    def mark_ready(self) -> None:
        with self._lock:
            self.ready = True
            self.last_heartbeat_at = datetime.now(UTC)

    def snapshot(self) -> dict[str, object]:
        with self._lock:
            now = datetime.now(UTC)
            age_seconds = (now - self.last_heartbeat_at).total_seconds()
            healthy = age_seconds <= self.stale_after_seconds
            return {
                "service": self.service_name,
                "status": "ok" if healthy else "stale",
                "ready": self.ready and healthy,
                "started_at": self.started_at.isoformat(),
                "last_heartbeat_at": self.last_heartbeat_at.isoformat(),
                "stale_after_seconds": self.stale_after_seconds,
                "heartbeat_age_seconds": round(age_seconds, 3),
            }


def start_health_server(service_name: str) -> HealthState:
    port = int(os.environ.get("OZIEBOT_HEALTH_PORT", "8080"))
    host = os.environ.get("OZIEBOT_HEALTH_HOST", "0.0.0.0")
    stale_after_seconds = int(os.environ.get("OZIEBOT_HEALTH_STALE_SECONDS", "90"))
    auto_touch_seconds = int(os.environ.get("OZIEBOT_HEALTH_AUTO_TOUCH_SECONDS", "0"))

    state = HealthState(service_name=service_name, stale_after_seconds=stale_after_seconds)

    class _Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            snapshot = state.snapshot()
            if self.path == "/health":
                status_code = HTTPStatus.OK if snapshot["status"] == "ok" else HTTPStatus.SERVICE_UNAVAILABLE
            elif self.path == "/ready":
                status_code = HTTPStatus.OK if snapshot["ready"] else HTTPStatus.SERVICE_UNAVAILABLE
            else:
                status_code = HTTPStatus.NOT_FOUND
                snapshot = {"detail": "not found"}

            body = json.dumps(snapshot).encode("utf-8")
            self.send_response(status_code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args: object) -> None:
            return

    server = ThreadingHTTPServer((host, port), _Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True, name=f"{service_name}-health")
    thread.start()
    if auto_touch_seconds > 0:
        def _auto_touch() -> None:
            while True:
                state.touch()
                threading.Event().wait(auto_touch_seconds)

        ticker = threading.Thread(target=_auto_touch, daemon=True, name=f"{service_name}-health-ticker")
        ticker.start()
    log.info(
        "health server started service=%s host=%s port=%s stale_after_seconds=%s auto_touch_seconds=%s",
        service_name,
        host,
        port,
        stale_after_seconds,
        auto_touch_seconds,
    )
    return state
