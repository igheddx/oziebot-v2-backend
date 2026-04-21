from __future__ import annotations

import logging
import time
from contextvars import ContextVar, Token
from dataclasses import dataclass

from sqlalchemy import Engine, event

logger = logging.getLogger(__name__)

_OBSERVED_PATH_PREFIXES = ("/v1/me/dashboard", "/v1/me/analytics")
_SQL_TIMING_STACK_KEY = "oziebot_sql_timing_stack"
_QUERY_OBSERVERS_REGISTERED_KEY = "oziebot_query_observers_registered"
_SQL_PREVIEW_MAX_LENGTH = 240


@dataclass(slots=True)
class RequestPerformanceStats:
    request_id: str
    method: str
    path: str
    observed: bool
    query_count: int = 0
    query_duration_ms: float = 0.0
    slow_query_count: int = 0


_request_stats_var: ContextVar[RequestPerformanceStats | None] = ContextVar(
    "oziebot_request_performance_stats",
    default=None,
)


def should_observe_path(path: str) -> bool:
    return path.startswith(_OBSERVED_PATH_PREFIXES)


def begin_request(request_id: str, method: str, path: str) -> Token[RequestPerformanceStats | None]:
    stats = RequestPerformanceStats(
        request_id=request_id,
        method=method,
        path=path,
        observed=should_observe_path(path),
    )
    return _request_stats_var.set(stats)


def end_request(token: Token[RequestPerformanceStats | None]) -> None:
    _request_stats_var.reset(token)


def current_request_stats() -> RequestPerformanceStats | None:
    return _request_stats_var.get()


def summarize_statement(statement: str) -> str:
    normalized = " ".join(statement.split())
    if len(normalized) <= _SQL_PREVIEW_MAX_LENGTH:
        return normalized
    return f"{normalized[: _SQL_PREVIEW_MAX_LENGTH - 3]}..."


def register_query_observers(engine: Engine, *, slow_query_ms: int) -> None:
    if getattr(engine, _QUERY_OBSERVERS_REGISTERED_KEY, False):
        return

    @event.listens_for(engine, "before_cursor_execute")
    def _before_cursor_execute(
        conn,
        cursor,
        statement,
        parameters,
        context,
        executemany,
    ) -> None:
        stack = conn.info.setdefault(_SQL_TIMING_STACK_KEY, [])
        stack.append(time.perf_counter())

    @event.listens_for(engine, "after_cursor_execute")
    def _after_cursor_execute(
        conn,
        cursor,
        statement,
        parameters,
        context,
        executemany,
    ) -> None:
        stack = conn.info.get(_SQL_TIMING_STACK_KEY)
        if not stack:
            return
        started_at = stack.pop()
        duration_ms = (time.perf_counter() - started_at) * 1000
        stats = current_request_stats()
        if stats is not None:
            stats.query_count += 1
            stats.query_duration_ms += duration_ms
        if duration_ms < slow_query_ms:
            return
        if stats is not None:
            stats.slow_query_count += 1
        logger.warning(
            "sql_slow request_id=%s path=%s duration_ms=%.1f statement=%s",
            stats.request_id if stats else "-",
            stats.path if stats else "-",
            duration_ms,
            summarize_statement(statement),
        )

    setattr(engine, _QUERY_OBSERVERS_REGISTERED_KEY, True)
