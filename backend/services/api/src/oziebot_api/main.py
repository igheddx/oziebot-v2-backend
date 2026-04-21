import logging
import re
import time
import uuid

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from oziebot_api.api.v1.router import api_router
from oziebot_api.config import get_settings
from oziebot_api.deps import cached_settings
from oziebot_api.services.performance_observability import (
    begin_request,
    current_request_stats,
    end_request,
)
from oziebot_api.services.slo_monitor import RouteSLODefinition, SLOMonitor

logger = logging.getLogger(__name__)
REQUEST_ID_HEADER = "X-Oziebot-Request-Id"
REQUEST_ID_PATTERN = re.compile(r"^[A-Za-z0-9._-]{8,64}$")


def create_app() -> FastAPI:
    settings = get_settings()
    slo_monitor = SLOMonitor(
        definitions=[
            RouteSLODefinition(
                name="dashboard_summary",
                path="/v1/me/dashboard/summary",
                target_ms=2000,
            ),
            RouteSLODefinition(
                name="analytics_summary",
                path="/v1/me/analytics/summary",
                target_ms=5000,
            ),
        ],
        sample_window=settings.api_slo_sample_window,
        min_samples=settings.api_slo_min_samples,
        breach_rate_warn_pct=settings.api_slo_breach_rate_warn_pct,
    )
    app = FastAPI(title="Oziebot API", version="0.1.0")

    @app.get("/health")
    def root_health() -> dict:
        return {"status": "ok"}

    @app.get("/health/slo")
    def slo_health() -> dict:
        return slo_monitor.snapshot()

    origins = [o.strip() for o in settings.cors_origins.split(",") if o.strip()]
    app.add_middleware(
        CORSMiddleware,
        allow_origins=origins or ["http://localhost:3000"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.on_event("startup")
    def _warm_settings() -> None:
        cached_settings.cache_clear()
        cached_settings()

    @app.middleware("http")
    async def observe_request(request, call_next):
        incoming_request_id = request.headers.get(REQUEST_ID_HEADER)
        request_id = (
            incoming_request_id.strip()
            if incoming_request_id and REQUEST_ID_PATTERN.fullmatch(incoming_request_id.strip())
            else uuid.uuid4().hex[:12]
        )
        started_at = time.perf_counter()
        token = begin_request(request_id, request.method, request.url.path)
        try:
            response = await call_next(request)
        except Exception:
            duration_ms = (time.perf_counter() - started_at) * 1000
            stats = current_request_stats()
            slo = slo_monitor.observe(
                path=request.url.path, duration_ms=duration_ms, status_code=500
            )
            if stats and (stats.observed or duration_ms >= settings.api_slow_request_ms):
                logger.exception(
                    "api_request_failed request_id=%s method=%s path=%s duration_ms=%.1f "
                    "db_query_count=%s db_duration_ms=%.1f slow_query_count=%s",
                    stats.request_id,
                    stats.method,
                    stats.path,
                    duration_ms,
                    stats.query_count,
                    stats.query_duration_ms,
                    stats.slow_query_count,
                )
            if slo and slo["status"] == "breached":
                logger.warning(
                    "api_slo_breached route=%s p95_ms=%s breach_rate_pct=%s error_rate_pct=%s target_ms=%s",
                    slo["path"],
                    slo["p95Ms"],
                    slo["breachRatePct"],
                    slo["errorRatePct"],
                    slo["targetMs"],
                )
            end_request(token)
            raise

        duration_ms = (time.perf_counter() - started_at) * 1000
        stats = current_request_stats()
        slo = slo_monitor.observe(
            path=request.url.path,
            duration_ms=duration_ms,
            status_code=response.status_code,
        )
        if stats:
            response.headers[REQUEST_ID_HEADER] = stats.request_id
        if stats and stats.observed:
            response.headers["X-Oziebot-Request-Duration-Ms"] = f"{duration_ms:.1f}"
            response.headers["X-Oziebot-DB-Query-Count"] = str(stats.query_count)
            response.headers["X-Oziebot-DB-Time-Ms"] = f"{stats.query_duration_ms:.1f}"
        if slo:
            response.headers["X-Oziebot-SLO-Target-Ms"] = str(slo["targetMs"])
            response.headers["X-Oziebot-SLO-Status"] = str(slo["status"])
            response.headers["X-Oziebot-SLO-P95-Ms"] = str(slo["p95Ms"])
        if stats and (stats.observed or duration_ms >= settings.api_slow_request_ms):
            log_fn = logger.warning if duration_ms >= settings.api_slow_request_ms else logger.info
            log_fn(
                "api_request request_id=%s method=%s path=%s status_code=%s duration_ms=%.1f "
                "db_query_count=%s db_duration_ms=%.1f slow_query_count=%s",
                stats.request_id,
                stats.method,
                stats.path,
                response.status_code,
                duration_ms,
                stats.query_count,
                stats.query_duration_ms,
                stats.slow_query_count,
            )
        if slo and slo["status"] == "breached":
            logger.warning(
                "api_slo_breached route=%s p95_ms=%s breach_rate_pct=%s error_rate_pct=%s target_ms=%s",
                slo["path"],
                slo["p95Ms"],
                slo["breachRatePct"],
                slo["errorRatePct"],
                slo["targetMs"],
            )
        end_request(token)
        return response

    app.include_router(api_router)
    return app


app = create_app()
