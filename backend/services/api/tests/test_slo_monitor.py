from oziebot_api.services.slo_monitor import RouteSLODefinition, SLOMonitor


def test_slo_monitor_reports_breached_route_after_min_samples() -> None:
    monitor = SLOMonitor(
        definitions=[
            RouteSLODefinition(
                name="dashboard_summary",
                path="/v1/me/dashboard/summary",
                target_ms=2000,
            )
        ],
        sample_window=5,
        min_samples=3,
        breach_rate_warn_pct=10.0,
    )

    monitor.observe(path="/v1/me/dashboard/summary", duration_ms=1500, status_code=200)
    monitor.observe(path="/v1/me/dashboard/summary", duration_ms=2500, status_code=200)
    snapshot = monitor.observe(
        path="/v1/me/dashboard/summary",
        duration_ms=2600,
        status_code=200,
    )

    assert snapshot is not None
    assert snapshot["status"] == "breached"
    assert snapshot["p95Ms"] == 2600.0
    assert snapshot["breachRatePct"] == 66.67


def test_slo_monitor_ignores_untracked_routes() -> None:
    monitor = SLOMonitor(
        definitions=[],
        sample_window=5,
        min_samples=3,
        breach_rate_warn_pct=10.0,
    )

    assert monitor.observe(path="/health", duration_ms=50, status_code=200) is None
