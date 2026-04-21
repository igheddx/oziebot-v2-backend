from fastapi.testclient import TestClient

from oziebot_api.config import Settings
from oziebot_api.db.session import make_engine, make_session_factory
from oziebot_api.services.performance_observability import should_observe_path


def test_make_engine_is_cached() -> None:
    settings = Settings(database_url="sqlite+pysqlite:///:memory:", api_slow_query_ms=250)

    first = make_engine(settings)
    second = make_engine(settings)

    assert first is second


def test_make_session_factory_is_cached() -> None:
    settings = Settings(database_url="sqlite+pysqlite:///:memory:", api_slow_query_ms=250)

    first = make_session_factory(settings)
    second = make_session_factory(settings)

    assert first is second


def test_should_observe_dashboard_and_analytics_paths() -> None:
    assert should_observe_path("/v1/me/dashboard")
    assert should_observe_path("/v1/me/dashboard/details")
    assert should_observe_path("/v1/me/analytics/summary")
    assert not should_observe_path("/v1/auth/login")


def test_dashboard_response_exposes_timing_headers(
    client: TestClient,
    regular_user_and_token: tuple[str, str],
) -> None:
    _, token = regular_user_and_token

    response = client.get(
        "/v1/me/dashboard?trading_mode=paper",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == 200, response.text
    assert response.headers["X-Oziebot-Request-Id"]
    assert float(response.headers["X-Oziebot-Request-Duration-Ms"]) >= 0
    assert int(response.headers["X-Oziebot-DB-Query-Count"]) >= 0
    assert float(response.headers["X-Oziebot-DB-Time-Ms"]) >= 0
