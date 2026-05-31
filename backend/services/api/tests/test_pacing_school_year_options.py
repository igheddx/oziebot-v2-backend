from __future__ import annotations

from datetime import date

from oziebot_api.services.teacher_assist.pacing_school_year_options import (
    build_pacing_school_year_options,
    build_pacing_school_year_specs,
    current_school_year_start_year,
)


def test_current_school_year_start_year_before_august():
    assert current_school_year_start_year(today=date(2026, 5, 28)) == 2025


def test_current_school_year_start_year_from_august():
    assert current_school_year_start_year(today=date(2026, 8, 1)) == 2026


def test_pacing_school_year_specs_may_default_next_and_include_above_next():
    specs = build_pacing_school_year_specs(today=date(2026, 5, 28))
    roles = [row["role"] for row in specs]
    assert roles == ["current", "next", "above_next"]
    assert specs[0]["title"] == "2025-2026"
    assert specs[1]["title"] == "2026-2027"
    assert specs[2]["title"] == "2027-2028"
    assert specs[1]["is_default"] is True


def test_pacing_school_year_specs_before_may_defaults_current():
    specs = build_pacing_school_year_specs(today=date(2026, 3, 15))
    assert specs[0]["is_default"] is True
    assert specs[1]["is_default"] is False


def test_pacing_school_year_specs_hides_above_next_before_january():
    specs = build_pacing_school_year_specs(today=date(2025, 12, 15))
    assert [row["role"] for row in specs] == ["current", "next"]


def test_build_pacing_school_year_options_creates_rows(client, db_session):
    from tests.test_education_catalog import _root_token
    from tests.test_teacher_assist_setup import _grant_teacher_assist_access, _register_user

    root_token = _root_token(client, db_session)
    teacher_token = _register_user(client, email="pacing-year-options@example.com", tenant_name="Pacing Year Tenant")
    _grant_teacher_assist_access(db_session, email="pacing-year-options@example.com")

    response = client.get(
        "/v1/teacher-assist/pacing-guides/school-year-options",
        headers={"Authorization": f"Bearer {teacher_token}"},
    )
    assert response.status_code == 200, response.text
    payload = response.json()
    assert len(payload["options"]) >= 2
    assert payload["default_school_year_id"] is not None
    assert any(row["is_default"] for row in payload["options"])

    # Root admin can also use the endpoint
    root_response = client.get(
        "/v1/teacher-assist/pacing-guides/school-year-options",
        headers={"Authorization": f"Bearer {root_token}"},
    )
    assert root_response.status_code == 200, root_response.text
