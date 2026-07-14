from __future__ import annotations

from datetime import date, timedelta

from sqlalchemy.orm import Session

from tests.test_current_week_foundation import _align_teacher_tenant, _create_week_guide
from tests.test_education_catalog import _root_token
from tests.test_pacing_guide_foundation import _catalog_scope, _school_year, _teacher_token


def _add_week_period(
    client, root_token: str, guide_id: str, *, title: str, sequence_number: int
) -> str:
    today = date.today()
    week_start = today - timedelta(days=today.weekday()) + timedelta(weeks=sequence_number - 1)
    week_end = week_start + timedelta(days=4)
    response = client.post(
        f"/v1/teacher-assist/pacing-guides/{guide_id}/periods",
        headers={"Authorization": f"Bearer {root_token}"},
        json={
            "period_type": "WEEK",
            "title": title,
            "description": f"{title} description",
            "sequence_number": sequence_number,
            "start_date": week_start.isoformat(),
            "end_date": week_end.isoformat(),
        },
    )
    assert response.status_code == 201, response.text
    return response.json()["id"]


def test_time_savings_reuse_duplicate_template_and_efficiency(client, db_session: Session):
    root_token = _root_token(client, db_session)
    teacher_token = _teacher_token(client, db_session, tenant_name="Time Savings Tenant")
    _align_teacher_tenant(db_session, teacher_email="pacing-guide-teacher@example.com")
    scope = _catalog_scope(client, root_token)
    school_year = _school_year(client, root_token)
    guide, period_id = _create_week_guide(client, root_token, scope, school_year)
    next_period_id = _add_week_period(
        client, root_token, guide["id"], title="Next Week", sequence_number=2
    )

    client.patch(
        "/v1/teacher-assist/pacing-guides/active-selection",
        headers={"Authorization": f"Bearer {teacher_token}"},
        json={"active_pacing_guide_id": guide["id"]},
    )

    reuse = client.get(
        f"/v1/teacher-assist/reuse/search?period_id={period_id}",
        headers={"Authorization": f"Bearer {teacher_token}"},
    )
    assert reuse.status_code == 200, reuse.text
    assert isinstance(reuse.json(), list)

    recommendations = client.get(
        f"/v1/teacher-assist/pacing-guide-periods/{period_id}/recommendations",
        headers={"Authorization": f"Bearer {teacher_token}"},
    )
    assert recommendations.status_code == 200, recommendations.text
    assert "recommended_for_this_week" in recommendations.json()

    duplicate = client.post(
        f"/v1/teacher-assist/pacing-guide-periods/{period_id}/duplicate",
        headers={"Authorization": f"Bearer {teacher_token}"},
        json={"copy_objectives": True, "copy_resources": True, "copy_notes": True},
    )
    assert duplicate.status_code == 200, duplicate.text
    assert duplicate.json()["navigation_href"]

    generate_next = client.post(
        f"/v1/teacher-assist/pacing-guide-periods/{period_id}/generate-next-week",
        headers={"Authorization": f"Bearer {teacher_token}"},
    )
    assert generate_next.status_code == 200, generate_next.text
    assert generate_next.json()["next_period_id"] == next_period_id
    assert generate_next.json()["requires_review"] is True

    template = client.post(
        f"/v1/teacher-assist/pacing-guide-periods/{period_id}/templates",
        headers={"Authorization": f"Bearer {teacher_token}"},
        json={
            "name": "Week 1 Template",
            "artifact_type": "WEEK",
            "template_type": "TEACHER",
            "visibility": "PRIVATE",
        },
    )
    assert template.status_code == 201, template.text
    template_id = template.json()["id"]

    templates = client.get(
        "/v1/teacher-assist/week-templates",
        headers={"Authorization": f"Bearer {teacher_token}"},
    )
    assert templates.status_code == 200, templates.text
    assert any(row["id"] == template_id for row in templates.json())

    applied = client.post(
        f"/v1/teacher-assist/week-templates/{template_id}/apply",
        headers={"Authorization": f"Bearer {teacher_token}"},
        json={"target_period_id": next_period_id},
    )
    assert applied.status_code == 200, applied.text

    efficiency = client.get(
        "/v1/teacher-assist/efficiency-dashboard",
        headers={"Authorization": f"Bearer {teacher_token}"},
    )
    assert efficiency.status_code == 200, efficiency.text
    payload = efficiency.json()
    assert payload["estimated_hours_saved"] >= 0
    assert "reuse_rate_percent" in payload

    home = client.get(
        "/v1/teacher-assist/home",
        headers={"Authorization": f"Bearer {teacher_token}"},
    )
    assert home.status_code == 200, home.text
    home_payload = home.json()
    assert "continue_planning" in home_payload
    assert "time_savings" in home_payload
    assert "efficiency_summary" in home_payload

    workspace = client.get(
        f"/v1/teacher-assist/week-workspace?period_id={period_id}",
        headers={"Authorization": f"Bearer {teacher_token}"},
    )
    assert workspace.status_code == 200, workspace.text
    assert "recommendations" in workspace.json()

    planning_group = client.post(
        "/v1/teacher-assist/planning-groups",
        headers={"Authorization": f"Bearer {teacher_token}"},
        json={
            "name": "5th Grade Science Team",
            "subject": "Science",
            "grade_level": "5",
            "visibility": "TEAM",
        },
    )
    assert planning_group.status_code == 201, planning_group.text
    group_id = planning_group.json()["id"]

    joined = client.post(
        f"/v1/teacher-assist/planning-groups/{group_id}/join",
        headers={"Authorization": f"Bearer {teacher_token}"},
    )
    assert joined.status_code == 201, joined.text
