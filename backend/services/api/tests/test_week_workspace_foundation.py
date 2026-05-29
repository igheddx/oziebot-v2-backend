from __future__ import annotations

from datetime import date, timedelta

from sqlalchemy.orm import Session

from tests.test_current_week_foundation import _align_teacher_tenant, _create_week_guide
from tests.test_education_catalog import _root_token
from tests.test_pacing_guide_foundation import _catalog_scope, _school_year, _teacher_token


def test_week_workspace_context_generate_and_history(client, db_session: Session):
    root_token = _root_token(client, db_session)
    teacher_token = _teacher_token(client, db_session, tenant_name="Week Workspace Tenant")
    _align_teacher_tenant(db_session, teacher_email="pacing-guide-teacher@example.com")
    scope = _catalog_scope(client, root_token)
    school_year = _school_year(client, root_token)
    guide, period_id = _create_week_guide(client, root_token, scope, school_year)

    client.patch(
        "/v1/teacher-assist/pacing-guides/active-selection",
        headers={"Authorization": f"Bearer {teacher_token}"},
        json={"active_pacing_guide_id": guide["id"]},
    )

    workspace = client.get(
        f"/v1/teacher-assist/week-workspace?period_id={period_id}",
        headers={"Authorization": f"Bearer {teacher_token}"},
    )
    assert workspace.status_code == 200, workspace.text
    payload = workspace.json()
    assert payload["week_context"]["pacing_guide_period_id"] == period_id
    assert len(payload["week_actions"]) >= 5
    assert "artifact_library" in payload

    week_context = client.get(
        f"/v1/teacher-assist/pacing-guide-periods/{period_id}/week-context",
        headers={"Authorization": f"Bearer {teacher_token}"},
    )
    assert week_context.status_code == 200, week_context.text
    assert week_context.json()["period_title"] == "Current Week"

    school_class = client.post(
        "/v1/teacher-assist/classes",
        headers={"Authorization": f"Bearer {teacher_token}"},
        json={"name": "Week Workspace Class", "grade_level": "5", "student_count": 20, "school_year_id": school_year["id"]},
    )
    assert school_class.status_code == 201, school_class.text
    class_id = school_class.json()["id"]

    subject = client.post(
        "/v1/teacher-assist/subjects",
        headers={"Authorization": f"Bearer {teacher_token}"},
        json={"code": "MATH", "name": "Math"},
    )
    assert subject.status_code == 201, subject.text

    client.post(
        f"/v1/teacher-assist/classes/{class_id}/subjects",
        headers={"Authorization": f"Bearer {teacher_token}"},
        json={"subject_id": subject.json()["id"]},
    )

    generated = client.post(
        f"/v1/teacher-assist/pacing-guide-periods/{period_id}/generate",
        headers={"Authorization": f"Bearer {teacher_token}"},
        json={"artifact_type": "ASSIGNMENT", "class_id": class_id},
    )
    assert generated.status_code == 201, generated.text
    assert generated.json()["artifact_type"] == "ASSIGNMENT"

    history = client.get(
        f"/v1/teacher-assist/pacing-guide-periods/{period_id}/artifacts",
        headers={"Authorization": f"Bearer {teacher_token}"},
    )
    assert history.status_code == 200, history.text
    assert len(history.json()) >= 1

    duplicate = client.post(
        f"/v1/teacher-assist/pacing-guide-periods/{period_id}/artifacts/{generated.json()['id']}/duplicate",
        headers={"Authorization": f"Bearer {teacher_token}"},
        json={},
    )
    assert duplicate.status_code == 201, duplicate.text
    assert duplicate.json()["title"].endswith("(Copy)")

    parent = client.post(
        f"/v1/teacher-assist/pacing-guide-periods/{period_id}/generate",
        headers={"Authorization": f"Bearer {teacher_token}"},
        json={"artifact_type": "PARENT_COMMUNICATION", "class_id": class_id},
    )
    assert parent.status_code == 201, parent.text
    assert parent.json()["artifact_type"] == "PARENT_COMMUNICATION"
